# ruff: noqa: INP001
"""Automatically export Matrix threads to YAML in enabled agents' workspaces.

Message hooks only record which room changed; one module-global runner task debounces triggers and
runs journal-backed export passes into every enabled agent's workspace, so bursts coalesce and at
most one pass runs at a time.
Each pass executes on a private event loop in a worker thread: export reconciliation re-reads and
re-parses every exported thread YAML synchronously, which blocked the runtime loop for over five
seconds per pass (``event_loop_stall_detected``) when run inline.
The runner task must stay inside the module-global ``_runner_tasks`` dict: plugin hot reload only
cancels tasks it finds in module globals or one level inside a global dict/list/set.
"""

from __future__ import annotations

import asyncio
import re
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import mindroom.thread_export as thread_export_pkg
from mindroom.constants import ROUTER_AGENT_NAME
from mindroom.hooks import (
    AfterResponseContext,
    AgentLifecycleContext,
    ConfigReloadedContext,
    MessageReceivedContext,
    hook,
)
from mindroom.matrix.identity import managed_account_key, managed_account_user_id
from mindroom.private_instance_identity import (
    PrivateInstanceIdentityError,
    load_private_instance_identity,
)
from mindroom.thread_export import (
    ThreadExportTarget,
    clear_thread_export_root,
    export_threads_to_targets_once,
)
from mindroom.tool_system.worker_routing import (
    agent_state_root_path,
    agent_workspace_root_path,
    private_instance_state_root_for_requester,
)
from mindroom.workspaces import resolve_agent_workspace_from_state_path

if TYPE_CHECKING:
    from pathlib import Path

    from structlog.stdlib import BoundLogger

    from mindroom.config.main import Config
    from mindroom.constants import RuntimePaths
    from mindroom.hooks.context import HookContext
    from mindroom.tool_system.worker_routing import WorkerScope

WORKSPACE_EXPORT_DIRNAME = "thread_exports"
PRIVATE_INSTANCES_DIRNAME = "private_instances"
DEFAULT_DEBOUNCE_SECONDS = 2.0
_MATRIX_USER_ID_PATTERN = re.compile(r"@[^:\s]+:\S+")

type PrivateRoomScope = Literal["owner", "owner_and_agent"]
DEFAULT_PRIVATE_ROOM_SCOPE: PrivateRoomScope = "owner_and_agent"

_runner_tasks: dict[str, asyncio.Task[None]] = {}
_pending_room_ids: set[str] = set()
_full_pass_pending = False
_pending_lock = threading.Lock()
_wakeup: asyncio.Event | None = None
_runner_loop: asyncio.AbstractEventLoop | None = None
_latest_env: _TriggerEnv | None = None
_private_instance_requesters: dict[tuple[str, Path], str] = {}
_private_instance_requesters_lock = threading.Lock()
_private_instance_requesters_revision = 0
_live_hook_seen = False

# Hot reload replaces this module but cannot interrupt a worker thread mid-pass, so the
# single-flight lock lives on the long-lived core package. The hook context's registry predicate
# keeps staged or superseded modules from starting another pass.
_EXPORT_PASS_LOCK: threading.Lock = thread_export_pkg.__dict__.setdefault(
    "_thread_export_plugin_pass_lock",
    threading.Lock(),
)


def _always_active() -> bool:
    return True


@dataclass(frozen=True)
class _TriggerEnv:
    """Runtime context captured from the most recent triggering hook."""

    config: Config
    runtime_paths: RuntimePaths
    settings: Mapping[str, object]
    logger: BoundLogger
    is_active: Callable[[], bool] = _always_active


@dataclass(frozen=True)
class _AgentExportSettings:
    """Per-agent export options from the plugin settings."""

    invited_rooms: bool = True
    private_room_scope: PrivateRoomScope = DEFAULT_PRIVATE_ROOM_SCOPE


def _agent_options(options: object) -> _AgentExportSettings:
    """Parse one agent's option mapping, tolerating missing or bare entries."""
    if not isinstance(options, Mapping):
        return _AgentExportSettings()
    typed_options = cast("Mapping[str, object]", options)
    invited_rooms = typed_options.get("invited_rooms", True)
    raw_private_room_scope = typed_options.get(
        "private_room_scope", DEFAULT_PRIVATE_ROOM_SCOPE
    )
    if raw_private_room_scope == "owner":
        private_room_scope: PrivateRoomScope = "owner"
    else:
        private_room_scope = DEFAULT_PRIVATE_ROOM_SCOPE
    return _AgentExportSettings(
        invited_rooms=invited_rooms if isinstance(invited_rooms, bool) else True,
        private_room_scope=private_room_scope,
    )


def _requested_agents(
    settings: Mapping[str, object],
) -> dict[str, _AgentExportSettings]:
    """Return per-agent export options for the agents listed in the plugin settings.

    ``agents`` accepts a plain list of names (all defaults) or a mapping of name to options
    (``invited_rooms`` and ``private_room_scope``).
    """
    raw = settings.get("agents")
    parsed: dict[str, _AgentExportSettings] = {}
    if isinstance(raw, (list, tuple)):
        for item in raw:
            if isinstance(item, str) and item.strip():
                parsed.setdefault(item.strip(), _AgentExportSettings())
    elif isinstance(raw, Mapping):
        for name, options in raw.items():
            if isinstance(name, str) and name.strip():
                parsed.setdefault(name.strip(), _agent_options(options))
    return parsed


def _requests_private_agents(ctx: HookContext) -> bool:
    """Return whether current settings enable any configured private agent."""
    return any(
        agent_config is not None and agent_config.private is not None
        for agent_name in _requested_agents(ctx.settings)
        if (agent_config := ctx.config.agents.get(agent_name)) is not None
    )


def _debounce_seconds(settings: Mapping[str, object]) -> float:
    """Return the configured trigger debounce in seconds."""
    raw = settings.get("debounce_seconds", DEFAULT_DEBOUNCE_SECONDS)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return max(float(raw), 0.0)
    return DEFAULT_DEBOUNCE_SECONDS


def _queue_full_pass() -> None:
    """Queue full reconciliation and wake the runner from any thread."""
    global _full_pass_pending  # noqa: PLW0603
    with _pending_lock:
        _full_pass_pending = True
    if _runner_loop is not None and _wakeup is not None:
        _runner_loop.call_soon_threadsafe(_wakeup.set)


def _queue_room(room_id: str) -> None:
    """Queue one dirty room without racing the runner's pending-work drain."""
    with _pending_lock:
        _pending_room_ids.add(room_id)


def _private_instance_requester(
    env: _TriggerEnv,
    agent_name: str,
    worker_scope: WorkerScope,
    state_root: Path,
) -> str | None:
    """Return the core-authorized requester for one private state root."""
    try:
        identity = load_private_instance_identity(
            env.runtime_paths.storage_root, state_root.parent
        )
    except PrivateInstanceIdentityError:
        return None
    if (
        identity is None
        or _MATRIX_USER_ID_PATTERN.fullmatch(identity.requester_id) is None
    ):
        return None
    expected_state_root = private_instance_state_root_for_requester(
        env.runtime_paths.storage_root,
        requester_id=identity.requester_id,
        agent_name=agent_name,
        worker_scope=worker_scope,
        runtime_paths=env.runtime_paths,
    )
    if (
        expected_state_root is None
        or expected_state_root.resolve() != state_root.resolve()
    ):
        return None
    return identity.requester_id


def _private_instance_requesters_for_requester(
    ctx: HookContext,
    requester_id: str,
) -> dict[tuple[str, Path], str]:
    """Forward-resolve every enabled private root for one requester."""
    if _MATRIX_USER_ID_PATTERN.fullmatch(requester_id) is None:
        return {}
    env = _TriggerEnv(ctx.config, ctx.runtime_paths, ctx.settings, ctx.logger)
    resolved: dict[tuple[str, Path], str] = {}
    for agent_name in _requested_agents(ctx.settings):
        agent_config = ctx.config.agents.get(agent_name)
        if agent_config is None or agent_config.private is None:
            continue
        state_root = private_instance_state_root_for_requester(
            ctx.runtime_paths.storage_root,
            requester_id=requester_id,
            agent_name=agent_name,
            worker_scope=agent_config.private.per,
            runtime_paths=ctx.runtime_paths,
        )
        if state_root is None or not state_root.is_dir() or state_root.is_symlink():
            continue
        if (
            _private_instance_requester(
                env, agent_name, agent_config.private.per, state_root
            )
            == requester_id
        ):
            resolved[(agent_name, state_root)] = requester_id
    return resolved


def _remember_private_instance_requester(
    ctx: HookContext,
    requester_id: str,
) -> None:
    """Remember core-authorized private roots observed by one hook."""
    global _private_instance_requesters_revision  # noqa: PLW0603
    discovered = _private_instance_requesters_for_requester(ctx, requester_id)
    if not ctx.is_active():
        return
    enabled_agent_names = set(_requested_agents(ctx.settings))
    with _private_instance_requesters_lock:
        updated = {
            cache_key: owner
            for cache_key, owner in _private_instance_requesters.items()
            if not (owner == requester_id and cache_key[0] in enabled_agent_names)
        }
        updated.update(discovered)
        if updated == _private_instance_requesters:
            return
        _private_instance_requesters.clear()
        _private_instance_requesters.update(updated)
        _private_instance_requesters_revision += 1
    _queue_full_pass()


async def _refresh_private_instance_requester(
    ctx: HookContext,
    requester_id: str,
) -> None:
    """Refresh one private requester without blocking the runtime event loop."""
    if _requests_private_agents(ctx):
        await asyncio.to_thread(_remember_private_instance_requester, ctx, requester_id)


def _record_trigger(ctx: HookContext) -> None:
    """Capture the trigger context and wake the runner, starting it when needed."""
    global _latest_env, _wakeup  # noqa: PLW0603
    _latest_env = _TriggerEnv(
        config=ctx.config,
        runtime_paths=ctx.runtime_paths,
        settings=ctx.settings,
        logger=ctx.logger,
        is_active=ctx.is_active,
    )
    if _wakeup is None:
        _wakeup = asyncio.Event()
    runner = _runner_tasks.get("runner")
    if runner is None or runner.done():
        _runner_tasks["runner"] = asyncio.create_task(
            _run_export_loop(), name="thread-export-runner"
        )
    _wakeup.set()


def _accept_live_hook(ctx: HookContext) -> bool:
    """Accept hooks only from the published registry and reconcile on first use."""
    global _live_hook_seen  # noqa: PLW0603
    if not ctx.is_active():
        return False
    if not _live_hook_seen:
        _live_hook_seen = True
        _queue_full_pass()
    return True


def _drain_pending() -> tuple[bool, frozenset[str]]:
    """Atomically consume the pending full-pass flag and dirty room set."""
    global _full_pass_pending  # noqa: PLW0603
    with _pending_lock:
        full_pass = _full_pass_pending
        _full_pass_pending = False
        room_ids = frozenset(_pending_room_ids)
        _pending_room_ids.clear()
    return full_pass, room_ids


async def _run_export_loop() -> None:
    """Drain export triggers one debounced single-flight pass at a time."""
    global _runner_loop  # noqa: PLW0603
    _runner_loop = asyncio.get_running_loop()
    while True:
        assert _wakeup is not None  # noqa: S101 - created before this task starts
        await _wakeup.wait()
        _wakeup.clear()
        env = _latest_env
        if env is None:
            continue
        debounce = _debounce_seconds(env.settings)
        if debounce > 0:
            await asyncio.sleep(debounce)
        full_pass, room_ids = _drain_pending()
        if not full_pass and not room_ids:
            continue
        env = _latest_env or env
        try:
            await asyncio.to_thread(
                _run_export_pass_blocking, env, full_pass=full_pass, room_ids=room_ids
            )
        except Exception:
            env.logger.exception("Thread export pass crashed")


def _run_export_pass_blocking(
    env: _TriggerEnv, *, full_pass: bool, room_ids: frozenset[str]
) -> None:
    """Run one export pass to completion on a private event loop in the calling thread."""
    with _EXPORT_PASS_LOCK:
        if not env.is_active():
            return
        asyncio.run(_run_export_pass(env, full_pass=full_pass, room_ids=room_ids))


def _private_instance_state_roots(
    storage_root: Path, agent_name: str
) -> tuple[Path, ...]:
    """Return existing private-instance state roots for one private agent."""
    instances_root = storage_root / PRIVATE_INSTANCES_DIRNAME
    if not instances_root.is_dir() or instances_root.is_symlink():
        return ()
    instance_dir_names = {
        agent_name,
        agent_state_root_path(storage_root, agent_name).name,
    }
    return tuple(
        sorted(
            state_root
            for scope_dir in instances_root.iterdir()
            if scope_dir.is_dir() and not scope_dir.is_symlink()
            for state_root in scope_dir.iterdir()
            if (
                state_root.is_dir()
                and not state_root.is_symlink()
                and state_root.name in instance_dir_names
            )
        ),
    )


def _private_instance_requesters_snapshot() -> tuple[dict[tuple[str, Path], str], int]:
    """Return the cached private-root index and its revision atomically."""
    with _private_instance_requesters_lock:
        return dict(_private_instance_requesters), _private_instance_requesters_revision


def _evict_private_instance_requester(
    cache_key: tuple[str, Path], expected_owner: str
) -> None:
    """Forget a still-stale cached root and reconcile every private export."""
    global _private_instance_requesters_revision  # noqa: PLW0603
    with _private_instance_requesters_lock:
        if _private_instance_requesters.get(cache_key) != expected_owner:
            return
        del _private_instance_requesters[cache_key]
        _private_instance_requesters_revision += 1
    _queue_full_pass()


def _discover_private_instance_requesters(
    env: _TriggerEnv, enabled_agent_names: set[str]
) -> dict[tuple[str, Path], str]:
    """Discover every core-authorized private state root for enabled agents."""
    discovered: dict[tuple[str, Path], str] = {}
    for agent_name in enabled_agent_names:
        agent_config = env.config.agents[agent_name]
        if agent_config.private is None:
            continue
        for state_root in _private_instance_state_roots(
            env.runtime_paths.storage_root, agent_name
        ):
            requester_id = _private_instance_requester(
                env, agent_name, agent_config.private.per, state_root
            )
            if requester_id is not None:
                discovered[(agent_name, state_root)] = requester_id
    return discovered


def _replace_private_instance_requesters(
    discovered: dict[tuple[str, Path], str], *, expected_revision: int
) -> bool:
    """Publish a full-pass index only when no hook changed it during discovery."""
    global _private_instance_requesters_revision  # noqa: PLW0603
    with _private_instance_requesters_lock:
        if _private_instance_requesters_revision != expected_revision:
            return False
        _private_instance_requesters.clear()
        _private_instance_requesters.update(discovered)
        _private_instance_requesters_revision += 1
        return True


def _remove_export_tree(env: _TriggerEnv, output_dir: Path) -> None:
    """Clear one anchored plugin-owned export target when its scope is revoked."""
    if not env.is_active():
        return
    try:
        clear_thread_export_root(
            output_dir,
            trusted_root=env.runtime_paths.storage_root,
            should_clear=env.is_active,
        )
    except (OSError, RuntimeError):
        env.logger.warning(
            "Skipping unsafe thread export cleanup",
            output_dir=str(output_dir),
        )


def _private_workspace_export_dir(
    env: _TriggerEnv, agent_name: str, state_root: Path
) -> Path | None:
    """Resolve one private instance's plugin-owned export directory."""
    try:
        workspace = resolve_agent_workspace_from_state_path(
            agent_name,
            env.config,
            runtime_paths=env.runtime_paths,
            state_storage_path=state_root,
            use_state_storage_path=True,
        )
    except ValueError:
        return None
    return (
        workspace.lexical_root / WORKSPACE_EXPORT_DIRNAME
        if workspace is not None
        else None
    )


def _shared_agent_export_targets(
    env: _TriggerEnv,
    agent_name: str,
    *,
    agent_user_id: str | None,
    options: _AgentExportSettings,
) -> tuple[ThreadExportTarget, ...]:
    """Return the membership-scoped target for one shared agent."""
    workspace_dir = agent_workspace_root_path(
        env.runtime_paths.storage_root, agent_name
    )
    output_dir = workspace_dir / WORKSPACE_EXPORT_DIRNAME
    if agent_user_id is None:
        _remove_export_tree(env, output_dir)
        env.logger.warning(
            "Skipping shared agent without persisted Matrix account",
            agent_name=agent_name,
        )
        return ()
    return (
        ThreadExportTarget(
            output_dir=output_dir,
            required_member_user_ids=(agent_user_id,),
            include_invited_rooms=options.invited_rooms,
            trusted_root=env.runtime_paths.storage_root,
        ),
    )


def _private_agent_export_targets(
    env: _TriggerEnv,
    agent_name: str,
    *,
    agent_user_id: str | None,
    options: _AgentExportSettings,
    worker_scope: WorkerScope,
    reconcile_instances: bool,
    private_instance_requesters: Mapping[tuple[str, Path], str],
) -> tuple[ThreadExportTarget, ...]:
    """Return membership-scoped private targets validated against core identity."""
    if reconcile_instances:
        state_roots = _private_instance_state_roots(
            env.runtime_paths.storage_root, agent_name
        )
    else:
        state_roots = tuple(
            sorted(
                root
                for cached_agent_name, root in private_instance_requesters
                if cached_agent_name == agent_name
                and root.is_dir()
                and not root.is_symlink()
            )
        )
    state_root_owners = tuple(
        (
            state_root,
            _private_instance_requester(env, agent_name, worker_scope, state_root),
        )
        for state_root in state_roots
    )
    if options.private_room_scope == "owner_and_agent" and agent_user_id is None:
        for state_root, _owner in state_root_owners:
            output_dir = _private_workspace_export_dir(env, agent_name, state_root)
            if output_dir is not None:
                _remove_export_tree(env, output_dir)
        env.logger.warning(
            "Skipping private agent without persisted Matrix account",
            agent_name=agent_name,
        )
        return ()
    targets: list[ThreadExportTarget] = []
    for state_root, owner in state_root_owners:
        cache_key = (agent_name, state_root)
        cached_owner = private_instance_requesters.get(cache_key)
        if owner is None or (not reconcile_instances and cached_owner != owner):
            if not reconcile_instances and cached_owner is not None:
                _evict_private_instance_requester(cache_key, cached_owner)
            output_dir = _private_workspace_export_dir(env, agent_name, state_root)
            if output_dir is not None:
                _remove_export_tree(env, output_dir)
            env.logger.warning(
                "Skipping private instance without valid core identity",
                agent_name=agent_name,
                instance_root=str(state_root),
            )
            continue
        output_dir = _private_workspace_export_dir(env, agent_name, state_root)
        if output_dir is None:
            continue
        required_member_user_ids = (owner,)
        if options.private_room_scope == "owner_and_agent":
            assert agent_user_id is not None
            required_member_user_ids += (agent_user_id,)
        targets.append(
            ThreadExportTarget(
                output_dir=output_dir,
                required_member_user_ids=required_member_user_ids,
                include_invited_rooms=options.invited_rooms,
                trusted_root=env.runtime_paths.storage_root,
            )
        )
    return tuple(targets)


def _agent_export_targets(
    env: _TriggerEnv,
    agent_name: str,
    *,
    options: _AgentExportSettings,
    reconcile_private_instances: bool,
    private_instance_requesters: Mapping[tuple[str, Path], str],
) -> tuple[ThreadExportTarget, ...]:
    """Return shared or requester-private export targets for one configured agent."""
    agent_config = env.config.agents.get(agent_name)
    if agent_config is None:
        return ()
    agent_user_id = managed_account_user_id(
        managed_account_key(agent_name),
        env.config.get_domain(env.runtime_paths),
        env.runtime_paths,
    )
    if agent_config.private is None:
        return _shared_agent_export_targets(
            env,
            agent_name,
            agent_user_id=agent_user_id,
            options=options,
        )
    return _private_agent_export_targets(
        env,
        agent_name,
        agent_user_id=agent_user_id,
        options=options,
        worker_scope=agent_config.private.per,
        reconcile_instances=reconcile_private_instances,
        private_instance_requesters=private_instance_requesters,
    )


def _cleanup_disabled_agent_exports(
    env: _TriggerEnv, enabled_agent_names: set[str]
) -> None:
    """Remove plugin-owned exports for configured agents no longer enabled in settings."""
    for agent_name, agent_config in env.config.agents.items():
        if agent_name in enabled_agent_names:
            continue
        if agent_config.private is None:
            workspace_dir = agent_workspace_root_path(
                env.runtime_paths.storage_root, agent_name
            )
            _remove_export_tree(env, workspace_dir / WORKSPACE_EXPORT_DIRNAME)
            continue
        for state_root in _private_instance_state_roots(
            env.runtime_paths.storage_root, agent_name
        ):
            output_dir = _private_workspace_export_dir(env, agent_name, state_root)
            if output_dir is not None:
                _remove_export_tree(env, output_dir)


async def _run_export_pass(
    env: _TriggerEnv, *, full_pass: bool, room_ids: frozenset[str]
) -> None:
    """Export the dirty rooms (or everything) into every enabled agent's workspace."""
    if not env.is_active():
        return
    requested = _requested_agents(env.settings)
    enabled = {
        name: options
        for name, options in requested.items()
        if name in env.config.agents
    }
    unknown = tuple(name for name in requested if name not in env.config.agents)
    if unknown:
        env.logger.warning(
            "thread-export settings list unknown agents", unknown_agents=list(unknown)
        )
    room_filters: tuple[str | None, ...] = (
        (None,) if full_pass else tuple(sorted(room_ids))
    )
    if full_pass:
        _cleanup_disabled_agent_exports(env, set(enabled))
        _cached_private_instance_requesters, index_revision = (
            _private_instance_requesters_snapshot()
        )
        private_instance_requesters = _discover_private_instance_requesters(
            env, set(enabled)
        )
        if not _replace_private_instance_requesters(
            private_instance_requesters, expected_revision=index_revision
        ):
            _queue_full_pass()
    else:
        private_instance_requesters, _index_revision = (
            _private_instance_requesters_snapshot()
        )
    target_records = [
        (agent_name, target, options)
        for agent_name, options in enabled.items()
        for target in _agent_export_targets(
            env,
            agent_name,
            options=options,
            reconcile_private_instances=full_pass,
            private_instance_requesters=private_instance_requesters,
        )
    ]
    targets = tuple(target for _, target, _ in target_records)
    for room_filter in room_filters:
        if not env.is_active():
            return
        try:
            target_stats = await export_threads_to_targets_once(
                config=env.config,
                runtime_paths=env.runtime_paths,
                targets=targets,
                room_filter=room_filter,
            )
        except Exception as exc:
            env.logger.warning(
                "Thread export pass failed",
                room_filter=room_filter,
                error=str(exc),
            )
            continue
        for (agent_name, _target, _options), stats in zip(
            target_records, target_stats, strict=True
        ):
            if not full_pass and not any(
                (
                    stats.rooms_exported,
                    stats.threads_exported,
                    stats.threads_unchanged,
                    stats.failures,
                )
            ):
                continue
            env.logger.info(
                "Exported threads to agent workspace",
                agent_name=agent_name,
                room_filter=room_filter,
                rooms_exported=stats.rooms_exported,
                threads_exported=stats.threads_exported,
                threads_unchanged=stats.threads_unchanged,
                failures=stats.failures,
            )


@hook(event="bot:ready", name="thread-export-startup", agents=(ROUTER_AGENT_NAME,))
async def queue_initial_full_pass(ctx: AgentLifecycleContext) -> None:
    """Queue one full export pass once the router bot is ready."""
    if not _accept_live_hook(ctx):
        return
    _queue_full_pass()
    _record_trigger(ctx)


@hook(event="config:reloaded", name="thread-export-config-reloaded", timeout_ms=1000)
async def queue_full_pass_after_config_reload(ctx: ConfigReloadedContext) -> None:
    """Queue a full pass after hot reload, including cleanup for removed agent settings."""
    if not _accept_live_hook(ctx):
        return
    _queue_full_pass()
    _record_trigger(ctx)


@hook(event="message:received", name="thread-export-on-message", timeout_ms=1000)
async def queue_room_on_message(ctx: MessageReceivedContext) -> None:
    """Queue the message's room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    if not _accept_live_hook(ctx):
        return
    _queue_room(ctx.envelope.room_id)
    _record_trigger(ctx)
    await _refresh_private_instance_requester(ctx, ctx.envelope.requester_id)


@hook(
    event="message:after_response", name="thread-export-after-response", timeout_ms=1000
)
async def queue_room_after_response(ctx: AfterResponseContext) -> None:
    """Queue the responded room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    if not _accept_live_hook(ctx):
        return
    _queue_room(ctx.result.envelope.room_id)
    _record_trigger(ctx)
    await _refresh_private_instance_requester(ctx, ctx.result.envelope.requester_id)
