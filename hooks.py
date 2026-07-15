# ruff: noqa: INP001
"""Automatically export Matrix threads to YAML in enabled agents' workspaces.

Message hooks only record which room changed; one module-global runner task debounces triggers and
runs cache-first export passes (``export_threads_once(prefer_cache=True)``) into every enabled
agent's workspace, so bursts coalesce and at most one pass runs at a time.
Each pass executes on a private event loop in a worker thread: export reconciliation re-reads and
re-parses every exported thread YAML synchronously, which blocked the runtime loop for over five
seconds per pass (``event_loop_stall_detected``) when run inline.
The runner task must stay inside the module-global ``_runner_tasks`` dict: plugin hot reload only
cancels tasks it finds in module globals or one level inside a global dict/list/set.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

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
from mindroom.thread_export import ThreadExportTarget, export_threads_to_targets_once
from mindroom.tool_system.worker_routing import (
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

WORKSPACE_EXPORT_DIRNAME = "thread_exports"
PRIVATE_INSTANCES_DIRNAME = "private_instances"
DEFAULT_DEBOUNCE_SECONDS = 2.0
_MATRIX_USER_ID_PATTERN = re.compile(r"@[^:\s]+:\S+")

_runner_tasks: dict[str, asyncio.Task[None]] = {}
_pending_room_ids: set[str] = set()
_full_pass_pending = False
_wakeup: asyncio.Event | None = None
_latest_env: _TriggerEnv | None = None

# Hot reload replaces this module but cannot interrupt a worker thread mid-pass, so the
# single-flight lock lives on the long-lived core package where every plugin copy finds it.
_EXPORT_PASS_LOCK: threading.Lock = thread_export_pkg.__dict__.setdefault(
    "_thread_export_plugin_pass_lock",
    threading.Lock(),
)


@dataclass(frozen=True)
class _TriggerEnv:
    """Runtime context captured from the most recent triggering hook."""

    config: Config
    runtime_paths: RuntimePaths
    settings: Mapping[str, object]
    logger: BoundLogger


@dataclass(frozen=True)
class _AgentExportSettings:
    """Per-agent export options from the plugin settings."""

    invited_rooms: bool = True


def _agent_options(options: object) -> _AgentExportSettings:
    """Parse one agent's option mapping, tolerating missing or bare entries."""
    if not isinstance(options, Mapping):
        return _AgentExportSettings()
    invited_rooms = options.get("invited_rooms", True)
    return _AgentExportSettings(
        invited_rooms=invited_rooms if isinstance(invited_rooms, bool) else True
    )


def _requested_agents(
    settings: Mapping[str, object],
) -> dict[str, _AgentExportSettings]:
    """Return per-agent export options for the agents listed in the plugin settings.

    ``agents`` accepts a plain list of names (all defaults) or a mapping of name to options
    (currently ``invited_rooms``, default true).
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


def _debounce_seconds(settings: Mapping[str, object]) -> float:
    """Return the configured trigger debounce in seconds."""
    raw = settings.get("debounce_seconds", DEFAULT_DEBOUNCE_SECONDS)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return max(float(raw), 0.0)
    return DEFAULT_DEBOUNCE_SECONDS


def _record_trigger(ctx: HookContext) -> None:
    """Capture the trigger context and wake the runner, starting it when needed."""
    global _latest_env, _wakeup  # noqa: PLW0603
    _latest_env = _TriggerEnv(
        config=ctx.config,
        runtime_paths=ctx.runtime_paths,
        settings=ctx.settings,
        logger=ctx.logger,
    )
    if _wakeup is None:
        _wakeup = asyncio.Event()
    runner = _runner_tasks.get("runner")
    if runner is None or runner.done():
        _runner_tasks["runner"] = asyncio.create_task(
            _run_export_loop(), name="thread-export-runner"
        )
    _wakeup.set()


def _drain_pending() -> tuple[bool, frozenset[str]]:
    """Atomically consume the pending full-pass flag and dirty room set."""
    global _full_pass_pending  # noqa: PLW0603
    full_pass = _full_pass_pending
    _full_pass_pending = False
    room_ids = frozenset(_pending_room_ids)
    _pending_room_ids.clear()
    return full_pass, room_ids


async def _run_export_loop() -> None:
    """Drain export triggers one debounced single-flight pass at a time."""
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
        asyncio.run(_run_export_pass(env, full_pass=full_pass, room_ids=room_ids))


def _private_instance_state_roots(
    storage_root: Path, agent_name: str
) -> tuple[Path, ...]:
    """Return existing private-instance state roots for one private agent."""
    instances_root = storage_root / PRIVATE_INSTANCES_DIRNAME
    if not instances_root.is_dir():
        return ()
    instance_dir_name = agent_name.strip("_") or agent_name
    return tuple(
        sorted(
            scope_dir / instance_dir_name
            for scope_dir in instances_root.iterdir()
            if scope_dir.is_dir() and (scope_dir / instance_dir_name).is_dir()
        ),
    )


def _authorized_requester_candidates(config: Config) -> tuple[str, ...]:
    """Return authorized Matrix user IDs that may own private agent instances."""
    authorization = config.authorization
    raw = [
        *authorization.global_users,
        *(user for users in authorization.room_permissions.values() for user in users),
        *(
            user
            for users in authorization.agent_reply_permissions.values()
            for user in users
        ),
        *authorization.aliases,
    ]
    return tuple(
        dict.fromkeys(user for user in raw if _MATRIX_USER_ID_PATTERN.fullmatch(user))
    )


def _private_instance_owners(
    env: _TriggerEnv, agent_name: str, worker_scope: str
) -> dict[Path, str]:
    """Map existing private-instance state roots to the authorized requester that owns them."""
    owners: dict[Path, str] = {}
    for requester_id in _authorized_requester_candidates(env.config):
        candidate_root = private_instance_state_root_for_requester(
            env.runtime_paths.storage_root,
            requester_id=requester_id,
            agent_name=agent_name,
            worker_scope=worker_scope,
            runtime_paths=env.runtime_paths,
        )
        if candidate_root is not None:
            owners[candidate_root.resolve()] = requester_id
    return owners


def _remove_export_tree(output_dir: Path) -> None:
    """Remove one plugin-owned export tree when its scope is revoked."""
    if output_dir.is_symlink() or output_dir.is_file():
        output_dir.unlink()
    elif output_dir.is_dir():
        shutil.rmtree(output_dir)


def _private_workspace_export_dir(
    env: _TriggerEnv, agent_name: str, state_root: Path
) -> Path | None:
    """Resolve one private instance's plugin-owned export directory."""
    workspace = resolve_agent_workspace_from_state_path(
        agent_name,
        env.config,
        runtime_paths=env.runtime_paths,
        state_storage_path=state_root,
        use_state_storage_path=True,
    )
    return workspace.root / WORKSPACE_EXPORT_DIRNAME if workspace is not None else None


def _agent_export_targets(
    env: _TriggerEnv,
    agent_name: str,
    *,
    include_invited_rooms: bool,
) -> tuple[ThreadExportTarget, ...]:
    """Return export targets for one agent: shared workspace, or one owner-scoped target per instance.

    Private instances export only rooms their owner is a member of; instances whose owner cannot be
    resolved from the authorization config are skipped entirely (fail closed).
    """
    agent_config = env.config.agents.get(agent_name)
    if agent_config is None:
        return ()
    if agent_config.private is None:
        workspace_dir = agent_workspace_root_path(
            env.runtime_paths.storage_root, agent_name
        )
        output_dir = workspace_dir / WORKSPACE_EXPORT_DIRNAME
        agent_user_id = managed_account_user_id(
            managed_account_key(agent_name),
            env.config.get_domain(env.runtime_paths),
            env.runtime_paths,
        )
        if agent_user_id is None:
            _remove_export_tree(output_dir)
            env.logger.warning(
                "Skipping shared agent without persisted Matrix account",
                agent_name=agent_name,
            )
            return ()
        return (
            ThreadExportTarget(
                output_dir=output_dir,
                required_member_user_id=agent_user_id,
                include_invited_rooms=include_invited_rooms,
            ),
        )
    owners = _private_instance_owners(env, agent_name, agent_config.private.per)
    targets: list[ThreadExportTarget] = []
    for state_root in _private_instance_state_roots(
        env.runtime_paths.storage_root, agent_name
    ):
        output_dir = _private_workspace_export_dir(env, agent_name, state_root)
        if output_dir is None:
            continue
        owner = owners.get(state_root.resolve())
        if owner is None:
            _remove_export_tree(output_dir)
            env.logger.warning(
                "Skipping private instance without resolvable owner",
                agent_name=agent_name,
                instance_root=str(state_root),
            )
            continue
        targets.append(
            ThreadExportTarget(
                output_dir=output_dir,
                required_member_user_id=owner,
                include_invited_rooms=include_invited_rooms,
            ),
        )
    return tuple(targets)


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
            _remove_export_tree(workspace_dir / WORKSPACE_EXPORT_DIRNAME)
            continue
        for state_root in _private_instance_state_roots(
            env.runtime_paths.storage_root, agent_name
        ):
            output_dir = _private_workspace_export_dir(env, agent_name, state_root)
            if output_dir is not None:
                _remove_export_tree(output_dir)


async def _run_export_pass(
    env: _TriggerEnv, *, full_pass: bool, room_ids: frozenset[str]
) -> None:
    """Export the dirty rooms (or everything) into every enabled agent's workspace."""
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
    target_records = [
        (agent_name, target, options)
        for agent_name, options in enabled.items()
        for target in _agent_export_targets(
            env,
            agent_name,
            include_invited_rooms=options.invited_rooms,
        )
    ]
    targets = tuple(target for _, target, _ in target_records)
    for room_filter in room_filters:
        try:
            target_stats = await export_threads_to_targets_once(
                config=env.config,
                runtime_paths=env.runtime_paths,
                targets=targets,
                room_filter=room_filter,
                prefer_cache=True,
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
    global _full_pass_pending  # noqa: PLW0603
    _full_pass_pending = True
    _record_trigger(ctx)


@hook(event="config:reloaded", name="thread-export-config-reloaded", timeout_ms=1000)
async def queue_full_pass_after_config_reload(ctx: ConfigReloadedContext) -> None:
    """Queue a full pass after hot reload, including cleanup for removed agent settings."""
    global _full_pass_pending  # noqa: PLW0603
    _full_pass_pending = True
    _record_trigger(ctx)


@hook(event="message:received", name="thread-export-on-message", timeout_ms=1000)
async def queue_room_on_message(ctx: MessageReceivedContext) -> None:
    """Queue the message's room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    _pending_room_ids.add(ctx.envelope.room_id)
    _record_trigger(ctx)


@hook(
    event="message:after_response", name="thread-export-after-response", timeout_ms=1000
)
async def queue_room_after_response(ctx: AfterResponseContext) -> None:
    """Queue the responded room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    _pending_room_ids.add(ctx.result.envelope.room_id)
    _record_trigger(ctx)
