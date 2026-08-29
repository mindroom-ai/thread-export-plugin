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
import json
import re
import shutil
import threading
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, cast

import mindroom.thread_export as thread_export_pkg
from mindroom.constants import ROUTER_AGENT_NAME
from mindroom.durable_write import write_json_file_durable
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
PRIVATE_INSTANCE_OWNER_FILENAME = ".mindroom-thread-export-owner.json"
PRIVATE_INSTANCE_OWNER_FORMAT = "mindroom-thread-export-owner"
PRIVATE_INSTANCE_OWNER_VERSION = 1
DEFAULT_DEBOUNCE_SECONDS = 2.0
_MATRIX_USER_ID_PATTERN = re.compile(r"@[^:\s]+:\S+")

type PrivateRoomScope = Literal["owner", "owner_and_agent"]
DEFAULT_PRIVATE_ROOM_SCOPE: PrivateRoomScope = "owner_and_agent"

_runner_tasks: dict[str, asyncio.Task[None]] = {}
_pending_room_ids: set[str] = set()
_observed_requester_ids: set[str] = set()
_owner_marker_roots: set[Path] = set()
_conflicting_owner_roots: set[Path] = set()
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
    requester_candidates: tuple[str, ...] = ()
    conflicting_owner_roots: frozenset[Path] = frozenset()


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


def _debounce_seconds(settings: Mapping[str, object]) -> float:
    """Return the configured trigger debounce in seconds."""
    raw = settings.get("debounce_seconds", DEFAULT_DEBOUNCE_SECONDS)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return max(float(raw), 0.0)
    return DEFAULT_DEBOUNCE_SECONDS


def _enabled_private_instance_roots(
    ctx: HookContext,
    requester_id: str,
) -> tuple[Path, ...]:
    """Return this requester's existing instances among the enabled private agents."""
    if _MATRIX_USER_ID_PATTERN.fullmatch(requester_id) is None:
        return ()
    roots: list[Path] = []
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
        if state_root is not None and state_root.is_dir():
            roots.append(state_root)
    return tuple(roots)


def _requester_has_enabled_private_instance(
    ctx: HookContext,
    requester_id: str,
) -> bool:
    """Return whether this requester owns an existing enabled private instance."""
    return bool(_enabled_private_instance_roots(ctx, requester_id))


def _persist_private_instance_owner(
    state_root: Path,
    requester_id: str,
    logger: BoundLogger,
) -> bool:
    """Durably remember this state root's requester without blocking exports on I/O failure."""
    try:
        write_json_file_durable(
            state_root / PRIVATE_INSTANCE_OWNER_FILENAME,
            {
                "format": PRIVATE_INSTANCE_OWNER_FORMAT,
                "version": PRIVATE_INSTANCE_OWNER_VERSION,
                "requester_id": requester_id,
            },
            sort_keys=True,
            trailing_newline=True,
        )
    except OSError as exc:
        logger.warning(
            "Could not persist private instance owner",
            instance_root=str(state_root),
            error=str(exc),
        )
        return False
    return True


def _remember_private_instance_requester(
    ctx: HookContext,
    requester_id: str,
) -> None:
    """Retain one requester only while they own an enabled private instance."""
    state_roots = _enabled_private_instance_roots(ctx, requester_id)
    if not state_roots:
        return
    for state_root in state_roots:
        resolved_root = state_root.resolve()
        marker_needs_write = (
            resolved_root not in _owner_marker_roots
            or resolved_root in _conflicting_owner_roots
        )
        if marker_needs_write and _persist_private_instance_owner(
            state_root, requester_id, ctx.logger
        ):
            _owner_marker_roots.add(resolved_root)
            _conflicting_owner_roots.discard(resolved_root)
    _observed_requester_ids.add(requester_id)


def _prune_private_instance_requesters(ctx: HookContext) -> None:
    """Drop observed requesters whose enabled private instances no longer exist."""
    retained = {
        requester_id
        for requester_id in _observed_requester_ids
        if _requester_has_enabled_private_instance(ctx, requester_id)
    }
    _observed_requester_ids.intersection_update(retained)
    retained_roots = {
        state_root.resolve()
        for requester_id in retained
        for state_root in _enabled_private_instance_roots(ctx, requester_id)
    }
    _owner_marker_roots.intersection_update(retained_roots)


def _record_trigger(ctx: HookContext) -> None:
    """Capture the trigger context and wake the runner, starting it when needed."""
    global _latest_env, _wakeup  # noqa: PLW0603
    _latest_env = _TriggerEnv(
        config=ctx.config,
        runtime_paths=ctx.runtime_paths,
        settings=ctx.settings,
        logger=ctx.logger,
        requester_candidates=tuple(_observed_requester_ids),
        conflicting_owner_roots=frozenset(_conflicting_owner_roots),
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
        env = replace(
            _latest_env or env,
            requester_candidates=tuple(_observed_requester_ids),
            conflicting_owner_roots=frozenset(_conflicting_owner_roots),
        )
        try:
            recovered_requester_ids, conflicting_owner_roots = await asyncio.to_thread(
                _run_export_pass_blocking, env, full_pass=full_pass, room_ids=room_ids
            )
        except Exception:
            env.logger.exception("Thread export pass crashed")
        else:
            _observed_requester_ids.update(recovered_requester_ids)
            if full_pass:
                _conflicting_owner_roots.clear()
                _conflicting_owner_roots.update(conflicting_owner_roots)


def _run_export_pass_blocking(
    env: _TriggerEnv, *, full_pass: bool, room_ids: frozenset[str]
) -> tuple[tuple[str, ...], tuple[Path, ...]]:
    """Run one export pass to completion on a private event loop in the calling thread."""
    with _EXPORT_PASS_LOCK:
        return asyncio.run(
            _run_export_pass(env, full_pass=full_pass, room_ids=room_ids)
        )


def _private_instance_state_roots(
    storage_root: Path, agent_name: str
) -> tuple[Path, ...]:
    """Return existing private-instance state roots for one private agent."""
    instances_root = storage_root / PRIVATE_INSTANCES_DIRNAME
    if not instances_root.is_dir():
        return ()
    instance_dir_name = agent_state_root_path(storage_root, agent_name).name
    return tuple(
        sorted(
            scope_dir / instance_dir_name
            for scope_dir in instances_root.iterdir()
            if scope_dir.is_dir() and (scope_dir / instance_dir_name).is_dir()
        ),
    )


def _authorized_requester_candidates(config: Config) -> tuple[str, ...]:
    """Return statically configured Matrix IDs that may own private instances."""
    authorization = config.authorization
    raw = [
        *config.administrators,
        *config.room_defaults.invite_users,
        *(user for room in config.rooms.values() for user in (room.invite_users or ())),
        *(
            user
            for entity in (
                *config.agents.values(),
                *config.teams.values(),
                config.router,
            )
            if entity.access is not None
            for user in entity.access.users
        ),
        *authorization.aliases,
    ]
    return tuple(
        dict.fromkeys(user for user in raw if _MATRIX_USER_ID_PATTERN.fullmatch(user))
    )


def _private_instance_owners(
    env: _TriggerEnv, agent_name: str, worker_scope: WorkerScope
) -> dict[Path, str]:
    """Map private-instance roots to statically configured or observed requesters."""
    owners: dict[Path, str] = {}
    requester_candidates = dict.fromkeys(
        (*_authorized_requester_candidates(env.config), *env.requester_candidates)
    )
    for requester_id in requester_candidates:
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


def _persisted_private_instance_owner(
    state_root: Path,
    *,
    agent_name: str,
    worker_scope: WorkerScope,
    runtime_paths: RuntimePaths,
) -> str | None:
    """Return a durable owner only when it resolves back to this private instance."""
    marker_path = state_root / PRIVATE_INSTANCE_OWNER_FILENAME
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    requester_id = payload.get("requester_id")
    if (
        payload.get("format") != PRIVATE_INSTANCE_OWNER_FORMAT
        or payload.get("version") != PRIVATE_INSTANCE_OWNER_VERSION
        or not isinstance(requester_id, str)
        or _MATRIX_USER_ID_PATTERN.fullmatch(requester_id) is None
    ):
        return None
    candidate_root = private_instance_state_root_for_requester(
        runtime_paths.storage_root,
        requester_id=requester_id,
        agent_name=agent_name,
        worker_scope=worker_scope,
        runtime_paths=runtime_paths,
    )
    if candidate_root is None or candidate_root.resolve() != state_root.resolve():
        return None
    return requester_id


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
        _remove_export_tree(output_dir)
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
    recovered_requester_ids: set[str],
    conflicting_owner_roots: set[Path],
) -> tuple[ThreadExportTarget, ...]:
    """Return membership-scoped private targets, discovering orphans when requested."""
    owners = _private_instance_owners(env, agent_name, worker_scope)
    persisted_owners: dict[Path, str] = {}
    if reconcile_instances:
        state_roots = _private_instance_state_roots(
            env.runtime_paths.storage_root, agent_name
        )
        for state_root in state_roots:
            resolved_root = state_root.resolve()
            owner = _persisted_private_instance_owner(
                state_root,
                agent_name=agent_name,
                worker_scope=worker_scope,
                runtime_paths=env.runtime_paths,
            )
            if owner is not None:
                configured_owner = owners.get(resolved_root)
                if configured_owner is not None and configured_owner != owner:
                    owners.pop(resolved_root)
                    conflicting_owner_roots.add(resolved_root)
                    continue
                persisted_owners[resolved_root] = owner
                owners[resolved_root] = owner
                recovered_requester_ids.add(owner)
    else:
        state_roots = tuple(sorted(root for root in owners if root.is_dir()))
    if options.private_room_scope == "owner_and_agent" and agent_user_id is None:
        for state_root in state_roots:
            output_dir = _private_workspace_export_dir(env, agent_name, state_root)
            if output_dir is not None:
                _remove_export_tree(output_dir)
        env.logger.warning(
            "Skipping private agent without persisted Matrix account",
            agent_name=agent_name,
        )
        return ()
    targets: list[ThreadExportTarget] = []
    active_conflicts = (
        conflicting_owner_roots if reconcile_instances else env.conflicting_owner_roots
    )
    for state_root in state_roots:
        output_dir = _private_workspace_export_dir(env, agent_name, state_root)
        if output_dir is None:
            continue
        resolved_root = state_root.resolve()
        if resolved_root in active_conflicts:
            _remove_export_tree(output_dir)
            env.logger.warning(
                "Skipping private instance with conflicting owners",
                agent_name=agent_name,
                instance_root=str(state_root),
            )
            continue
        owner = owners.get(resolved_root)
        if owner is None:
            _remove_export_tree(output_dir)
            env.logger.warning(
                "Skipping private instance without resolvable owner",
                agent_name=agent_name,
                instance_root=str(state_root),
            )
            continue
        if reconcile_instances and persisted_owners.get(resolved_root) != owner:
            _persist_private_instance_owner(state_root, owner, env.logger)
        required_member_user_ids = (owner,)
        if options.private_room_scope == "owner_and_agent":
            assert agent_user_id is not None
            required_member_user_ids += (agent_user_id,)
        targets.append(
            ThreadExportTarget(
                output_dir=output_dir,
                required_member_user_ids=required_member_user_ids,
                include_invited_rooms=options.invited_rooms,
            )
        )
    return tuple(targets)


def _agent_export_targets(
    env: _TriggerEnv,
    agent_name: str,
    *,
    options: _AgentExportSettings,
    reconcile_private_instances: bool,
    recovered_requester_ids: set[str],
    conflicting_owner_roots: set[Path],
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
        recovered_requester_ids=recovered_requester_ids,
        conflicting_owner_roots=conflicting_owner_roots,
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
) -> tuple[tuple[str, ...], tuple[Path, ...]]:
    """Export the dirty rooms (or everything) into every enabled agent's workspace."""
    recovered_requester_ids: set[str] = set()
    conflicting_owner_roots: set[Path] = set()
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
            options=options,
            reconcile_private_instances=full_pass,
            recovered_requester_ids=recovered_requester_ids,
            conflicting_owner_roots=conflicting_owner_roots,
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
    return (
        tuple(sorted(recovered_requester_ids)),
        tuple(sorted(conflicting_owner_roots)),
    )


@hook(event="bot:ready", name="thread-export-startup", agents=(ROUTER_AGENT_NAME,))
async def queue_initial_full_pass(ctx: AgentLifecycleContext) -> None:
    """Queue one full export pass once the router bot is ready."""
    global _full_pass_pending  # noqa: PLW0603
    _prune_private_instance_requesters(ctx)
    _full_pass_pending = True
    _record_trigger(ctx)


@hook(event="config:reloaded", name="thread-export-config-reloaded", timeout_ms=1000)
async def queue_full_pass_after_config_reload(ctx: ConfigReloadedContext) -> None:
    """Queue a full pass after hot reload, including cleanup for removed agent settings."""
    global _full_pass_pending  # noqa: PLW0603
    _prune_private_instance_requesters(ctx)
    _full_pass_pending = True
    _record_trigger(ctx)


@hook(event="message:received", name="thread-export-on-message", timeout_ms=1000)
async def queue_room_on_message(ctx: MessageReceivedContext) -> None:
    """Queue the message's room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    _remember_private_instance_requester(ctx, ctx.envelope.requester_id)
    _pending_room_ids.add(ctx.envelope.room_id)
    _record_trigger(ctx)


@hook(
    event="message:after_response", name="thread-export-after-response", timeout_ms=1000
)
async def queue_room_after_response(ctx: AfterResponseContext) -> None:
    """Queue the responded room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    _remember_private_instance_requester(ctx, ctx.result.envelope.requester_id)
    _pending_room_ids.add(ctx.result.envelope.room_id)
    _record_trigger(ctx)
