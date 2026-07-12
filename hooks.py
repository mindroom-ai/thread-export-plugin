# ruff: noqa: INP001
"""Automatically export Matrix threads to YAML in enabled agents' workspaces.

Message hooks only record which room changed; one module-global runner task debounces triggers and
runs cache-first export passes (``export_threads_once(prefer_cache=True)``) into every enabled
agent's workspace, so bursts coalesce and at most one pass runs at a time.
The runner task must stay inside the module-global ``_runner_tasks`` dict: plugin hot reload only
cancels tasks it finds in module globals or one level inside a global dict/list/set.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mindroom.constants import ROUTER_AGENT_NAME
from mindroom.hooks import AfterResponseContext, AgentLifecycleContext, MessageReceivedContext, hook
from mindroom.thread_export import export_threads_once
from mindroom.tool_system.worker_routing import agent_workspace_root_path
from mindroom.workspaces import resolve_agent_workspace_from_state_path

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from structlog.stdlib import BoundLogger

    from mindroom.config.main import Config
    from mindroom.constants import RuntimePaths
    from mindroom.hooks.context import HookContext

WORKSPACE_EXPORT_DIRNAME = "thread_exports"
PRIVATE_INSTANCES_DIRNAME = "private_instances"
DEFAULT_DEBOUNCE_SECONDS = 2.0

_runner_tasks: dict[str, asyncio.Task[None]] = {}
_pending_room_ids: set[str] = set()
_full_pass_pending = False
_wakeup: asyncio.Event | None = None
_latest_env: _TriggerEnv | None = None


@dataclass(frozen=True)
class _TriggerEnv:
    """Runtime context captured from the most recent triggering hook."""

    config: Config
    runtime_paths: RuntimePaths
    settings: Mapping[str, object]
    logger: BoundLogger


def _requested_agents(settings: Mapping[str, object]) -> tuple[str, ...]:
    """Return the deduplicated agent names listed in the plugin settings."""
    raw = settings.get("agents")
    if not isinstance(raw, (list, tuple)):
        return ()
    names = [item.strip() for item in raw if isinstance(item, str) and item.strip()]
    return tuple(dict.fromkeys(names))


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
        _runner_tasks["runner"] = asyncio.create_task(_run_export_loop(), name="thread-export-runner")
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
            await _run_export_pass(env, full_pass=full_pass, room_ids=room_ids)
        except Exception:
            env.logger.exception("Thread export pass crashed")


def _private_instance_state_roots(storage_root: Path, agent_name: str) -> tuple[Path, ...]:
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


def _agent_export_dirs(env: _TriggerEnv, agent_name: str) -> tuple[Path, ...]:
    """Return the export target dirs for one agent: shared workspace, or one per private instance."""
    agent_config = env.config.agents.get(agent_name)
    if agent_config is None:
        return ()
    if agent_config.private is None:
        return (agent_workspace_root_path(env.runtime_paths.storage_root, agent_name) / WORKSPACE_EXPORT_DIRNAME,)
    export_dirs: list[Path] = []
    for state_root in _private_instance_state_roots(env.runtime_paths.storage_root, agent_name):
        workspace = resolve_agent_workspace_from_state_path(
            agent_name,
            env.config,
            runtime_paths=env.runtime_paths,
            state_storage_path=state_root,
            use_state_storage_path=True,
        )
        if workspace is not None:
            export_dirs.append(workspace.root / WORKSPACE_EXPORT_DIRNAME)
    return tuple(export_dirs)


async def _run_export_pass(env: _TriggerEnv, *, full_pass: bool, room_ids: frozenset[str]) -> None:
    """Export the dirty rooms (or everything) into every enabled agent's workspace."""
    requested = _requested_agents(env.settings)
    enabled = tuple(name for name in requested if name in env.config.agents)
    unknown = tuple(name for name in requested if name not in env.config.agents)
    if unknown:
        env.logger.warning("thread-export settings list unknown agents", unknown_agents=list(unknown))
    room_filters: tuple[str | None, ...] = (None,) if full_pass else tuple(sorted(room_ids))
    targets = [
        (agent_name, output_dir) for agent_name in enabled for output_dir in _agent_export_dirs(env, agent_name)
    ]
    for agent_name, output_dir in targets:
        for room_filter in room_filters:
            try:
                stats = await export_threads_once(
                    config=env.config,
                    runtime_paths=env.runtime_paths,
                    output_dir=output_dir,
                    room_filter=room_filter,
                    prefer_cache=True,
                )
            except Exception as exc:
                env.logger.warning(
                    "Thread export pass failed",
                    agent_name=agent_name,
                    room_filter=room_filter,
                    error=str(exc),
                )
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
    if not _requested_agents(ctx.settings):
        return
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


@hook(event="message:after_response", name="thread-export-after-response", timeout_ms=1000)
async def queue_room_after_response(ctx: AfterResponseContext) -> None:
    """Queue the responded room for re-export."""
    if not _requested_agents(ctx.settings):
        return
    _pending_room_ids.add(ctx.result.envelope.room_id)
    _record_trigger(ctx)
