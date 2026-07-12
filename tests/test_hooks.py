# ruff: noqa: INP001
"""Tests for the thread-export trigger hooks and single-flight runner."""

from __future__ import annotations

import asyncio
import contextlib
import sys
from importlib import util
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import pytest

from mindroom.hooks.decorators import get_hook_metadata

if TYPE_CHECKING:
    from types import ModuleType

PACKAGE_NAME = f"mindroom_plugin_{Path(__file__).resolve().parents[1].name.replace('-', '_')}"


def _load_hooks_module() -> ModuleType:
    """Load the plugin hooks module under its synthetic package name."""
    hooks_path = Path(__file__).resolve().parents[1] / "hooks.py"
    module_name = f"{PACKAGE_NAME}.hooks"
    sys.modules.pop(module_name, None)
    spec = util.spec_from_file_location(module_name, hooks_path)
    assert spec is not None
    assert spec.loader is not None
    module = util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _settings(agents: list[str] | None = None) -> dict[str, object]:
    return {"agents": agents or ["code"], "debounce_seconds": 0}


def _base_ctx(tmp_path: Path, settings: dict[str, object]) -> dict[str, object]:
    shared_agent = SimpleNamespace(private=None)
    return {
        "settings": settings,
        "config": SimpleNamespace(agents={"code": shared_agent, "research": shared_agent}),
        "runtime_paths": SimpleNamespace(storage_root=tmp_path),
        "logger": Mock(),
    }


def _message_ctx(tmp_path: Path, room_id: str, settings: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(envelope=SimpleNamespace(room_id=room_id), **_base_ctx(tmp_path, settings))


def _after_response_ctx(tmp_path: Path, room_id: str, settings: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(
        result=SimpleNamespace(envelope=SimpleNamespace(room_id=room_id)),
        **_base_ctx(tmp_path, settings),
    )


def _lifecycle_ctx(tmp_path: Path, settings: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(**_base_ctx(tmp_path, settings))


async def _drain(module: ModuleType, cycles: int = 50) -> None:
    """Give the runner task enough loop iterations to finish pending passes."""
    for _ in range(cycles):
        await asyncio.sleep(0)


async def _shutdown_runner(module: ModuleType) -> None:
    """Cancel the module's runner task so tests exit cleanly."""
    task = module._runner_tasks.get("runner")
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def test_hook_metadata_matches_spec() -> None:
    """The three hooks should target the right events, and only bot:ready is router-scoped."""
    module = _load_hooks_module()

    startup = get_hook_metadata(module.queue_initial_full_pass)
    assert startup is not None
    assert startup.event_name == "bot:ready"
    assert startup.agents == ("router",)

    on_message = get_hook_metadata(module.queue_room_on_message)
    assert on_message is not None
    assert on_message.event_name == "message:received"
    assert on_message.agents is None

    after_response = get_hook_metadata(module.queue_room_after_response)
    assert after_response is not None
    assert after_response.event_name == "message:after_response"
    assert after_response.agents is None


@pytest.mark.asyncio
async def test_hooks_inactive_without_agents_setting(tmp_path: Path) -> None:
    """Hooks should do nothing when the settings list no agents."""
    module = _load_hooks_module()
    empty_settings: dict[str, object] = {}

    await module.queue_room_on_message(_message_ctx(tmp_path, "!a:hs", empty_settings))
    await module.queue_room_after_response(_after_response_ctx(tmp_path, "!b:hs", empty_settings))
    await module.queue_initial_full_pass(_lifecycle_ctx(tmp_path, empty_settings))

    assert module._runner_tasks == {}
    assert module._pending_room_ids == set()
    assert module._full_pass_pending is False


@pytest.mark.asyncio
async def test_message_triggers_coalesce_into_one_pass(tmp_path: Path) -> None:
    """Repeated triggers should coalesce into one pass with one export per (agent, room)."""
    module = _load_hooks_module()
    module.export_threads_once = AsyncMock(return_value=Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0))
    settings = _settings()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await module.queue_room_after_response(_after_response_ctx(tmp_path, "!beta:hs", settings))
    await _drain(module)
    await _shutdown_runner(module)

    assert module.export_threads_once.await_count == 2
    room_filters = {call.kwargs["room_filter"] for call in module.export_threads_once.await_args_list}
    assert room_filters == {"!alpha:hs", "!beta:hs"}
    expected_output_dir = tmp_path / "agents" / "code" / "workspace" / "thread_exports"
    for call in module.export_threads_once.await_args_list:
        assert call.kwargs["prefer_cache"] is True
        assert call.kwargs["output_dir"] == expected_output_dir
        assert call.kwargs["required_member_user_id"] is None


@pytest.mark.asyncio
async def test_bot_ready_runs_full_pass(tmp_path: Path) -> None:
    """bot:ready should queue one full export pass per enabled agent."""
    module = _load_hooks_module()
    module.export_threads_once = AsyncMock(return_value=Mock(rooms_exported=2, threads_exported=3, threads_unchanged=0, failures=0))
    settings = _settings(agents=["code", "research"])

    await module.queue_initial_full_pass(_lifecycle_ctx(tmp_path, settings))
    await _drain(module)
    await _shutdown_runner(module)

    assert module.export_threads_once.await_count == 2
    for call in module.export_threads_once.await_args_list:
        assert call.kwargs["room_filter"] is None
    output_dirs = {call.kwargs["output_dir"] for call in module.export_threads_once.await_args_list}
    assert output_dirs == {
        tmp_path / "agents" / "code" / "workspace" / "thread_exports",
        tmp_path / "agents" / "research" / "workspace" / "thread_exports",
    }


@pytest.mark.asyncio
async def test_full_pass_subsumes_pending_rooms(tmp_path: Path) -> None:
    """A pending full pass should replace per-room exports in the same drain."""
    module = _load_hooks_module()
    module.export_threads_once = AsyncMock(return_value=Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0))
    settings = _settings()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await module.queue_initial_full_pass(_lifecycle_ctx(tmp_path, settings))
    await _drain(module)
    await _shutdown_runner(module)

    assert module.export_threads_once.await_count == 1
    assert module.export_threads_once.await_args.kwargs["room_filter"] is None


@pytest.mark.asyncio
async def test_mid_pass_triggers_drain_in_one_followup(tmp_path: Path) -> None:
    """Triggers arriving during a pass should coalesce into exactly one follow-up pass."""
    module = _load_hooks_module()
    release = asyncio.Event()
    started = asyncio.Event()

    async def _blocking_export(**_kwargs: object) -> Mock:
        started.set()
        await release.wait()
        return Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0)

    module.export_threads_once = AsyncMock(side_effect=_blocking_export)
    settings = _settings()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await asyncio.wait_for(started.wait(), timeout=1)

    await module.queue_room_on_message(_message_ctx(tmp_path, "!beta:hs", settings))
    await module.queue_room_on_message(_message_ctx(tmp_path, "!gamma:hs", settings))
    release.set()
    await _drain(module)
    await _shutdown_runner(module)

    assert module.export_threads_once.await_count == 3
    room_filters = [call.kwargs["room_filter"] for call in module.export_threads_once.await_args_list]
    assert room_filters[0] == "!alpha:hs"
    assert set(room_filters[1:]) == {"!beta:hs", "!gamma:hs"}


@pytest.mark.asyncio
async def test_export_failure_does_not_kill_runner_or_other_agents(tmp_path: Path) -> None:
    """One agent's failing export should not block other agents or later passes."""
    module = _load_hooks_module()
    module.export_threads_once = AsyncMock(
        side_effect=[
            RuntimeError("export failed"),
            Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0),
        ],
    )
    settings = _settings(agents=["code", "research"])

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await _drain(module)

    assert module.export_threads_once.await_count == 2
    runner = module._runner_tasks["runner"]
    assert not runner.done()
    await _shutdown_runner(module)


@pytest.mark.asyncio
async def test_unknown_agents_are_warned_and_skipped(tmp_path: Path) -> None:
    """Settings naming unknown agents should warn and export only for known ones."""
    module = _load_hooks_module()
    module.export_threads_once = AsyncMock(return_value=Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0))
    logger = Mock()
    env = module._TriggerEnv(
        config=SimpleNamespace(agents={"code": SimpleNamespace(private=None)}),
        runtime_paths=SimpleNamespace(storage_root=tmp_path),
        settings={"agents": ["ghost", "code"]},
        logger=logger,
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert module.export_threads_once.await_count == 1
    assert module.export_threads_once.await_args.kwargs["output_dir"] == (
        tmp_path / "agents" / "code" / "workspace" / "thread_exports"
    )
    logger.warning.assert_called_once()
    assert logger.warning.call_args.kwargs["unknown_agents"] == ["ghost"]


@pytest.mark.asyncio
async def test_private_agent_exports_scoped_to_resolved_owners(tmp_path: Path) -> None:
    """Private instances should export only for resolvable owners, scoped to the owner's rooms."""
    from mindroom.config.agent import AgentConfig, AgentPrivateConfig
    from mindroom.config.auth import AuthorizationConfig
    from mindroom.config.main import Config
    from mindroom.tool_system.worker_routing import private_instance_state_root_for_requester

    module = _load_hooks_module()
    module.export_threads_once = AsyncMock(return_value=Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0))
    config = Config(
        agents={"secret": AgentConfig(display_name="Secret", private=AgentPrivateConfig(per="user"))},
        authorization=AuthorizationConfig(global_users=["@alice:hs", "@bob:hs"]),
    )
    runtime_paths = SimpleNamespace(storage_root=tmp_path, env_value=lambda _name, default=None: default)
    expected_dirs = {}
    for requester_id in ("@alice:hs", "@bob:hs"):
        instance_root = private_instance_state_root_for_requester(
            tmp_path,
            requester_id=requester_id,
            agent_name="secret",
            worker_scope="user",
            runtime_paths=runtime_paths,
        )
        assert instance_root is not None
        instance_root.mkdir(parents=True)
        expected_dirs[requester_id] = instance_root / "secret_data" / "thread_exports"
    (tmp_path / "private_instances" / "ghost-0000000000000000" / "secret").mkdir(parents=True)
    logger = Mock()
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=logger,
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert module.export_threads_once.await_count == 2
    exported = {
        call.kwargs["required_member_user_id"]: call.kwargs["output_dir"]
        for call in module.export_threads_once.await_args_list
    }
    assert exported == expected_dirs
    orphan_warnings = [
        call for call in logger.warning.call_args_list if "without resolvable owner" in call.args[0]
    ]
    assert len(orphan_warnings) == 1
    assert "ghost-0000000000000000" in orphan_warnings[0].kwargs["instance_root"]
