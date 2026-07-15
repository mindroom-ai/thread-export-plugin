# ruff: noqa: INP001
"""Tests for the thread-export trigger hooks and single-flight runner."""

from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
from importlib import util
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import pytest

from mindroom.config.agent import AgentConfig
from mindroom.config.main import Config
from mindroom.constants import RuntimePaths
from mindroom.hooks.decorators import get_hook_metadata
from mindroom.matrix.identity import managed_account_key
from mindroom.matrix.state import MatrixState

if TYPE_CHECKING:
    from types import ModuleType

PACKAGE_NAME = (
    f"mindroom_plugin_{Path(__file__).resolve().parents[1].name.replace('-', '_')}"
)
_TEST_PASSWORD = "mock_test_password"  # noqa: S105


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


def _shared_runtime(
    tmp_path: Path,
    agent_names: tuple[str, ...] = ("code", "research"),
    *,
    persisted_agent_names: tuple[str, ...] | None = None,
) -> tuple[Config, RuntimePaths]:
    """Build a shared-agent config with authoritative persisted Matrix identities."""
    config = Config(
        agents={
            agent_name: AgentConfig(display_name=agent_name.title())
            for agent_name in agent_names
        },
    )
    runtime_paths = RuntimePaths(
        config_path=tmp_path / "config.yaml",
        config_dir=tmp_path,
        env_path=tmp_path / ".env",
        storage_root=tmp_path,
    )
    state = MatrixState()
    persisted_names = (
        agent_names if persisted_agent_names is None else persisted_agent_names
    )
    for agent_name in persisted_names:
        state.add_account(
            managed_account_key(agent_name),
            f"mindroom_{agent_name}",
            _TEST_PASSWORD,
            domain="localhost",
        )
    state.save(runtime_paths)
    return config, runtime_paths


def _base_ctx(tmp_path: Path, settings: dict[str, object]) -> dict[str, object]:
    config, runtime_paths = _shared_runtime(tmp_path)
    return {
        "settings": settings,
        "config": config,
        "runtime_paths": runtime_paths,
        "logger": Mock(),
    }


def _message_ctx(
    tmp_path: Path, room_id: str, settings: dict[str, object]
) -> SimpleNamespace:
    return SimpleNamespace(
        envelope=SimpleNamespace(room_id=room_id), **_base_ctx(tmp_path, settings)
    )


def _after_response_ctx(
    tmp_path: Path, room_id: str, settings: dict[str, object]
) -> SimpleNamespace:
    return SimpleNamespace(
        result=SimpleNamespace(envelope=SimpleNamespace(room_id=room_id)),
        **_base_ctx(tmp_path, settings),
    )


def _lifecycle_ctx(tmp_path: Path, settings: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(**_base_ctx(tmp_path, settings))


async def _drain(module: ModuleType, cycles: int = 400) -> None:
    """Wait until the runner and its worker thread finished all pending passes.

    Passes run on a worker thread, so this polls real time and requires the idle
    condition to hold across consecutive polls to bridge the dispatch gap between
    draining the pending set and the thread acquiring the pass lock.
    """
    idle_streak = 0
    for _ in range(cycles):
        await asyncio.sleep(0.005)
        wakeup = module._wakeup
        idle = (
            not module._full_pass_pending
            and not module._pending_room_ids
            and not module._EXPORT_PASS_LOCK.locked()
            and (wakeup is None or not wakeup.is_set())
        )
        idle_streak = idle_streak + 1 if idle else 0
        if idle_streak >= 3:
            return


async def _shutdown_runner(module: ModuleType) -> None:
    """Cancel the module's runner task so tests exit cleanly."""
    task = module._runner_tasks.get("runner")
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _target_stats(
    *, targets: tuple[object, ...], **_kwargs: object
) -> tuple[Mock, ...]:
    """Return one successful export statistic object per requested target."""
    return tuple(
        Mock(rooms_exported=1, threads_exported=1, threads_unchanged=0, failures=0)
        for _target in targets
    )


def test_hook_metadata_matches_spec() -> None:
    """The hooks should target the expected lifecycle and message events."""
    module = _load_hooks_module()

    startup = get_hook_metadata(module.queue_initial_full_pass)
    assert startup is not None
    assert startup.event_name == "bot:ready"
    assert startup.agents == ("router",)

    config_reloaded = get_hook_metadata(module.queue_full_pass_after_config_reload)
    assert config_reloaded is not None
    assert config_reloaded.event_name == "config:reloaded"
    assert config_reloaded.agents is None

    on_message = get_hook_metadata(module.queue_room_on_message)
    assert on_message is not None
    assert on_message.event_name == "message:received"
    assert on_message.agents is None

    after_response = get_hook_metadata(module.queue_room_after_response)
    assert after_response is not None
    assert after_response.event_name == "message:after_response"
    assert after_response.agents is None


@pytest.mark.asyncio
async def test_message_hooks_inactive_without_agents_setting(tmp_path: Path) -> None:
    """Message hooks should do nothing when the settings list no agents."""
    module = _load_hooks_module()
    empty_settings: dict[str, object] = {}

    await module.queue_room_on_message(_message_ctx(tmp_path, "!a:hs", empty_settings))
    await module.queue_room_after_response(
        _after_response_ctx(tmp_path, "!b:hs", empty_settings)
    )
    assert module._runner_tasks == {}
    assert module._pending_room_ids == set()
    assert module._full_pass_pending is False


@pytest.mark.asyncio
async def test_config_reload_queues_full_pass(tmp_path: Path) -> None:
    """Hot reload should backfill all rooms through one full export pass."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    settings = _settings()

    await module.queue_full_pass_after_config_reload(_lifecycle_ctx(tmp_path, settings))
    await _drain(module)
    await _shutdown_runner(module)

    module.export_threads_to_targets_once.assert_awaited_once()
    assert (
        module.export_threads_to_targets_once.await_args.kwargs["room_filter"] is None
    )


@pytest.mark.asyncio
async def test_message_triggers_coalesce_into_one_pass(tmp_path: Path) -> None:
    """Repeated triggers should coalesce into one shared export per dirty room."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    settings = _settings()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await module.queue_room_after_response(
        _after_response_ctx(tmp_path, "!beta:hs", settings)
    )
    await _drain(module)
    await _shutdown_runner(module)

    assert module.export_threads_to_targets_once.await_count == 2
    room_filters = {
        call.kwargs["room_filter"]
        for call in module.export_threads_to_targets_once.await_args_list
    }
    assert room_filters == {"!alpha:hs", "!beta:hs"}
    expected_output_dir = tmp_path / "agents" / "code" / "workspace" / "thread_exports"
    for call in module.export_threads_to_targets_once.await_args_list:
        assert call.kwargs["prefer_cache"] is True
        assert len(call.kwargs["targets"]) == 1
        target = call.kwargs["targets"][0]
        assert target.output_dir == expected_output_dir
        assert target.required_member_user_id == "@mindroom_code:localhost"
        assert target.include_invited_rooms is True


@pytest.mark.asyncio
async def test_agent_mapping_settings_control_invited_rooms(tmp_path: Path) -> None:
    """The mapping form of the agents setting should control invited-room export per agent."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    settings: dict[str, object] = {
        "agents": {"code": {"invited_rooms": False}, "research": None},
        "debounce_seconds": 0,
    }

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await _drain(module)
    await _shutdown_runner(module)

    module.export_threads_to_targets_once.assert_awaited_once()
    invited_by_agent = {
        target.output_dir.parts[-3]: (
            target.required_member_user_id,
            target.include_invited_rooms,
        )
        for target in module.export_threads_to_targets_once.await_args.kwargs["targets"]
    }
    assert invited_by_agent == {
        "code": ("@mindroom_code:localhost", False),
        "research": ("@mindroom_research:localhost", True),
    }


@pytest.mark.asyncio
async def test_bot_ready_runs_full_pass(tmp_path: Path) -> None:
    """bot:ready should queue one full pass shared by all enabled agents."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    settings = _settings(agents=["code", "research"])

    await module.queue_initial_full_pass(_lifecycle_ctx(tmp_path, settings))
    await _drain(module)
    await _shutdown_runner(module)

    module.export_threads_to_targets_once.assert_awaited_once()
    assert (
        module.export_threads_to_targets_once.await_args.kwargs["room_filter"] is None
    )
    output_dirs = {
        target.output_dir
        for target in module.export_threads_to_targets_once.await_args.kwargs["targets"]
    }
    assert output_dirs == {
        tmp_path / "agents" / "code" / "workspace" / "thread_exports",
        tmp_path / "agents" / "research" / "workspace" / "thread_exports",
    }


@pytest.mark.asyncio
async def test_full_pass_subsumes_pending_rooms(tmp_path: Path) -> None:
    """A pending full pass should replace per-room exports in the same drain."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    settings = _settings()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await module.queue_initial_full_pass(_lifecycle_ctx(tmp_path, settings))
    await _drain(module)
    await _shutdown_runner(module)

    module.export_threads_to_targets_once.assert_awaited_once()
    assert (
        module.export_threads_to_targets_once.await_args.kwargs["room_filter"] is None
    )


@pytest.mark.asyncio
async def test_mid_pass_triggers_drain_in_one_followup(tmp_path: Path) -> None:
    """Triggers arriving during a pass should coalesce into exactly one follow-up pass."""
    module = _load_hooks_module()
    release = threading.Event()
    started = threading.Event()

    async def _blocking_export(
        *, targets: tuple[object, ...], **_kwargs: object
    ) -> tuple[Mock, ...]:
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.005)
        return _target_stats(targets=targets)

    module.export_threads_to_targets_once = AsyncMock(side_effect=_blocking_export)
    settings = _settings()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    for _ in range(200):
        if started.is_set():
            break
        await asyncio.sleep(0.005)
    assert started.is_set()

    await module.queue_room_on_message(_message_ctx(tmp_path, "!beta:hs", settings))
    await module.queue_room_on_message(_message_ctx(tmp_path, "!gamma:hs", settings))
    release.set()
    await _drain(module)
    await _shutdown_runner(module)

    assert module.export_threads_to_targets_once.await_count == 3
    room_filters = [
        call.kwargs["room_filter"]
        for call in module.export_threads_to_targets_once.await_args_list
    ]
    assert room_filters[0] == "!alpha:hs"
    assert set(room_filters[1:]) == {"!beta:hs", "!gamma:hs"}


@pytest.mark.asyncio
async def test_export_failure_does_not_kill_runner_or_later_passes(
    tmp_path: Path,
) -> None:
    """One failed shared pass should not block a later dirty-room pass."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(
        side_effect=[
            RuntimeError("export failed"),
            (
                Mock(
                    rooms_exported=1,
                    threads_exported=1,
                    threads_unchanged=0,
                    failures=0,
                ),
                Mock(
                    rooms_exported=1,
                    threads_exported=1,
                    threads_unchanged=0,
                    failures=0,
                ),
            ),
        ],
    )
    settings = _settings(agents=["code", "research"])

    await module.queue_room_on_message(_message_ctx(tmp_path, "!alpha:hs", settings))
    await _drain(module)
    await module.queue_room_on_message(_message_ctx(tmp_path, "!beta:hs", settings))
    await _drain(module)

    assert module.export_threads_to_targets_once.await_count == 2
    runner = module._runner_tasks["runner"]
    assert not runner.done()
    await _shutdown_runner(module)


@pytest.mark.asyncio
async def test_unknown_agents_are_warned_and_skipped(tmp_path: Path) -> None:
    """Settings naming unknown agents should warn and export only for known ones."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    logger = Mock()
    config, runtime_paths = _shared_runtime(tmp_path, ("code",))
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["ghost", "code"]},
        logger=logger,
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    module.export_threads_to_targets_once.assert_awaited_once()
    target = module.export_threads_to_targets_once.await_args.kwargs["targets"][0]
    assert target.output_dir == (
        tmp_path / "agents" / "code" / "workspace" / "thread_exports"
    )
    logger.warning.assert_called_once()
    assert logger.warning.call_args.kwargs["unknown_agents"] == ["ghost"]


@pytest.mark.asyncio
async def test_full_pass_removes_exports_for_disabled_agents(tmp_path: Path) -> None:
    """Removing an agent from plugin settings should delete its plugin-owned exports."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    research_export_dir = (
        tmp_path / "agents" / "research" / "workspace" / "thread_exports"
    )
    research_export_dir.mkdir(parents=True)
    (research_export_dir / "old.yaml").write_text("secret", encoding="utf-8")
    config, runtime_paths = _shared_runtime(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert not research_export_dir.exists()
    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert [target.output_dir.parts[-3] for target in targets] == ["code"]


@pytest.mark.asyncio
async def test_shared_agent_without_persisted_identity_fails_closed(
    tmp_path: Path,
) -> None:
    """A missing shared-agent account should remove prior exports instead of widening access."""
    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    export_dir = tmp_path / "agents" / "code" / "workspace" / "thread_exports"
    export_dir.mkdir(parents=True)
    (export_dir / "old.yaml").write_text("secret", encoding="utf-8")
    config, runtime_paths = _shared_runtime(
        tmp_path,
        ("code",),
        persisted_agent_names=(),
    )
    logger = Mock()
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code"]},
        logger=logger,
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert not export_dir.exists()
    assert module.export_threads_to_targets_once.await_args.kwargs["targets"] == ()
    logger.warning.assert_called_once_with(
        "Skipping shared agent without persisted Matrix account",
        agent_name="code",
    )


@pytest.mark.asyncio
async def test_private_agent_exports_scoped_to_resolved_owners(tmp_path: Path) -> None:
    """Private instances should export only for resolvable owners, scoped to the owner's rooms."""
    from mindroom.config.agent import AgentConfig, AgentPrivateConfig
    from mindroom.config.auth import AuthorizationConfig
    from mindroom.config.main import Config
    from mindroom.tool_system.worker_routing import (
        private_instance_state_root_for_requester,
    )

    module = _load_hooks_module()
    module.export_threads_to_targets_once = AsyncMock(side_effect=_target_stats)
    config = Config(
        agents={
            "secret": AgentConfig(
                display_name="Secret", private=AgentPrivateConfig(per="user")
            )
        },
        authorization=AuthorizationConfig(global_users=["@alice:hs", "@bob:hs"]),
    )
    runtime_paths = SimpleNamespace(
        storage_root=tmp_path, env_value=lambda _name, default=None: default
    )
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
    ghost_root = tmp_path / "private_instances" / "ghost-0000000000000000" / "secret"
    ghost_export_dir = ghost_root / "secret_data" / "thread_exports"
    ghost_export_dir.mkdir(parents=True)
    (ghost_export_dir / "old.yaml").write_text("secret", encoding="utf-8")
    logger = Mock()
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=logger,
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    module.export_threads_to_targets_once.assert_awaited_once()
    exported = {
        target.required_member_user_id: target.output_dir
        for target in module.export_threads_to_targets_once.await_args.kwargs["targets"]
    }
    assert exported == expected_dirs
    assert not ghost_export_dir.exists()
    orphan_warnings = [
        call
        for call in logger.warning.call_args_list
        if "without resolvable owner" in call.args[0]
    ]
    assert len(orphan_warnings) == 1
    assert "ghost-0000000000000000" in orphan_warnings[0].kwargs["instance_root"]
