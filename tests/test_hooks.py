# ruff: noqa: INP001
"""Tests for the thread-export trigger hooks and single-flight runner."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import sys
import threading
from importlib import util
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock, create_autospec

import pytest

from mindroom.config.access import ResponderAccessConfig
from mindroom.config.agent import AgentConfig, AgentPrivateConfig
from mindroom.config.main import Config
from mindroom.constants import RuntimePaths
from mindroom.hooks.decorators import get_hook_metadata
from mindroom.matrix.identity import managed_account_key
from mindroom.matrix.state import MatrixState
from mindroom.runtime_resolution import resolve_agent_runtime
from mindroom.thread_export.models import ThreadExportRoom
from mindroom.thread_export.storage import (
    _ROOT_MARKER_FILENAME,
    _ROOT_MARKER_TEXT,
    write_room_index,
    write_thread_payload,
)
from mindroom.tool_system.worker_routing import (
    ToolExecutionIdentity,
)

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


def _private_runtime(
    tmp_path: Path,
    requester_ids: tuple[str, ...],
    *,
    persist_agent_identity: bool,
    authorize_requesters: bool = True,
) -> tuple[Config, RuntimePaths, dict[str, Path]]:
    """Build one private-agent runtime and its requester-scoped instance roots."""
    config = Config(
        agents={
            "secret": AgentConfig(
                display_name="Secret",
                private=AgentPrivateConfig(per="user"),
            ),
        },
        administrators=list(requester_ids) if authorize_requesters else [],
    )
    runtime_paths = RuntimePaths(
        config_path=tmp_path / "config.yaml",
        config_dir=tmp_path,
        env_path=tmp_path / ".env",
        storage_root=tmp_path,
    )
    if persist_agent_identity:
        state = MatrixState()
        state.add_account(
            managed_account_key("secret"),
            "mindroom_secret",
            _TEST_PASSWORD,
            domain="localhost",
        )
        state.save(runtime_paths)
    instance_roots: dict[str, Path] = {}
    for requester_id in requester_ids:
        instance_roots[requester_id] = _materialize_private_instance(
            config, runtime_paths, requester_id
        )
    return config, runtime_paths, instance_roots


def _materialize_private_instance(
    config: Config,
    runtime_paths: RuntimePaths,
    requester_id: str,
    *,
    agent_name: str = "secret",
) -> Path:
    """Create a private runtime through the core materialization boundary."""
    return resolve_agent_runtime(
        agent_name,
        config,
        runtime_paths,
        ToolExecutionIdentity(
            channel="matrix",
            agent_name=agent_name,
            requester_id=requester_id,
            room_id="!private:hs",
            thread_id="thread",
            resolved_thread_id="thread",
            session_id="session",
        ),
        create=True,
    ).state_root


def _symlinked_private_state_root(tmp_path: Path) -> Path:
    """Create an untrusted private-root symlink and an external export sentinel."""
    external_state_root = tmp_path / "external" / "secret"
    external_export_dir = external_state_root / "secret_data" / "thread_exports"
    external_export_dir.mkdir(parents=True)
    (external_export_dir / "sentinel.yaml").write_text("keep", encoding="utf-8")
    symlink_root = tmp_path / "private_instances" / "untrusted" / "secret"
    symlink_root.parent.mkdir(parents=True)
    symlink_root.symlink_to(external_state_root, target_is_directory=True)
    return external_export_dir


def _write_owned_export(output_dir: Path) -> None:
    """Create one marker-backed export tree owned by the core exporter."""
    room = ThreadExportRoom(
        key="lobby",
        room_id="!lobby:hs",
        alias="#lobby:hs",
        name="Lobby",
    )
    write_thread_payload(
        output_dir,
        room,
        "$thread:hs",
        {
            "version": 1,
            "room": {
                "key": room.key,
                "id": room.room_id,
                "alias": room.alias,
                "name": room.name,
            },
            "thread": {"id": "$thread:hs", "source": "matrix"},
            "messages": [],
        },
    )
    write_room_index(output_dir, room)


def _assert_export_retracted(output_dir: Path) -> None:
    """Assert that only the core ownership marker remains after retraction."""
    assert tuple(path.name for path in output_dir.iterdir()) == (_ROOT_MARKER_FILENAME,)
    assert (output_dir / _ROOT_MARKER_FILENAME).read_text(
        encoding="utf-8"
    ) == _ROOT_MARKER_TEXT


def _symlinked_private_instances_root(tmp_path: Path) -> Path:
    """Create an untrusted private-instances symlink and external export sentinel."""
    external_instances_root = tmp_path / "external-instances"
    external_export_dir = (
        external_instances_root
        / "untrusted"
        / "secret"
        / "secret_data"
        / "thread_exports"
    )
    external_export_dir.mkdir(parents=True)
    (external_export_dir / "sentinel.yaml").write_text("keep", encoding="utf-8")
    (tmp_path / "private_instances").symlink_to(
        external_instances_root, target_is_directory=True
    )
    return external_export_dir


def _base_ctx(tmp_path: Path, settings: dict[str, object]) -> dict[str, object]:
    config, runtime_paths = _shared_runtime(tmp_path)
    return {
        "settings": settings,
        "config": config,
        "runtime_paths": runtime_paths,
        "logger": Mock(),
        "is_active": lambda: True,
    }


def _message_ctx(
    tmp_path: Path, room_id: str, settings: dict[str, object]
) -> SimpleNamespace:
    return SimpleNamespace(
        envelope=SimpleNamespace(room_id=room_id, requester_id="@requester:hs"),
        **_base_ctx(tmp_path, settings),
    )


def _after_response_ctx(
    tmp_path: Path, room_id: str, settings: dict[str, object]
) -> SimpleNamespace:
    return SimpleNamespace(
        result=SimpleNamespace(
            envelope=SimpleNamespace(room_id=room_id, requester_id="@requester:hs")
        ),
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
            and not module._pending_private_requester_ids
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


def _autospec_export(module: ModuleType, *, side_effect: object) -> None:
    """Replace the MindRoom export API with a signature-enforcing async mock."""
    module.export_threads_to_targets_once = create_autospec(
        module.export_threads_to_targets_once,
        spec_set=True,
        side_effect=side_effect,
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
    _autospec_export(module, side_effect=_target_stats)
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
    module._live_hook_seen = True
    _autospec_export(module, side_effect=_target_stats)
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
        assert len(call.kwargs["targets"]) == 1
        target = call.kwargs["targets"][0]
        assert target.output_dir == expected_output_dir
        assert target.required_member_user_ids == ("@mindroom_code:localhost",)
        assert target.include_invited_rooms is True


@pytest.mark.asyncio
async def test_agent_mapping_settings_control_invited_rooms(tmp_path: Path) -> None:
    """The mapping form of the agents setting should control invited-room export per agent."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
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
            target.required_member_user_ids,
            target.include_invited_rooms,
        )
        for target in module.export_threads_to_targets_once.await_args.kwargs["targets"]
    }
    assert invited_by_agent == {
        "code": (("@mindroom_code:localhost",), False),
        "research": (("@mindroom_research:localhost",), True),
    }


@pytest.mark.asyncio
async def test_incremental_pass_logs_only_targets_with_activity(tmp_path: Path) -> None:
    """Dirty-room passes should not emit one no-op log per unrelated target."""
    module = _load_hooks_module()

    async def _activity_stats(**_kwargs: object) -> tuple[Mock, Mock]:
        return (
            Mock(
                rooms_exported=0,
                threads_exported=0,
                threads_unchanged=0,
                failures=0,
            ),
            Mock(
                rooms_exported=1,
                threads_exported=3,
                threads_unchanged=3,
                failures=0,
            ),
        )

    _autospec_export(module, side_effect=_activity_stats)
    logger = Mock()
    config, runtime_paths = _shared_runtime(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code", "research"]},
        logger=logger,
    )

    await module._run_export_pass(
        env,
        full_pass=False,
        room_ids=frozenset({"!changed:hs"}),
    )

    logger.info.assert_called_once()
    assert logger.info.call_args.kwargs["agent_name"] == "research"


@pytest.mark.asyncio
async def test_full_pass_logs_targets_without_activity(tmp_path: Path) -> None:
    """Full reconciliation should retain a completion log for every target."""
    module = _load_hooks_module()

    async def _no_activity_stats(**_kwargs: object) -> tuple[Mock]:
        return (
            Mock(
                rooms_exported=0,
                threads_exported=0,
                threads_unchanged=0,
                failures=0,
            ),
        )

    _autospec_export(module, side_effect=_no_activity_stats)
    logger = Mock()
    config, runtime_paths = _shared_runtime(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code"]},
        logger=logger,
    )

    await module._run_export_pass(
        env,
        full_pass=True,
        room_ids=frozenset(),
    )

    logger.info.assert_called_once()
    assert logger.info.call_args.kwargs["agent_name"] == "code"


def test_unknown_private_room_scope_defaults_to_intersection() -> None:
    """A misspelled private scope must not silently widen exported room access."""
    module = _load_hooks_module()

    options = module._agent_options({"private_room_scope": "unknown"})

    assert options.private_room_scope == "owner_and_agent"


@pytest.mark.asyncio
async def test_bot_ready_runs_full_pass(tmp_path: Path) -> None:
    """bot:ready should queue one full pass shared by all enabled agents."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
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
    module._live_hook_seen = True
    _autospec_export(module, side_effect=_target_stats)
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
    module._live_hook_seen = True
    release = threading.Event()
    started = threading.Event()

    async def _blocking_export(
        *, targets: tuple[object, ...], **_kwargs: object
    ) -> tuple[Mock, ...]:
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.005)
        return _target_stats(targets=targets)

    _autospec_export(module, side_effect=_blocking_export)
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
    _autospec_export(
        module,
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
    _autospec_export(module, side_effect=_target_stats)
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
    _autospec_export(module, side_effect=_target_stats)
    research_export_dir = (
        tmp_path / "agents" / "research" / "workspace" / "thread_exports"
    )
    _write_owned_export(research_export_dir)
    config, runtime_paths = _shared_runtime(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    _assert_export_retracted(research_export_dir)
    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert [target.output_dir.parts[-3] for target in targets] == ["code"]


@pytest.mark.asyncio
async def test_shared_agent_without_persisted_identity_fails_closed(
    tmp_path: Path,
) -> None:
    """A missing shared-agent account should remove prior exports instead of widening access."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    export_dir = tmp_path / "agents" / "code" / "workspace" / "thread_exports"
    _write_owned_export(export_dir)
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

    _assert_export_retracted(export_dir)
    assert module.export_threads_to_targets_once.await_args.kwargs["targets"] == ()
    logger.warning.assert_called_once_with(
        "Skipping shared agent without persisted Matrix account",
        agent_name="code",
    )


@pytest.mark.asyncio
async def test_private_agent_without_persisted_identity_fails_closed(
    tmp_path: Path,
) -> None:
    """Intersection scope must remove private exports when the agent identity is unknown."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        ("@alice:hs",),
        persist_agent_identity=False,
    )
    export_dir = instance_roots["@alice:hs"] / "secret_data" / "thread_exports"
    _write_owned_export(export_dir)
    logger = Mock()
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=logger,
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    _assert_export_retracted(export_dir)
    assert module.export_threads_to_targets_once.await_args.kwargs["targets"] == ()
    logger.warning.assert_called_once_with(
        "Skipping private agent without persisted Matrix account",
        agent_name="secret",
    )


@pytest.mark.asyncio
async def test_private_agent_with_mismatched_forwarded_root_fails_closed(
    tmp_path: Path,
) -> None:
    """A root that cannot be forward-resolved must not receive exports."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    agent_name = "___"
    requester_id = "@alice:hs"
    config = Config(
        agents={
            agent_name: AgentConfig(
                display_name="Underscore Agent",
                private=AgentPrivateConfig(per="user"),
            ),
        },
        administrators=[requester_id],
    )
    runtime_paths = RuntimePaths(
        config_path=tmp_path / "config.yaml",
        config_dir=tmp_path,
        env_path=tmp_path / ".env",
        storage_root=tmp_path,
    )
    state = MatrixState()
    state.add_account(
        managed_account_key(agent_name),
        "mindroom_worker",
        _TEST_PASSWORD,
        domain="localhost",
    )
    state.save(runtime_paths)
    _materialize_private_instance(
        config, runtime_paths, requester_id, agent_name=agent_name
    )
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": [agent_name]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert module.export_threads_to_targets_once.await_args.kwargs["targets"] == ()


@pytest.mark.asyncio
async def test_private_agent_exports_require_owner_and_agent_membership_by_default(
    tmp_path: Path,
) -> None:
    """Private exports should default to rooms where both owner and agent are members."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        ("@alice:hs", "@bob:hs"),
        persist_agent_identity=True,
    )
    expected_dirs = {
        owner: instance_root / "secret_data" / "thread_exports"
        for owner, instance_root in instance_roots.items()
    }
    ghost_root = tmp_path / "private_instances" / "ghost-0000000000000000" / "secret"
    ghost_export_dir = ghost_root / "secret_data" / "thread_exports"
    _write_owned_export(ghost_export_dir)
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
        target.required_member_user_ids: target.output_dir
        for target in module.export_threads_to_targets_once.await_args.kwargs["targets"]
    }
    assert exported == {
        (owner, "@mindroom_secret:localhost"): output_dir
        for owner, output_dir in expected_dirs.items()
    }
    _assert_export_retracted(ghost_export_dir)
    orphan_warnings = [
        call
        for call in logger.warning.call_args_list
        if "without valid core identity" in call.args[0]
    ]
    assert len(orphan_warnings) == 1
    assert "ghost-0000000000000000" in orphan_warnings[0].kwargs["instance_root"]


def test_private_workspace_swap_keeps_lexical_export_target(tmp_path: Path) -> None:
    """A post-validation symlink swap cannot redirect an in-root export target."""
    module = _load_hooks_module()
    requester_id = "@alice:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
    )
    state_root = instance_roots[requester_id]
    saved_state_root = state_root.with_name("secret-saved")
    victim_root = tmp_path / "victim"
    victim_root.mkdir()
    original_resolver = module.resolve_agent_workspace_from_state_path

    def swap_then_resolve(*args: object, **kwargs: object) -> object:
        state_root.rename(saved_state_root)
        state_root.symlink_to(victim_root, target_is_directory=True)
        return original_resolver(*args, **kwargs)

    module.resolve_agent_workspace_from_state_path = swap_then_resolve
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    targets = module._private_agent_export_targets(
        env,
        "secret",
        agent_user_id="@mindroom_secret:localhost",
        options=module._AgentExportSettings(
            invited_rooms=True,
            private_room_scope="owner_and_agent",
        ),
        worker_scope="user",
        reconcile_instances=True,
        private_instance_requesters={},
    )

    assert targets[0].output_dir == state_root / "secret_data" / "thread_exports"
    assert targets[0].output_dir != victim_root / "secret_data" / "thread_exports"


@pytest.mark.asyncio
async def test_full_pass_recovers_unconfigured_private_requester_from_core_identity(
    tmp_path: Path,
) -> None:
    """A reload must discover a materialized private instance without config hints."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@unconfigured:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
        authorize_requesters=False,
    )
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert tuple(target.required_member_user_ids for target in targets) == (
        (requester_id, "@mindroom_secret:localhost"),
    )
    assert targets[0].output_dir == (
        instance_roots[requester_id] / "secret_data" / "thread_exports"
    )


@pytest.mark.asyncio
async def test_private_exports_do_not_create_private_owner_metadata(
    tmp_path: Path,
) -> None:
    """Exporting a materialized private root must leave its core scope unchanged."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@alice:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path, (requester_id,), persist_agent_identity=True
    )
    instance_root = instance_roots[requester_id]
    scope_entries = {entry.name for entry in instance_root.parent.iterdir()}
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert {entry.name for entry in instance_root.parent.iterdir()} == scope_entries


@pytest.mark.asyncio
async def test_invalid_symlinked_private_root_does_not_remove_external_exports(
    tmp_path: Path,
) -> None:
    """An invalid private-root symlink must not delete exports outside its scope."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path, (), persist_agent_identity=True
    )
    external_export_dir = _symlinked_private_state_root(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert (external_export_dir / "sentinel.yaml").exists()


@pytest.mark.asyncio
async def test_disabled_private_cleanup_ignores_symlinked_state_roots(
    tmp_path: Path,
) -> None:
    """Disabled-agent cleanup must not follow untrusted private-root symlinks."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path, (), persist_agent_identity=True
    )
    external_export_dir = _symlinked_private_state_root(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": []},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert (external_export_dir / "sentinel.yaml").exists()


@pytest.mark.asyncio
async def test_disabled_private_cleanup_removes_recordless_legacy_exports(
    tmp_path: Path,
) -> None:
    """Disabled cleanup removes plugin exports from regular legacy state roots."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path, (), persist_agent_identity=True
    )
    export_dir = (
        tmp_path
        / "private_instances"
        / "legacy"
        / "secret"
        / "secret_data"
        / "thread_exports"
    )
    _write_owned_export(export_dir)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": []},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    _assert_export_retracted(export_dir)


@pytest.mark.asyncio
async def test_disabled_private_cleanup_ignores_workspace_validation_errors(
    tmp_path: Path,
) -> None:
    """Disabled cleanup leaves a target alone when its workspace path is invalid."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@alice:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path, (requester_id,), persist_agent_identity=True
    )
    export_dir = instance_roots[requester_id] / "secret_data" / "thread_exports"
    _write_owned_export(export_dir)
    module.resolve_agent_workspace_from_state_path = Mock(
        side_effect=ValueError("invalid workspace path")
    )
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": []},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert export_dir.exists()


@pytest.mark.asyncio
async def test_full_pass_ignores_symlinked_private_instances_root(
    tmp_path: Path,
) -> None:
    """Full reconciliation must not traverse a symlinked private-instances root."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path, (), persist_agent_identity=True
    )
    external_export_dir = _symlinked_private_instances_root(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert (external_export_dir / "sentinel.yaml").exists()


@pytest.mark.asyncio
async def test_disabled_cleanup_ignores_symlinked_private_instances_root(
    tmp_path: Path,
) -> None:
    """Disabled cleanup must not traverse a symlinked private-instances root."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path, (), persist_agent_identity=True
    )
    external_export_dir = _symlinked_private_instances_root(tmp_path)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": []},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert (external_export_dir / "sentinel.yaml").exists()


@pytest.mark.asyncio
async def test_missing_core_identity_removes_stale_private_exports(
    tmp_path: Path,
) -> None:
    """A private root without core identity must not retain prior export files."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@alice:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path, (requester_id,), persist_agent_identity=True
    )
    instance_root = instance_roots[requester_id]
    export_dir = instance_root / "secret_data" / "thread_exports"
    _write_owned_export(export_dir)
    (instance_root.parent / ".mindroom-private-instance.json").unlink()
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    _assert_export_retracted(export_dir)
    assert module.export_threads_to_targets_once.await_args.kwargs["targets"] == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_record", ["{not-json", "mismatched"])
async def test_invalid_core_identity_removes_stale_private_exports(
    tmp_path: Path, invalid_record: str
) -> None:
    """Malformed and mismatched core identities must both fail closed."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@alice:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path, (requester_id, "@bob:hs"), persist_agent_identity=True
    )
    instance_root = instance_roots[requester_id]
    export_dir = instance_root / "secret_data" / "thread_exports"
    _write_owned_export(export_dir)
    record_path = instance_root.parent / ".mindroom-private-instance.json"
    if invalid_record == "mismatched":
        record_path.write_text(
            (
                instance_roots["@bob:hs"].parent / ".mindroom-private-instance.json"
            ).read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    else:
        record_path.write_text(invalid_record, encoding="utf-8")
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    _assert_export_retracted(export_dir)
    assert tuple(
        target.required_member_user_ids
        for target in module.export_threads_to_targets_once.await_args.kwargs["targets"]
    ) == (("@bob:hs", "@mindroom_secret:localhost"),)


@pytest.mark.asyncio
async def test_private_requester_observed_during_full_pass_survives_incremental_export(
    tmp_path: Path,
) -> None:
    """A full-pass snapshot must not discard a requester learned by a hook mid-pass."""
    module = _load_hooks_module()
    requester_id = "@first:hs"
    later_requester_id = "@later:hs"
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
        authorize_requesters=False,
    )
    later_root = _materialize_private_instance(
        config, runtime_paths, later_requester_id
    )
    export_once = module.export_threads_to_targets_once
    _autospec_export(module, side_effect=_target_stats)
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"], "debounce_seconds": 0},
        logger=Mock(),
    )
    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())
    started = threading.Event()
    release = threading.Event()

    async def pause_full_pass(
        *, targets: tuple[object, ...], **_kwargs: object
    ) -> tuple[Mock, ...]:
        started.set()
        await asyncio.to_thread(release.wait)
        return _target_stats(targets=targets)

    module.export_threads_to_targets_once = create_autospec(
        export_once, spec_set=True, side_effect=pause_full_pass
    )
    pass_task = asyncio.create_task(
        asyncio.to_thread(
            module._run_export_pass_blocking,
            env,
            full_pass=True,
            room_ids=frozenset(),
        )
    )
    await asyncio.to_thread(started.wait)
    module._remember_private_instance_requester(
        SimpleNamespace(
            config=config,
            runtime_paths=runtime_paths,
            settings=env.settings,
            logger=env.logger,
            is_active=lambda: True,
        ),
        later_requester_id,
    )
    release.set()
    await pass_task
    module.export_threads_to_targets_once = create_autospec(
        export_once, spec_set=True, side_effect=_target_stats
    )

    await module._run_export_pass(
        env, full_pass=False, room_ids=frozenset({"!changed:hs"})
    )

    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert later_root / "secret_data" / "thread_exports" in {
        target.output_dir for target in targets
    }


@pytest.mark.asyncio
async def test_discarded_staged_module_does_not_disable_active_worker_pass(
    tmp_path: Path,
) -> None:
    """A staged import must not invalidate a module that remains in the live registry."""
    active_module = _load_hooks_module()
    active_export = active_module.export_threads_to_targets_once
    _autospec_export(active_module, side_effect=_target_stats)
    active_settings = {"agents": ["missing"], "debounce_seconds": 0}

    await active_module.queue_full_pass_after_config_reload(
        _lifecycle_ctx(tmp_path, active_settings)
    )
    await _drain(active_module)
    await _shutdown_runner(active_module)

    config, runtime_paths = _shared_runtime(tmp_path, ("code",))
    export_dir = tmp_path / "agents" / "code" / "workspace" / "thread_exports"

    async def write_active_export(
        *, targets: tuple[object, ...], **_kwargs: object
    ) -> tuple[Mock, ...]:
        for target in targets:
            target.output_dir.mkdir(parents=True, exist_ok=True)
            (target.output_dir / "active.yaml").write_text("active", encoding="utf-8")
        return _target_stats(targets=targets)

    active_module.export_threads_to_targets_once = create_autospec(
        active_export, spec_set=True, side_effect=write_active_export
    )
    _staged_module = _load_hooks_module()
    active_env = active_module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code"]},
        logger=Mock(),
    )

    await asyncio.to_thread(
        active_module._run_export_pass_blocking,
        active_env,
        full_pass=True,
        room_ids=frozenset(),
    )

    assert (export_dir / "active.yaml").exists()


@pytest.mark.asyncio
async def test_reloaded_module_discards_cancelled_old_worker_pass(
    tmp_path: Path,
) -> None:
    """An old worker queued behind a reload must not recreate retracted exports."""
    old_module = _load_hooks_module()
    config, runtime_paths = _shared_runtime(tmp_path, ("code",))
    old_active = True
    old_env = old_module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["code"]},
        logger=Mock(),
        is_active=lambda: old_active,
    )
    old_export = old_module.export_threads_to_targets_once

    async def recreate_exports(
        *, targets: tuple[object, ...], **_kwargs: object
    ) -> tuple[Mock, ...]:
        for target in targets:
            target.output_dir.mkdir(parents=True, exist_ok=True)
            (target.output_dir / "stale.yaml").write_text("old", encoding="utf-8")
        return _target_stats(targets=targets)

    old_module.export_threads_to_targets_once = create_autospec(
        old_export, spec_set=True, side_effect=recreate_exports
    )
    lock = old_module._EXPORT_PASS_LOCK
    worker_started = threading.Event()
    worker_completed = threading.Event()

    def run_old_pass() -> None:
        worker_started.set()
        try:
            old_module._run_export_pass_blocking(
                old_env, full_pass=True, room_ids=frozenset()
            )
        finally:
            worker_completed.set()

    lock.acquire()
    old_pass = asyncio.create_task(asyncio.to_thread(run_old_pass))
    try:
        await asyncio.to_thread(worker_started.wait)
        old_pass.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await old_pass

        new_module = _load_hooks_module()
        old_active = False
        _autospec_export(new_module, side_effect=_target_stats)
        export_dir = tmp_path / "agents" / "code" / "workspace" / "thread_exports"
        _write_owned_export(export_dir)
        new_env = new_module._TriggerEnv(
            config=config,
            runtime_paths=runtime_paths,
            settings={"agents": []},
            logger=Mock(),
        )

        await new_module.queue_full_pass_after_config_reload(
            SimpleNamespace(
                config=config,
                runtime_paths=runtime_paths,
                settings=new_env.settings,
                logger=new_env.logger,
                is_active=lambda: True,
            )
        )
        await new_module._run_export_pass(new_env, full_pass=True, room_ids=frozenset())
        lock.release()
        await asyncio.to_thread(worker_completed.wait)
        await _drain(new_module)
        await _shutdown_runner(new_module)

        _assert_export_retracted(export_dir)
    finally:
        if lock.locked():
            lock.release()


@pytest.mark.asyncio
async def test_incremental_pass_ignores_unresolved_private_instances(
    tmp_path: Path,
) -> None:
    """Dirty-room exports should defer orphan discovery and cleanup to full passes."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        ("@alice:hs",),
        persist_agent_identity=True,
    )
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
    module._remember_private_instance_requester(
        SimpleNamespace(
            config=config,
            runtime_paths=runtime_paths,
            settings=env.settings,
            logger=logger,
            is_active=lambda: True,
        ),
        "@alice:hs",
    )

    await module._run_export_pass(
        env,
        full_pass=False,
        room_ids=frozenset({"!changed:hs"}),
    )

    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert len(targets) == 1
    assert targets[0].output_dir == (
        instance_roots["@alice:hs"] / "secret_data" / "thread_exports"
    )
    assert ghost_export_dir.is_dir()
    assert not any(
        "without valid core identity" in call.args[0]
        for call in logger.warning.call_args_list
    )


@pytest.mark.asyncio
async def test_incremental_private_exports_ignore_other_agent_roots(
    tmp_path: Path,
) -> None:
    """An incremental pass must not inspect another private agent's export tree."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@alice:hs"
    config = Config(
        agents={
            "alpha": AgentConfig(
                display_name="Alpha",
                private=AgentPrivateConfig(per="user", root="private_workspace"),
            ),
            "beta": AgentConfig(
                display_name="Beta",
                private=AgentPrivateConfig(per="user", root="private_workspace"),
            ),
        },
        administrators=[requester_id],
    )
    runtime_paths = RuntimePaths(
        config_path=tmp_path / "config.yaml",
        config_dir=tmp_path,
        env_path=tmp_path / ".env",
        storage_root=tmp_path,
    )
    alpha_root = _materialize_private_instance(
        config, runtime_paths, requester_id, agent_name="alpha"
    )
    beta_root = _materialize_private_instance(
        config, runtime_paths, requester_id, agent_name="beta"
    )
    beta_export_dir = beta_root / "private_workspace" / "thread_exports"
    beta_export_dir.mkdir(parents=True)
    (beta_export_dir / "sentinel.yaml").write_text("keep", encoding="utf-8")
    logger = Mock()
    module._remember_private_instance_requester(
        SimpleNamespace(
            config=config,
            runtime_paths=runtime_paths,
            settings={"agents": ["alpha", "beta"]},
            logger=logger,
            is_active=lambda: True,
        ),
        requester_id,
    )
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": {"alpha": {"private_room_scope": "owner"}}},
        logger=logger,
    )

    await module._run_export_pass(
        env, full_pass=False, room_ids=frozenset({"!changed:hs"})
    )

    assert (beta_export_dir / "sentinel.yaml").exists()
    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert tuple(target.output_dir for target in targets) == (
        alpha_root / "private_workspace" / "thread_exports",
    )


@pytest.mark.asyncio
async def test_private_agent_owner_scope_requires_only_owner_membership(
    tmp_path: Path,
) -> None:
    """The explicit owner scope should include every room visible to the owner."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path,
        ("@alice:hs",),
        persist_agent_identity=True,
    )
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={
            "agents": {"secret": {"private_room_scope": "owner"}},
        },
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert len(targets) == 1
    assert targets[0].required_member_user_ids == ("@alice:hs",)


@pytest.mark.asyncio
async def test_message_requester_resolves_unlisted_private_owner(
    tmp_path: Path,
) -> None:
    """A current-room requester should resolve their private instance without static config."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config = Config(
        agents={
            "secret": AgentConfig(
                display_name="Secret",
                access=ResponderAccessConfig(current_room_members=True),
                private=AgentPrivateConfig(per="user_agent"),
            )
        },
    )
    runtime_paths = SimpleNamespace(
        storage_root=tmp_path, env_value=lambda _name, default=None: default
    )
    requester_id = "@alice:hs"
    instance_root = _materialize_private_instance(config, runtime_paths, requester_id)
    ctx = SimpleNamespace(
        envelope=SimpleNamespace(room_id="!private:hs", requester_id=requester_id),
        settings={
            "agents": {"secret": {"private_room_scope": "owner"}},
            "debounce_seconds": 0,
        },
        config=config,
        runtime_paths=runtime_paths,
        logger=Mock(),
        is_active=lambda: True,
    )

    await module.queue_room_on_message(ctx)
    await _drain(module)
    await _shutdown_runner(module)

    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert tuple(target.required_member_user_ids for target in targets) == (
        (requester_id,),
    )
    assert targets[0].output_dir == (instance_root / "secret_data" / "thread_exports")


@pytest.mark.asyncio
async def test_message_requester_without_private_instance_is_not_retained(
    tmp_path: Path,
) -> None:
    """Owner discovery state should not retain users without a private instance."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config = Config(
        agents={
            "secret": AgentConfig(
                display_name="Secret",
                access=ResponderAccessConfig(current_room_members=True),
                private=AgentPrivateConfig(per="user_agent"),
            )
        },
    )
    runtime_paths = SimpleNamespace(
        storage_root=tmp_path, env_value=lambda _name, default=None: default
    )
    ctx = SimpleNamespace(
        envelope=SimpleNamespace(room_id="!public:hs", requester_id="@visitor:hs"),
        settings={"agents": ["secret"], "debounce_seconds": 0},
        config=config,
        runtime_paths=runtime_paths,
        logger=Mock(),
        is_active=lambda: True,
    )

    await module.queue_room_on_message(ctx)
    await _drain(module)
    await _shutdown_runner(module)

    assert module._private_instance_requesters == {}


@pytest.mark.asyncio
async def test_after_response_resolves_new_private_instance(tmp_path: Path) -> None:
    """The response hook should discover an instance created after message ingress."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    config = Config(
        agents={
            "secret": AgentConfig(
                display_name="Secret",
                access=ResponderAccessConfig(current_room_members=True),
                private=AgentPrivateConfig(per="user_agent"),
            )
        },
    )
    runtime_paths = SimpleNamespace(
        storage_root=tmp_path, env_value=lambda _name, default=None: default
    )
    requester_id = "@new-owner:hs"
    settings = {
        "agents": {"secret": {"private_room_scope": "owner"}},
        "debounce_seconds": 0,
    }
    message_ctx = SimpleNamespace(
        envelope=SimpleNamespace(room_id="!private:hs", requester_id=requester_id),
        settings=settings,
        config=config,
        runtime_paths=runtime_paths,
        logger=Mock(),
        is_active=lambda: True,
    )

    await module.queue_room_on_message(message_ctx)
    await _drain(module)

    _materialize_private_instance(config, runtime_paths, requester_id)
    response_ctx = SimpleNamespace(
        result=SimpleNamespace(
            envelope=SimpleNamespace(room_id="!private:hs", requester_id=requester_id)
        ),
        settings=settings,
        config=config,
        runtime_paths=runtime_paths,
        logger=Mock(),
        is_active=lambda: True,
    )

    await module.queue_room_after_response(response_ctx)
    await _drain(module)
    await _shutdown_runner(module)

    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert tuple(target.required_member_user_ids for target in targets) == (
        (requester_id,),
    )
    assert (
        module.export_threads_to_targets_once.await_args.kwargs["room_filter"] is None
    )

    module._remember_private_instance_requester(response_ctx, requester_id)
    assert module._full_pass_pending is False


def test_private_identity_invalidation_and_repair_each_queue_full_reconciliation(
    tmp_path: Path,
) -> None:
    """Removing and restoring the same identity cannot strand non-triggering rooms."""
    module = _load_hooks_module()
    requester_id = "@owner:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
    )
    ctx = SimpleNamespace(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
        is_active=lambda: True,
    )
    record_path = (
        instance_roots[requester_id].parent / ".mindroom-private-instance.json"
    )
    valid_record = record_path.read_text(encoding="utf-8")

    module._remember_private_instance_requester(ctx, requester_id)
    module._full_pass_pending = False
    cached_requesters, _revision = module._private_instance_requesters_snapshot()
    record_path.unlink()
    targets = module._private_agent_export_targets(
        module._TriggerEnv(
            config=config,
            runtime_paths=runtime_paths,
            settings=ctx.settings,
            logger=ctx.logger,
        ),
        "secret",
        agent_user_id="@mindroom_secret:localhost",
        options=module._AgentExportSettings(),
        worker_scope="user",
        reconcile_instances=False,
        private_instance_requesters=cached_requesters,
    )

    assert targets == ()
    assert module._private_instance_requesters == {}
    assert module._full_pass_pending is True

    module._full_pass_pending = False
    record_path.write_text(valid_record, encoding="utf-8")
    module._remember_private_instance_requester(ctx, requester_id)

    assert module._full_pass_pending is True


@pytest.mark.asyncio
async def test_identity_invalidated_during_full_pass_is_evicted_for_repair(
    tmp_path: Path,
) -> None:
    """A full-pass invalidation must not leave an unchanged owner cached."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@owner:hs"
    config, runtime_paths, instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
    )
    record_path = (
        instance_roots[requester_id].parent / ".mindroom-private-instance.json"
    )
    valid_record = record_path.read_text(encoding="utf-8")
    original_discovery = module._discover_private_instance_requesters

    def invalidate_after_discovery(*args: object, **kwargs: object) -> object:
        discovered = original_discovery(*args, **kwargs)
        record_path.unlink()
        return discovered

    module._discover_private_instance_requesters = invalidate_after_discovery
    env = module._TriggerEnv(
        config=config,
        runtime_paths=runtime_paths,
        settings={"agents": ["secret"]},
        logger=Mock(),
    )

    await module._run_export_pass(env, full_pass=True, room_ids=frozenset())

    assert module._private_instance_requesters == {}
    assert module._full_pass_pending is True

    module._full_pass_pending = False
    record_path.write_text(valid_record, encoding="utf-8")
    module._remember_private_instance_requester(
        SimpleNamespace(
            config=config,
            runtime_paths=runtime_paths,
            settings=env.settings,
            logger=env.logger,
            is_active=lambda: True,
        ),
        requester_id,
    )

    assert module._full_pass_pending is True


def test_full_pass_queued_while_pending_work_drains_is_not_lost() -> None:
    """A worker-thread reconciliation request must survive a concurrent drain."""
    module = _load_hooks_module()
    source_lines, first_line = inspect.getsourcelines(module._drain_pending)
    reset_line = first_line + next(
        index
        for index, line in enumerate(source_lines)
        if line.strip() == "_full_pass_pending = False"
    )
    drain_paused = threading.Event()
    resume_drain = threading.Event()
    queue_finished = threading.Event()

    def pause_before_reset(frame: object, event: str, _arg: object) -> object:
        if (
            event == "line"
            and getattr(frame, "f_code", None) is module._drain_pending.__code__
            and getattr(frame, "f_lineno", None) == reset_line
        ):
            drain_paused.set()
            assert resume_drain.wait(1)
        return pause_before_reset

    def drain() -> None:
        sys.settrace(pause_before_reset)
        try:
            module._drain_pending()
        finally:
            sys.settrace(None)

    def queue() -> None:
        module._queue_full_pass()
        queue_finished.set()

    drain_thread = threading.Thread(target=drain)
    queue_thread = threading.Thread(target=queue)
    drain_thread.start()
    assert drain_paused.wait(1)
    try:
        queue_thread.start()
        assert not queue_finished.wait(0.05)
    finally:
        resume_drain.set()
        drain_thread.join(1)
        queue_thread.join(1)

    assert queue_finished.is_set()
    assert module._full_pass_pending is True


@pytest.mark.asyncio
@pytest.mark.parametrize("hook_name", ["message", "after_response"])
async def test_message_hooks_return_before_private_identity_discovery_finishes(
    tmp_path: Path,
    hook_name: str,
) -> None:
    """Message hooks must not wait for private identity filesystem reads."""
    module = _load_hooks_module()
    _autospec_export(module, side_effect=_target_stats)
    requester_id = "@requester:hs"
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
    )
    settings = _settings(["secret"])
    envelope = SimpleNamespace(room_id="!room:hs", requester_id=requester_id)
    common = {
        "config": config,
        "runtime_paths": runtime_paths,
        "settings": settings,
        "logger": Mock(),
        "is_active": lambda: True,
    }
    ctx = (
        SimpleNamespace(envelope=envelope, **common)
        if hook_name == "message"
        else SimpleNamespace(result=SimpleNamespace(envelope=envelope), **common)
    )
    original_discovery = module._private_instance_requesters_for_requester
    discovery_started = threading.Event()
    release_discovery = threading.Event()
    discovery_finished = threading.Event()

    def delayed_discovery(*args: object, **kwargs: object) -> object:
        discovery_started.set()
        assert release_discovery.wait(1)
        try:
            return original_discovery(*args, **kwargs)
        finally:
            discovery_finished.set()

    module._private_instance_requesters_for_requester = delayed_discovery
    hook = (
        module.queue_room_on_message
        if hook_name == "message"
        else module.queue_room_after_response
    )
    hook_task = asyncio.create_task(hook(ctx))
    try:
        assert await asyncio.to_thread(discovery_started.wait, 1)
        await asyncio.wait_for(asyncio.shield(hook_task), timeout=0.05)
        assert not discovery_finished.is_set()
    finally:
        release_discovery.set()
        with contextlib.suppress(asyncio.CancelledError):
            await hook_task
        await _drain(module)
        await _shutdown_runner(module)
    targets = module.export_threads_to_targets_once.await_args.kwargs["targets"]
    assert tuple(target.required_member_user_ids for target in targets) == (
        (requester_id, "@mindroom_secret:localhost"),
    )
    assert (
        module.export_threads_to_targets_once.await_args.kwargs["room_filter"] is None
    )


@pytest.mark.asyncio
async def test_private_identity_discovery_failure_does_not_stop_runner(
    tmp_path: Path,
) -> None:
    """A failed requester lookup must not block the queued room export."""
    module = _load_hooks_module()
    module._live_hook_seen = True
    _autospec_export(module, side_effect=_target_stats)
    module._private_instance_requesters_for_requester = Mock(
        side_effect=OSError("unavailable")
    )
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path,
        ("@requester:hs",),
        persist_agent_identity=True,
    )
    ctx = SimpleNamespace(
        envelope=SimpleNamespace(room_id="!room:hs", requester_id="@requester:hs"),
        config=config,
        runtime_paths=runtime_paths,
        settings=_settings(["secret"]),
        logger=Mock(),
        is_active=lambda: True,
    )

    await module.queue_room_on_message(ctx)
    await _drain(module)

    module.export_threads_to_targets_once.assert_awaited_once()
    assert module.export_threads_to_targets_once.await_args.kwargs["room_filter"] == (
        "!room:hs"
    )
    assert not module._runner_tasks["runner"].done()
    await _shutdown_runner(module)


@pytest.mark.asyncio
async def test_promoted_full_scan_failure_does_not_lose_dirty_room(
    tmp_path: Path,
) -> None:
    """A failed follow-up reconciliation must not suppress the dirty-room export."""
    module = _load_hooks_module()
    module._live_hook_seen = True
    _autospec_export(module, side_effect=_target_stats)
    module._discover_private_instance_requesters = Mock(
        side_effect=OSError("unavailable")
    )
    requester_id = "@requester:hs"
    config, runtime_paths, _instance_roots = _private_runtime(
        tmp_path,
        (requester_id,),
        persist_agent_identity=True,
    )
    ctx = SimpleNamespace(
        envelope=SimpleNamespace(room_id="!room:hs", requester_id=requester_id),
        config=config,
        runtime_paths=runtime_paths,
        settings=_settings(["secret"]),
        logger=Mock(),
        is_active=lambda: True,
    )

    await module.queue_room_on_message(ctx)
    await _drain(module)

    module.export_threads_to_targets_once.assert_awaited_once()
    assert module.export_threads_to_targets_once.await_args.kwargs["room_filter"] == (
        "!room:hs"
    )
    await _shutdown_runner(module)


@pytest.mark.asyncio
async def test_first_live_hook_after_source_reload_queues_full_pass(
    tmp_path: Path,
) -> None:
    """The first hook from a newly published module reconciles all export state."""
    module = _load_hooks_module()
    module._record_trigger = Mock()

    await module.queue_room_on_message(
        _message_ctx(tmp_path, "!changed:hs", _settings())
    )

    assert module._full_pass_pending is True
    assert module._pending_room_ids == {"!changed:hs"}


@pytest.mark.asyncio
async def test_stale_hook_after_source_reload_cannot_queue_work(tmp_path: Path) -> None:
    """A callback from a superseded registry cannot reactivate its old module."""
    module = _load_hooks_module()
    module._record_trigger = Mock()
    ctx = _message_ctx(tmp_path, "!changed:hs", _settings())
    ctx.is_active = lambda: False

    await module.queue_room_on_message(ctx)

    assert module._full_pass_pending is False
    assert module._pending_room_ids == set()
    module._record_trigger.assert_not_called()
