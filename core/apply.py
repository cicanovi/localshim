from __future__ import annotations
import asyncio
import copy
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, Callable, Literal
from core.config import load_config
from core.errors import LocalShimError
from core.events import EventRecorder
from core.runtime import RuntimeManager, ShimRuntime
from core.runtime_builder import build_runtime

ApplyStatus = Literal["built", "applied", "rejected", "superseded"]
ApplyMode = Literal["late_gate", "early_gate"]
DEFAULT_APPLY_MODE: ApplyMode = "late_gate"
SUPPORTED_APPLY_MODES: tuple[ApplyMode, ...] = ("late_gate", "early_gate")
ApplyStateName = Literal[
    "idle", "queued", "building", "closing_gate", "draining", "swapping"
]
BuildRuntimeFn = Callable[[str, dict[str, Any], int], ShimRuntime]
ConfigLoaderFn = Callable[[str], Any]


@dataclass(frozen=True)
class ConfigApplyResult:
    status: ApplyStatus
    apply_id: int
    previous_generation: int
    current_runtime_generation: int
    candidate_generation: int | None = None
    runtime_generation: int | None = None
    candidate_runtime: ShimRuntime | None = None
    superseded_by: int | None = None
    phase: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    old_runtime_preserved: bool = True


@dataclass(frozen=True)
class ConfigReloadResult:
    status: ApplyStatus
    config_path: str
    source: str = "disk"
    apply_result: ConfigApplyResult | None = None
    phase: str = "config_load"
    error_type: str | None = None
    error_message: str | None = None
    current_runtime_generation: int | None = None
    old_runtime_preserved: bool = True


@dataclass(frozen=True)
class ConfigApplyState:
    latest_desired_apply_id: int | None
    active_apply_id: int | None
    active_state: ApplyStateName
    latest_desired_config: dict[str, Any] | None


class ConfigApplyCoordinator:
    def __init__(
        self,
        runtime_manager: RuntimeManager,
        *,
        config_path: str,
        build_runtime_fn: BuildRuntimeFn = build_runtime,
        config_loader_fn: ConfigLoaderFn = load_config,
        event_recorder: EventRecorder | None = None,
    ):
        self._runtime_manager = runtime_manager
        self._config_path = config_path
        self._build_runtime_fn = build_runtime_fn
        self._config_loader_fn = config_loader_fn
        (self._event_recorder): EventRecorder | None = event_recorder
        self._lock = asyncio.Lock()
        self._apply_lock = asyncio.Lock()
        self._next_apply_id = 1
        (self._latest_desired_apply_id): int | None = None
        (self._latest_desired_config): dict[str, Any] | None = None
        (self._apply_states): dict[int, ApplyStateName] = {}
        (self._running_apply_id): int | None = None
        (self._active_apply_id): int | None = None
        (self._active_state): ApplyStateName = "idle"
        (self._runtime_fingerprints_by_generation): dict[int, str] = {}

    async def request_config_apply(
        self, candidate_config: dict[str, Any], *, mode: str = DEFAULT_APPLY_MODE
    ) -> ConfigApplyResult:
        validate_apply_mode(mode)
        if mode == "early_gate":
            return await self._request_early_gate_apply(candidate_config)
        return await self._request_late_gate_apply(candidate_config)

    async def request_config_reload(
        self, *, mode: str = DEFAULT_APPLY_MODE
    ) -> ConfigReloadResult:
        validate_apply_mode(mode)
        active_runtime = await self._runtime_manager.get_active_runtime()
        self._remember_runtime_fingerprint(active_runtime)
        config_path = active_runtime.config_path
        self._record_reload_event(
            "config_reload_requested",
            phase="config_load",
            runtime_generation=active_runtime.generation,
            runtime_fingerprint=active_runtime.runtime_fingerprint,
            config_path=config_path,
            mode=mode,
        )
        try:
            loaded_config = await asyncio.to_thread(self._config_loader_fn, config_path)
            if not isinstance(loaded_config, dict):
                raise ValueError("Reloaded config must be a JSON object")
        except Exception as error:
            current_runtime = await self._runtime_manager.get_active_runtime()
            self._remember_runtime_fingerprint(current_runtime)
            error_type = get_config_reload_error_type(error)
            self._record_reload_event(
                "config_reload_rejected",
                phase="config_load",
                runtime_generation=current_runtime.generation,
                runtime_fingerprint=current_runtime.runtime_fingerprint,
                config_path=config_path,
                mode=mode,
                error_type=error_type,
            )
            return ConfigReloadResult(
                status="rejected",
                config_path=config_path,
                phase="config_load",
                error_type=error_type,
                error_message=str(error),
                current_runtime_generation=current_runtime.generation,
                old_runtime_preserved=True,
            )
        apply_result = await self.request_config_apply(loaded_config, mode=mode)
        current_runtime = await self._runtime_manager.get_active_runtime()
        self._remember_runtime_fingerprint(current_runtime)
        self._record_reload_event(
            "config_reload_completed",
            phase=apply_result.phase or "applied",
            runtime_generation=current_runtime.generation,
            runtime_fingerprint=current_runtime.runtime_fingerprint,
            config_path=config_path,
            mode=mode,
            error_type=apply_result.error_type,
        )
        return ConfigReloadResult(
            status=apply_result.status,
            config_path=config_path,
            apply_result=apply_result,
            phase=apply_result.phase or "applied",
            current_runtime_generation=apply_result.current_runtime_generation,
            old_runtime_preserved=apply_result.old_runtime_preserved,
        )

    def _remember_runtime_fingerprint(self, runtime: ShimRuntime | None) -> str | None:
        if runtime is None:
            return None
        self._runtime_fingerprints_by_generation[runtime.generation] = (
            runtime.runtime_fingerprint
        )
        return runtime.runtime_fingerprint

    def _runtime_fingerprint_for_generation(
        self, runtime_generation: int | None
    ) -> str | None:
        if runtime_generation is None:
            return None
        return self._runtime_fingerprints_by_generation.get(runtime_generation)

    async def _request_late_gate_apply(
        self, candidate_config: dict[str, Any]
    ) -> ConfigApplyResult:
        candidate_config_snapshot = copy.deepcopy(candidate_config)
        apply_id = await self._register_request(candidate_config_snapshot)
        active_runtime = await self._runtime_manager.get_active_runtime()
        self._remember_runtime_fingerprint(active_runtime)
        previous_generation = active_runtime.generation
        self._record_apply_event(
            "config_apply_requested",
            apply_id=apply_id,
            phase="queued",
            runtime_generation=previous_generation,
            previous_generation=previous_generation,
            candidate_generation=None,
        )
        candidate_generation: int | None = None
        gate_closed = False
        phase = "queued"
        apply_completed_pending = False
        completed_runtime_generation: int | None = None
        try:
            async with self._apply_lock:
                try:
                    active_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(active_runtime)
                    previous_generation = active_runtime.generation
                    superseded_by = await self._get_superseded_by(apply_id)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    if superseded_by is not None:
                        self._record_apply_event(
                            "config_candidate_superseded",
                            apply_id=apply_id,
                            phase="queued",
                            runtime_generation=current_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=None,
                            superseded_by=superseded_by,
                        )
                        return ConfigApplyResult(
                            status="superseded",
                            apply_id=apply_id,
                            previous_generation=previous_generation,
                            current_runtime_generation=current_runtime.generation,
                            candidate_generation=None,
                            runtime_generation=current_runtime.generation,
                            candidate_runtime=None,
                            superseded_by=superseded_by,
                            phase="queued",
                            old_runtime_preserved=True,
                        )
                    phase = "candidate_build"
                    await self._set_active_state(apply_id, "building")
                    candidate_generation = (
                        await self._runtime_manager.reserve_generation()
                    )
                    self._record_apply_event(
                        "config_candidate_build_started",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                    )
                    candidate_runtime = await asyncio.to_thread(
                        self._build_runtime_fn,
                        self._config_path,
                        candidate_config_snapshot,
                        candidate_generation,
                    )
                    self._remember_runtime_fingerprint(candidate_runtime)
                    self._record_apply_event(
                        "config_candidate_build_succeeded",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                    )
                    phase = "before_gate"
                    superseded_by = await self._get_superseded_by(apply_id)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    if superseded_by is not None:
                        self._record_apply_event(
                            "config_candidate_superseded",
                            apply_id=apply_id,
                            phase=phase,
                            runtime_generation=current_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            superseded_by=superseded_by,
                        )
                        return ConfigApplyResult(
                            status="superseded",
                            apply_id=apply_id,
                            previous_generation=previous_generation,
                            current_runtime_generation=current_runtime.generation,
                            candidate_generation=candidate_generation,
                            runtime_generation=current_runtime.generation,
                            candidate_runtime=None,
                            superseded_by=superseded_by,
                            phase=phase,
                            old_runtime_preserved=True,
                        )
                    phase = "closing_gate"
                    await self._set_active_state(apply_id, "closing_gate")
                    await self._runtime_manager.close_gate()
                    gate_closed = True
                    self._record_apply_event(
                        "runtime_gate_closed",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                    )
                    phase = "drain"
                    await self._set_active_state(apply_id, "draining")
                    self._record_apply_event(
                        "runtime_gate_waiting",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                    )
                    await self._runtime_manager.wait_for_drain()
                    self._record_apply_event(
                        "runtime_drained",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                    )
                    superseded_by = await self._get_superseded_by(apply_id)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    if superseded_by is not None:
                        self._record_apply_event(
                            "config_candidate_superseded",
                            apply_id=apply_id,
                            phase=phase,
                            runtime_generation=current_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            superseded_by=superseded_by,
                        )
                        return ConfigApplyResult(
                            status="superseded",
                            apply_id=apply_id,
                            previous_generation=previous_generation,
                            current_runtime_generation=current_runtime.generation,
                            candidate_generation=candidate_generation,
                            runtime_generation=current_runtime.generation,
                            candidate_runtime=None,
                            superseded_by=superseded_by,
                            phase=phase,
                            old_runtime_preserved=True,
                        )
                    phase = "swap"
                    await self._set_active_state(apply_id, "swapping")
                    await self._runtime_manager.swap_runtime(candidate_runtime)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    self._record_apply_event(
                        "runtime_generation_swapped",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=current_runtime.generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                    )
                    apply_completed_pending = True
                    completed_runtime_generation = current_runtime.generation
                    return ConfigApplyResult(
                        status="applied",
                        apply_id=apply_id,
                        previous_generation=previous_generation,
                        current_runtime_generation=current_runtime.generation,
                        candidate_generation=candidate_generation,
                        runtime_generation=current_runtime.generation,
                        candidate_runtime=candidate_runtime,
                        phase="applied",
                        old_runtime_preserved=False,
                    )
                finally:
                    if gate_closed:
                        await self._runtime_manager.open_gate()
                        gate_closed = False
                        gate_open_runtime = (
                            await self._runtime_manager.get_active_runtime()
                        )
                        self._remember_runtime_fingerprint(gate_open_runtime)
                        self._record_apply_event(
                            "runtime_gate_opened",
                            level="detailed",
                            apply_id=apply_id,
                            phase=phase,
                            runtime_generation=gate_open_runtime.generation,
                            runtime_fingerprint=gate_open_runtime.runtime_fingerprint,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                        )
                    if apply_completed_pending:
                        self._record_apply_event(
                            "config_apply_completed",
                            apply_id=apply_id,
                            phase="applied",
                            runtime_generation=completed_runtime_generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            old_runtime_preserved=False,
                        )
                        apply_completed_pending = False
        except Exception as error:
            current_runtime = await self._runtime_manager.get_active_runtime()
            self._remember_runtime_fingerprint(current_runtime)
            error_type = get_error_type(error)
            old_runtime_preserved = current_runtime.generation == previous_generation
            if phase == "candidate_build":
                self._record_apply_event(
                    "config_candidate_build_failed",
                    level="detailed",
                    apply_id=apply_id,
                    phase=phase,
                    runtime_generation=current_runtime.generation,
                    previous_generation=previous_generation,
                    candidate_generation=candidate_generation,
                    error_type=error_type,
                )
            self._record_apply_event(
                "config_apply_rejected",
                apply_id=apply_id,
                phase=phase,
                runtime_generation=current_runtime.generation,
                previous_generation=previous_generation,
                candidate_generation=candidate_generation,
                error_type=error_type,
                old_runtime_preserved=old_runtime_preserved,
            )
            return ConfigApplyResult(
                status="rejected",
                apply_id=apply_id,
                previous_generation=previous_generation,
                current_runtime_generation=current_runtime.generation,
                candidate_generation=candidate_generation,
                runtime_generation=current_runtime.generation,
                candidate_runtime=None,
                phase=phase,
                error_type=error_type,
                error_message=str(error),
                old_runtime_preserved=old_runtime_preserved,
            )
        finally:
            await self._finish_request(apply_id)

    async def _request_early_gate_apply(
        self, candidate_config: dict[str, Any]
    ) -> ConfigApplyResult:
        mode: ApplyMode = "early_gate"
        candidate_config_snapshot = copy.deepcopy(candidate_config)
        apply_id = await self._register_request(candidate_config_snapshot)
        active_runtime = await self._runtime_manager.get_active_runtime()
        self._remember_runtime_fingerprint(active_runtime)
        previous_generation = active_runtime.generation
        self._record_apply_event(
            "config_apply_requested",
            apply_id=apply_id,
            phase="queued",
            runtime_generation=previous_generation,
            previous_generation=previous_generation,
            candidate_generation=None,
            mode=mode,
        )
        candidate_generation: int | None = None
        gate_closed = False
        phase = "queued"
        apply_completed_pending = False
        completed_runtime_generation: int | None = None
        try:
            async with self._apply_lock:
                try:
                    active_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(active_runtime)
                    previous_generation = active_runtime.generation
                    superseded_by = await self._get_superseded_by(apply_id)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    if superseded_by is not None:
                        self._record_apply_event(
                            "config_candidate_superseded",
                            apply_id=apply_id,
                            phase="queued",
                            runtime_generation=current_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=None,
                            superseded_by=superseded_by,
                            mode=mode,
                        )
                        return ConfigApplyResult(
                            status="superseded",
                            apply_id=apply_id,
                            previous_generation=previous_generation,
                            current_runtime_generation=current_runtime.generation,
                            candidate_generation=None,
                            runtime_generation=current_runtime.generation,
                            candidate_runtime=None,
                            superseded_by=superseded_by,
                            phase="queued",
                            old_runtime_preserved=True,
                        )
                    phase = "closing_gate"
                    await self._set_active_state(apply_id, "closing_gate")
                    await self._runtime_manager.close_gate()
                    gate_closed = True
                    self._record_apply_event(
                        "runtime_gate_closed",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=None,
                        mode=mode,
                    )
                    phase = "candidate_build"
                    await self._set_active_state(apply_id, "building")
                    candidate_generation = (
                        await self._runtime_manager.reserve_generation()
                    )
                    self._record_apply_event(
                        "config_candidate_build_started",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                        mode=mode,
                    )
                    candidate_runtime = await asyncio.to_thread(
                        self._build_runtime_fn,
                        self._config_path,
                        candidate_config_snapshot,
                        candidate_generation,
                    )
                    self._remember_runtime_fingerprint(candidate_runtime)
                    self._record_apply_event(
                        "config_candidate_build_succeeded",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                        mode=mode,
                    )
                    superseded_by = await self._get_superseded_by(apply_id)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    if superseded_by is not None:
                        self._record_apply_event(
                            "config_candidate_superseded",
                            apply_id=apply_id,
                            phase=phase,
                            runtime_generation=current_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            superseded_by=superseded_by,
                            mode=mode,
                        )
                        return ConfigApplyResult(
                            status="superseded",
                            apply_id=apply_id,
                            previous_generation=previous_generation,
                            current_runtime_generation=current_runtime.generation,
                            candidate_generation=candidate_generation,
                            runtime_generation=current_runtime.generation,
                            candidate_runtime=None,
                            superseded_by=superseded_by,
                            phase=phase,
                            old_runtime_preserved=True,
                        )
                    phase = "drain"
                    await self._set_active_state(apply_id, "draining")
                    self._record_apply_event(
                        "runtime_gate_waiting",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                        mode=mode,
                    )
                    await self._runtime_manager.wait_for_drain()
                    self._record_apply_event(
                        "runtime_drained",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=previous_generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                        mode=mode,
                    )
                    superseded_by = await self._get_superseded_by(apply_id)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    if superseded_by is not None:
                        self._record_apply_event(
                            "config_candidate_superseded",
                            apply_id=apply_id,
                            phase=phase,
                            runtime_generation=current_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            superseded_by=superseded_by,
                            mode=mode,
                        )
                        return ConfigApplyResult(
                            status="superseded",
                            apply_id=apply_id,
                            previous_generation=previous_generation,
                            current_runtime_generation=current_runtime.generation,
                            candidate_generation=candidate_generation,
                            runtime_generation=current_runtime.generation,
                            candidate_runtime=None,
                            superseded_by=superseded_by,
                            phase=phase,
                            old_runtime_preserved=True,
                        )
                    phase = "swap"
                    await self._set_active_state(apply_id, "swapping")
                    await self._runtime_manager.swap_runtime(candidate_runtime)
                    current_runtime = await self._runtime_manager.get_active_runtime()
                    self._remember_runtime_fingerprint(current_runtime)
                    self._record_apply_event(
                        "runtime_generation_swapped",
                        level="detailed",
                        apply_id=apply_id,
                        phase=phase,
                        runtime_generation=current_runtime.generation,
                        previous_generation=previous_generation,
                        candidate_generation=candidate_generation,
                        mode=mode,
                    )
                    apply_completed_pending = True
                    completed_runtime_generation = current_runtime.generation
                    return ConfigApplyResult(
                        status="applied",
                        apply_id=apply_id,
                        previous_generation=previous_generation,
                        current_runtime_generation=current_runtime.generation,
                        candidate_generation=candidate_generation,
                        runtime_generation=current_runtime.generation,
                        candidate_runtime=candidate_runtime,
                        phase="applied",
                        old_runtime_preserved=False,
                    )
                finally:
                    if gate_closed:
                        await self._runtime_manager.open_gate()
                        gate_closed = False
                        gate_open_runtime = (
                            await self._runtime_manager.get_active_runtime()
                        )
                        self._remember_runtime_fingerprint(gate_open_runtime)
                        self._record_apply_event(
                            "runtime_gate_opened",
                            level="detailed",
                            apply_id=apply_id,
                            phase=phase,
                            runtime_generation=gate_open_runtime.generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            mode=mode,
                        )
                    if apply_completed_pending:
                        self._record_apply_event(
                            "config_apply_completed",
                            apply_id=apply_id,
                            phase="applied",
                            runtime_generation=completed_runtime_generation,
                            previous_generation=previous_generation,
                            candidate_generation=candidate_generation,
                            old_runtime_preserved=False,
                            mode=mode,
                        )
                        apply_completed_pending = False
        except Exception as error:
            current_runtime = await self._runtime_manager.get_active_runtime()
            self._remember_runtime_fingerprint(current_runtime)
            error_type = get_error_type(error)
            old_runtime_preserved = current_runtime.generation == previous_generation
            if phase == "candidate_build":
                self._record_apply_event(
                    "config_candidate_build_failed",
                    level="detailed",
                    apply_id=apply_id,
                    phase=phase,
                    runtime_generation=current_runtime.generation,
                    previous_generation=previous_generation,
                    candidate_generation=candidate_generation,
                    error_type=error_type,
                    mode=mode,
                )
            self._record_apply_event(
                "config_apply_rejected",
                apply_id=apply_id,
                phase=phase,
                runtime_generation=current_runtime.generation,
                previous_generation=previous_generation,
                candidate_generation=candidate_generation,
                error_type=error_type,
                old_runtime_preserved=old_runtime_preserved,
                mode=mode,
            )
            return ConfigApplyResult(
                status="rejected",
                apply_id=apply_id,
                previous_generation=previous_generation,
                current_runtime_generation=current_runtime.generation,
                candidate_generation=candidate_generation,
                runtime_generation=current_runtime.generation,
                candidate_runtime=None,
                phase=phase,
                error_type=error_type,
                error_message=str(error),
                old_runtime_preserved=old_runtime_preserved,
            )
        finally:
            await self._finish_request(apply_id)

    def _record_apply_event(
        self,
        event_type: str,
        *,
        level: str = "basic",
        apply_id: int,
        phase: str,
        runtime_generation: int | None = None,
        runtime_fingerprint: str | None = None,
        previous_generation: int | None = None,
        candidate_generation: int | None = None,
        superseded_by: int | None = None,
        error_type: str | None = None,
        old_runtime_preserved: bool | None = None,
        mode: str = "late_gate",
    ) -> None:
        if self._event_recorder is None:
            return
        event_runtime_fingerprint = (
            runtime_fingerprint
            or self._runtime_fingerprint_for_generation(runtime_generation)
        )
        details: dict[str, Any] = {
            "mode": mode,
            "candidate_generation": candidate_generation,
        }
        if previous_generation is not None:
            details["previous_generation"] = previous_generation
        if superseded_by is not None:
            details["superseded_by"] = superseded_by
        if error_type is not None:
            details["error_type"] = error_type
        if old_runtime_preserved is not None:
            details["old_runtime_preserved"] = old_runtime_preserved
        try:
            self._event_recorder.record(
                event_type,
                level=level,
                apply_id=apply_id,
                phase=phase,
                runtime_generation=runtime_generation,
                runtime_fingerprint=event_runtime_fingerprint,
                details=details,
            )
        except Exception:
            return

    def _record_reload_event(
        self,
        event_type: str,
        *,
        phase: str,
        runtime_generation: int | None,
        config_path: str,
        mode: str = "late_gate",
        runtime_fingerprint: str | None = None,
        error_type: str | None = None,
    ) -> None:
        if self._event_recorder is None:
            return
        event_runtime_fingerprint = (
            runtime_fingerprint
            or self._runtime_fingerprint_for_generation(runtime_generation)
        )
        details: dict[str, Any] = {
            "mode": mode,
            "source": "disk",
            "config_path": config_path,
            "config_body_omitted": True,
        }
        if error_type is not None:
            details["error_type"] = error_type
        try:
            self._event_recorder.record(
                event_type,
                phase=phase,
                runtime_generation=runtime_generation,
                runtime_fingerprint=event_runtime_fingerprint,
                details=details,
            )
        except Exception:
            return

    async def get_state(self) -> ConfigApplyState:
        async with self._lock:
            latest_desired_config = (
                copy.deepcopy(self._latest_desired_config)
                if self._latest_desired_config is not None
                else None
            )
            return ConfigApplyState(
                latest_desired_apply_id=self._latest_desired_apply_id,
                active_apply_id=self._active_apply_id,
                active_state=self._active_state,
                latest_desired_config=latest_desired_config,
            )

    async def _set_active_state(
        self, apply_id: int, active_state: ApplyStateName
    ) -> None:
        async with self._lock:
            if apply_id not in self._apply_states:
                return
            self._apply_states[apply_id] = active_state
            if active_state != "queued":
                self._running_apply_id = apply_id
            elif self._running_apply_id == apply_id:
                self._running_apply_id = None
            self._refresh_active_state_locked()

    async def _register_request(self, candidate_config_snapshot: dict[str, Any]) -> int:
        async with self._lock:
            apply_id = self._next_apply_id
            self._next_apply_id += 1
            self._latest_desired_apply_id = apply_id
            self._latest_desired_config = copy.deepcopy(candidate_config_snapshot)
            self._apply_states[apply_id] = "queued"
            self._refresh_active_state_locked()
            return apply_id

    async def _get_superseded_by(self, apply_id: int) -> int | None:
        async with self._lock:
            if self._latest_desired_apply_id != apply_id:
                return self._latest_desired_apply_id
            return None

    async def _finish_request(self, apply_id: int) -> None:
        async with self._lock:
            self._apply_states.pop(apply_id, None)
            if self._running_apply_id == apply_id:
                self._running_apply_id = None
            self._refresh_active_state_locked()

    def _refresh_active_state_locked(self) -> None:
        if (
            self._running_apply_id is not None
            and self._running_apply_id in self._apply_states
        ):
            self._active_apply_id = self._running_apply_id
            self._active_state = self._apply_states[self._running_apply_id]
            return
        if self._apply_states:
            self._active_apply_id = max(self._apply_states)
            self._active_state = self._apply_states[self._active_apply_id]
            return
        self._active_apply_id = None
        self._active_state = "idle"


def validate_apply_mode(mode: str) -> None:
    if mode not in SUPPORTED_APPLY_MODES:
        supported = ", ".join(SUPPORTED_APPLY_MODES)
        raise ValueError(f"Config apply mode must be one of: {supported}")


def get_error_type(error: Exception) -> str:
    if isinstance(error, LocalShimError):
        return error.error_type
    return error.__class__.__name__


def get_config_reload_error_type(error: Exception) -> str:
    if isinstance(error, FileNotFoundError):
        return "config_file_not_found"
    if isinstance(error, JSONDecodeError):
        return "config_json_invalid"
    if isinstance(error, OSError):
        return "config_file_read_error"
    if isinstance(error, ValueError):
        return "config_shape_invalid"
    return get_error_type(error)
