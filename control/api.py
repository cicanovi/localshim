from __future__ import annotations
from datetime import datetime
from typing import Any, cast
from fastapi import APIRouter, HTTPException, Query, Request
from core.apply import (
    DEFAULT_APPLY_MODE,
    SUPPORTED_APPLY_MODES,
    ConfigApplyCoordinator,
    ConfigApplyResult,
    ConfigApplyState,
    ConfigReloadResult,
)
from core.events import EventRecord, EventRecorder, EventRecorderStats
from core.plugins import PluginRuntime
from core.runtime import RuntimeManager, ShimRuntime

router = APIRouter(prefix="/shim", tags=["shim-control"])
DEFAULT_CONFIG_APPLY_MODE = DEFAULT_APPLY_MODE
DEFAULT_CONFIG_PERSIST = False


def _missing_dependency_error(name: str) -> HTTPException:
    return HTTPException(
        status_code=500,
        detail={
            "error": {
                "type": "control_dependency_missing",
                "message": f"Missing app.state.{name}",
            }
        },
    )


def _unsupported_config_apply_option_error(
    *, parameter: str, message: str
) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "error": {
                "type": "unsupported_config_apply_option",
                "parameter": parameter,
                "message": message,
            }
        },
    )


def _validate_config_apply_options(*, mode: str, persist: bool) -> None:
    if mode not in SUPPORTED_APPLY_MODES:
        supported = ", ".join(SUPPORTED_APPLY_MODES)
        raise _unsupported_config_apply_option_error(
            parameter="mode", message=f"mode must be one of: {supported}"
        )
    if persist:
        raise _unsupported_config_apply_option_error(
            parameter="persist", message="persist=true is not supported yet"
        )


def _get_app_state_value(request: Request, name: str) -> Any:
    value = getattr(request.app.state, name, None)
    if value is None:
        raise _missing_dependency_error(name)
    return value


def _get_runtime_manager(request: Request) -> RuntimeManager:
    return cast(RuntimeManager, _get_app_state_value(request, "runtime_manager"))


def _get_event_recorder(request: Request) -> EventRecorder:
    return cast(EventRecorder, _get_app_state_value(request, "event_recorder"))


def _get_apply_coordinator(request: Request) -> ConfigApplyCoordinator:
    return cast(
        ConfigApplyCoordinator,
        _get_app_state_value(request, "config_apply_coordinator"),
    )


def _serialize_datetime(value: datetime) -> str:
    return value.isoformat()


def _serialize_event(record: EventRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "timestamp": _serialize_datetime(record.timestamp),
        "event_type": record.event_type,
        "level": record.level,
        "request_id": record.request_id,
        "pipeline_run_id": record.pipeline_run_id,
        "attempt_number": record.attempt_number,
        "runtime_generation": record.runtime_generation,
        "runtime_fingerprint": record.runtime_fingerprint,
        "apply_id": record.apply_id,
        "phase": record.phase,
        "plugin": record.plugin,
        "checkpoint": record.checkpoint,
        "elapsed_ms": record.elapsed_ms,
        "details": record.details,
    }


def _serialize_event_stats(stats: EventRecorderStats) -> dict[str, Any]:
    return {
        "enabled": stats.enabled,
        "level": stats.level,
        "max_events": stats.max_events,
        "retained_events": stats.retained_events,
        "total_recorded": stats.total_recorded,
        "evicted_events": stats.evicted_events,
        "dropped_events": stats.dropped_events,
    }


def _serialize_plugin_runtime(plugin: PluginRuntime) -> dict[str, Any]:
    return {
        "name": plugin.name,
        "source": plugin.source,
        "entrypoint": plugin.entrypoint,
        "fail_mode": plugin.fail_mode,
        "timeout_seconds": plugin.timeout_seconds,
        "params_keys": sorted(plugin.params),
        "params_redacted": True,
    }


def _serialize_plugin_groups(runtime: ShimRuntime) -> dict[str, Any]:
    return {
        "upstream": [
            _serialize_plugin_runtime(plugin) for plugin in runtime.upstream_plugins
        ],
        "downstream": [
            _serialize_plugin_runtime(plugin) for plugin in runtime.downstream_plugins
        ],
    }


def _serialize_plugin_counts(runtime: ShimRuntime) -> dict[str, int]:
    return {
        "upstream_count": len(runtime.upstream_plugins),
        "downstream_count": len(runtime.downstream_plugins),
    }


def _runtime_fingerprint_short(runtime_fingerprint: str) -> str:
    digest = runtime_fingerprint.removeprefix("sha256:")
    if digest:
        return digest[:12]
    return runtime_fingerprint[:12]


def _serialize_runtime_fingerprint(runtime: ShimRuntime) -> dict[str, str]:
    return {
        "runtime_fingerprint": runtime.runtime_fingerprint,
        "runtime_fingerprint_short": _runtime_fingerprint_short(
            runtime.runtime_fingerprint
        ),
    }


def _serialize_config_summary(runtime: ShimRuntime) -> dict[str, Any]:
    config = runtime.config
    return {
        "redacted": True,
        "summary": {
            "has_backend": "backend" in config,
            "has_plugins": "plugins" in config,
            "has_control": "control" in config,
            "has_pipeline": "pipeline" in config,
        },
    }


def _serialize_apply_state(state: ConfigApplyState) -> dict[str, Any]:
    return {
        "latest_desired_apply_id": state.latest_desired_apply_id,
        "active_apply_id": state.active_apply_id,
        "active_state": state.active_state,
        "has_latest_desired_config": state.latest_desired_config is not None,
    }


def _serialize_config_apply_result(
    result: ConfigApplyResult,
    *,
    mode: str,
    persisted: bool,
    active_runtime: ShimRuntime | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": result.status,
        "apply_id": result.apply_id,
        "previous_generation": result.previous_generation,
        "candidate_generation": result.candidate_generation,
        "runtime_generation": result.runtime_generation,
        "current_runtime_generation": result.current_runtime_generation,
        "phase": result.phase,
        "persisted": persisted,
        "mode": mode,
        "old_runtime_preserved": result.old_runtime_preserved,
    }
    if active_runtime is not None:
        payload.update(_serialize_runtime_fingerprint(active_runtime))
    if result.status == "rejected":
        payload["error"] = {"type": result.error_type, "message": result.error_message}
    if result.status == "superseded":
        payload["superseded_by"] = result.superseded_by
        payload["message"] = (
            "A newer config request replaced this candidate before it was applied"
        )
    return payload


def _add_reload_metadata(
    payload: dict[str, Any], *, reload_result: ConfigReloadResult
) -> dict[str, Any]:
    payload["reload"] = {
        "source": reload_result.source,
        "config_path": reload_result.config_path,
    }
    return payload


def _serialize_config_reload_result(
    result: ConfigReloadResult, *, mode: str, active_runtime: ShimRuntime | None = None
) -> dict[str, Any]:
    if result.apply_result is not None:
        payload = _serialize_config_apply_result(
            result.apply_result,
            mode=mode,
            persisted=False,
            active_runtime=active_runtime,
        )
        return _add_reload_metadata(payload, reload_result=result)
    current_generation = result.current_runtime_generation
    payload: dict[str, Any] = {
        "status": result.status,
        "apply_id": None,
        "previous_generation": current_generation,
        "candidate_generation": None,
        "runtime_generation": current_generation,
        "current_runtime_generation": current_generation,
        "phase": result.phase,
        "persisted": False,
        "mode": mode,
        "old_runtime_preserved": result.old_runtime_preserved,
        "error": {"type": result.error_type, "message": result.error_message},
    }
    return _add_reload_metadata(payload, reload_result=result)


def _serialize_capabilities() -> dict[str, bool]:
    return {
        "plugin_pipeline": True,
        "raw_passthrough": True,
        "dynamic_config_core": True,
        "dynamic_config_api": True,
        "event_log": True,
        "model_streaming_sse": True,
    }


def _serialize_control_policy() -> dict[str, Any]:
    return {
        "apply_mode": DEFAULT_CONFIG_APPLY_MODE,
        "supported_apply_modes": list(SUPPORTED_APPLY_MODES),
        "gate_policy": "wait",
        "queue_policy": "latest_wins",
    }


def _serialize_pipeline_policy(runtime: ShimRuntime) -> dict[str, Any]:
    policy = runtime.pipeline_policy
    return {
        "in_flight_policy": policy.in_flight_policy,
        "max_replays_per_request": policy.max_replays_per_request,
        "max_retries_per_request": policy.max_retries_per_request,
        "max_reattempts_per_request": policy.max_reattempts_per_request,
    }


async def _serialize_status_payload(
    *, manager: RuntimeManager, event_recorder: EventRecorder
) -> dict[str, Any]:
    runtime = await manager.get_active_runtime()
    active_runs = await manager.active_run_count()
    gate_open = await manager.runtime_gate_open()
    event_stats = event_recorder.get_stats()
    return {
        "status": "ok",
        "service": "localshim",
        "runtime_generation": runtime.generation,
        **_serialize_runtime_fingerprint(runtime),
        "active_runs": active_runs,
        "gate_open": gate_open,
        "backend_url": runtime.backend_url,
        "plugins": _serialize_plugin_counts(runtime),
        "events": {
            "enabled": event_stats.enabled,
            "level": event_stats.level,
            "retained_events": event_stats.retained_events,
            "dropped_events": event_stats.dropped_events,
        },
        "capabilities": _serialize_capabilities(),
    }


async def _serialize_runtime_snapshot(
    *, manager: RuntimeManager, apply_coordinator: ConfigApplyCoordinator
) -> dict[str, Any]:
    runtime = await manager.get_active_runtime()
    active_runs = await manager.active_run_count()
    gate_open = await manager.runtime_gate_open()
    apply_state = await apply_coordinator.get_state()
    return {
        "runtime_generation": runtime.generation,
        **_serialize_runtime_fingerprint(runtime),
        "created_at": runtime.created_at,
        "config_path": runtime.config_path,
        "backend_url": runtime.backend_url,
        "plugins_enabled": runtime.plugins_enabled(),
        "state": {
            "active_runs": active_runs,
            "gate_open": gate_open,
            "gate_closed": not gate_open,
            "apply_in_progress": apply_state.active_state != "idle",
        },
        "apply": _serialize_apply_state(apply_state),
        "control_policy": _serialize_control_policy(),
        "pipeline_policy": _serialize_pipeline_policy(runtime),
        "config": _serialize_config_summary(runtime),
        "plugins": _serialize_plugin_groups(runtime),
    }


def _serialize_events_payload(
    *,
    event_recorder: EventRecorder,
    limit: int,
    since_id: int | None,
    event_type: str | None,
    level: str | None,
) -> dict[str, Any]:
    try:
        events = event_recorder.list_events(
            limit=limit, since_id=since_id, event_type=event_type, level=level
        )
    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail={"error": {"type": "invalid_event_filter", "message": str(error)}},
        ) from error
    stats = event_recorder.get_stats()
    return {
        "events": [_serialize_event(event) for event in events],
        "stats": _serialize_event_stats(stats),
        "filters": {
            "limit": limit,
            "since_id": since_id,
            "event_type": event_type,
            "level": level,
        },
    }


@router.get("/status")
async def get_status(request: Request):
    manager = _get_runtime_manager(request)
    event_recorder = _get_event_recorder(request)
    return await _serialize_status_payload(
        manager=manager, event_recorder=event_recorder
    )


@router.get("/runtime")
async def get_runtime(request: Request):
    manager = _get_runtime_manager(request)
    apply_coordinator = _get_apply_coordinator(request)
    return await _serialize_runtime_snapshot(
        manager=manager, apply_coordinator=apply_coordinator
    )


@router.get("/events")
async def get_events(
    request: Request,
    limit: int = Query(default=50, ge=0, le=500),
    since_id: int | None = Query(default=None, ge=0),
    event_type: str | None = None,
    level: str | None = None,
):
    event_recorder = _get_event_recorder(request)
    return _serialize_events_payload(
        event_recorder=event_recorder,
        limit=limit,
        since_id=since_id,
        event_type=event_type,
        level=level,
    )


@router.put("/config")
async def put_config(
    request: Request,
    candidate_config: dict[str, Any],
    mode: str = Query(default=DEFAULT_CONFIG_APPLY_MODE),
    persist: bool = Query(default=DEFAULT_CONFIG_PERSIST),
):
    _validate_config_apply_options(mode=mode, persist=persist)
    manager = _get_runtime_manager(request)
    apply_coordinator = _get_apply_coordinator(request)
    result = await apply_coordinator.request_config_apply(candidate_config, mode=mode)
    active_runtime = await manager.get_active_runtime()
    return _serialize_config_apply_result(
        result, mode=mode, persisted=persist, active_runtime=active_runtime
    )


@router.post("/config/reload")
async def reload_config(
    request: Request, mode: str = Query(default=DEFAULT_CONFIG_APPLY_MODE)
):
    _validate_config_apply_options(mode=mode, persist=False)
    manager = _get_runtime_manager(request)
    apply_coordinator = _get_apply_coordinator(request)
    result = await apply_coordinator.request_config_reload(mode=mode)
    active_runtime = await manager.get_active_runtime()
    return _serialize_config_reload_result(
        result, mode=mode, active_runtime=active_runtime
    )
