from __future__ import annotations
import asyncio
from typing import Any, cast
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from control.api import router as control_router
from core.apply import ConfigApplyCoordinator
from core.config import load_config, resolve_config_path
from core.events import EventRecorder
from core.errors import LocalShimError, error_response
from core.forwarder import forward_models
from core.lifecycle import PipelineRun
from core.logging import configure_logging, get_logger
from core.overrides import AppOverrides, apply_app_overrides
from core.runtime import RuntimeManager, ShimRuntime
from core.runtime_builder import create_initial_runtime

configure_logging()
logger = get_logger("localshim.app")


def create_event_recorder_from_config(config: dict[str, Any]) -> EventRecorder:
    observability = config.get("observability", {})
    events = observability.get("events", {})
    return EventRecorder(
        max_events=events.get("max_events", 500),
        enabled=events.get("enabled", True),
        level=events.get("level", "basic"),
    )


def log_runtime_startup(*, config_path: str, runtime: ShimRuntime) -> None:
    logger.info("Loaded config from %s", config_path)
    logger.info(
        "Created runtime generation=%s backend_url=%s",
        runtime.generation,
        runtime.backend_url,
    )
    logger.info(
        "Loaded upstream plugins: %s",
        [
            (plugin.name, plugin.fail_mode, plugin.timeout_seconds)
            for plugin in runtime.upstream_plugins
        ],
    )
    logger.info(
        "Loaded downstream plugins: %s",
        [
            (plugin.name, plugin.fail_mode, plugin.timeout_seconds)
            for plugin in runtime.downstream_plugins
        ],
    )


def create_app(
    *, config_path: str | None = None, overrides: AppOverrides | None = None
) -> FastAPI:
    resolved_config_path = (
        str(config_path) if config_path is not None else resolve_config_path()
    )
    loaded_config = load_config(resolved_config_path)
    effective_config = apply_app_overrides(loaded_config, overrides)
    initial_runtime = create_initial_runtime(
        config_path=resolved_config_path, config=effective_config, generation=1
    )
    runtime_manager = RuntimeManager(initial_runtime)
    event_recorder = create_event_recorder_from_config(initial_runtime.config)
    config_apply_coordinator = ConfigApplyCoordinator(
        runtime_manager, config_path=resolved_config_path, event_recorder=event_recorder
    )
    log_runtime_startup(config_path=resolved_config_path, runtime=initial_runtime)
    app = FastAPI()
    app.include_router(control_router)
    app.state.config_path = resolved_config_path
    app.state.initial_runtime = initial_runtime
    app.state.runtime_manager = runtime_manager
    app.state.event_recorder = event_recorder
    app.state.config_apply_coordinator = config_apply_coordinator

    @app.get("/")
    def root():
        return {"status": "ok"}

    @app.get("/v1/models")
    async def list_models(request: Request):
        manager = request.app.state.runtime_manager
        try:
            async with await manager.enter_run() as active_run:
                body, status_code, headers = await asyncio.to_thread(
                    forward_models, active_run.runtime.backend_url
                )
        except LocalShimError as error:
            return error_response(error)
        return JSONResponse(status_code=status_code, content=body, headers=headers)

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        manager = request.app.state.runtime_manager
        event_recorder = cast(
            EventRecorder | None, getattr(request.app.state, "event_recorder", None)
        )
        pipeline_run = PipelineRun(
            request=request,
            runtime_manager=manager,
            logger=logger,
            event_recorder=event_recorder,
        )
        return await pipeline_run.execute()

    return app
