from __future__ import annotations
import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from logging import Logger
from typing import Any
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask
from core.context import ShimContext
from core.errors import (
    BackendRequestError,
    BadRequestError,
    InternalServerError,
    LocalShimError,
    RuntimeReconfiguringError,
    StreamingDownstreamPluginsUnsupportedError,
    error_response,
)
from core.events import EventRecorder
from core.forwarder import (
    filter_response_headers,
    forward_raw_body,
    forward_request,
    forward_streaming_json_request,
)
from core.logging import log_plugin_summary
from core.pipeline import PluginExecutionError, run_plugin_chain_async
from core.runtime import ActiveRun, RuntimeManager, ShimRuntime


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PreBackendReplayRequested(Exception):
    def __init__(
        self,
        *,
        checkpoint: str,
        old_runtime_generation: int,
        replay_count: int,
        max_replays: int,
        plugin_name: str | None = None,
    ) -> None:
        self.checkpoint = checkpoint
        self.old_runtime_generation = old_runtime_generation
        self.replay_count = replay_count
        self.max_replays = max_replays
        self.plugin_name = plugin_name
        super().__init__(f"Pre-backend replay requested at {checkpoint}")


class PostBackendGenerationReattemptRequested(Exception):
    def __init__(
        self,
        *,
        checkpoint: str,
        old_runtime_generation: int,
        reattempt_count: int,
        max_reattempts: int,
        plugin_name: str | None = None,
    ) -> None:
        self.checkpoint = checkpoint
        self.old_runtime_generation = old_runtime_generation
        self.reattempt_count = reattempt_count
        self.max_reattempts = max_reattempts
        self.plugin_name = plugin_name
        super().__init__(f"Post-backend generation reattempt requested at {checkpoint}")


class PipelineRun:
    def __init__(
        self,
        *,
        request: Request,
        runtime_manager: RuntimeManager,
        logger: Logger,
        event_recorder: EventRecorder | None = None,
    ):
        self.request = request
        self.runtime_manager = runtime_manager
        (self.runtime): ShimRuntime | None = None
        self.logger = logger
        (self._event_recorder): EventRecorder | None = event_recorder
        self.request_id = uuid.uuid4().hex
        self.pipeline_run_id = uuid.uuid4().hex
        (self.ingress_body): bytes | None = None
        (self.attempts): list[PipelineAttempt] = []
        self.replay_count = 0
        self.retry_count = 0
        self.reattempt_count = 0
        self.started_at = utc_now()
        self._started_perf = time.perf_counter()
        (self.finished_at): str | None = None
        (self.final_status): str | None = None

    def _elapsed_ms(self) -> float:
        return (time.perf_counter() - self._started_perf) * 1000

    def _completion_details(self) -> dict[str, Any]:
        return {
            "final_status": self.final_status,
            "attempt_count": len(self.attempts),
            "replay_count": self.replay_count,
            "retry_count": self.retry_count,
            "reattempt_count": self.reattempt_count,
        }

    def _record_pipeline_event(
        self,
        event_type: str,
        *,
        level: str = "basic",
        phase: str | None = None,
        checkpoint: str | None = None,
        elapsed_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if self._event_recorder is None:
            return
        runtime = self.runtime
        event_details: dict[str, Any] = {
            "method": self.request.method,
            "path": self.request.url.path,
            "plugins_enabled": runtime.plugins_enabled()
            if runtime is not None
            else False,
        }
        if details is not None:
            event_details.update(details)
        try:
            self._event_recorder.record(
                event_type,
                level=level,
                request_id=self.request_id,
                pipeline_run_id=self.pipeline_run_id,
                runtime_generation=runtime.generation if runtime is not None else None,
                runtime_fingerprint=runtime.runtime_fingerprint
                if runtime is not None
                else None,
                phase=phase,
                checkpoint=checkpoint,
                elapsed_ms=elapsed_ms,
                details=event_details,
            )
        except Exception:
            return

    async def execute(self):
        try:
            ingress_body = await self.request.body()
            self.ingress_body = ingress_body
        except Exception:
            self.final_status = "failed"
            self.finished_at = utc_now()
            self._record_pipeline_event(
                "pipeline_run_completed",
                phase="run",
                elapsed_ms=self._elapsed_ms(),
                details=self._completion_details(),
            )
            raise
        attempt_number = 1
        while True:
            try:
                return await self._execute_single_attempt(
                    attempt_number=attempt_number, ingress_body=ingress_body
                )
            except PreBackendReplayRequested as replay:
                if self.replay_count >= replay.max_replays:
                    self.final_status = "error"
                    self.finished_at = utc_now()
                    internal_error = InternalServerError(
                        "Pre-backend replay requested after replay limit was reached"
                    )
                    self._record_pipeline_event(
                        "pipeline_run_completed",
                        phase="run",
                        checkpoint=replay.checkpoint,
                        elapsed_ms=self._elapsed_ms(),
                        details={
                            **self._completion_details(),
                            "error_type": internal_error.error_type,
                            "old_runtime_generation": replay.old_runtime_generation,
                            "replay_count": self.replay_count,
                            "max_replays": replay.max_replays,
                        },
                    )
                    return error_response(internal_error)
                self.replay_count += 1
                attempt_number += 1
                self._record_pipeline_event(
                    "pipeline_replayed_from_ingress",
                    phase="run",
                    checkpoint=replay.checkpoint,
                    details={
                        "old_runtime_generation": replay.old_runtime_generation,
                        "replay_count": self.replay_count,
                        "max_replays": replay.max_replays,
                        "plugin_name": replay.plugin_name,
                    },
                )
                continue
            except PostBackendGenerationReattemptRequested as reattempt:
                if self.reattempt_count >= reattempt.max_reattempts:
                    self.final_status = "error"
                    self.finished_at = utc_now()
                    internal_error = InternalServerError(
                        "Post-backend generation reattempt requested after reattempt limit was reached"
                    )
                    self._record_pipeline_event(
                        "pipeline_run_completed",
                        phase="run",
                        checkpoint=reattempt.checkpoint,
                        elapsed_ms=self._elapsed_ms(),
                        details={
                            **self._completion_details(),
                            "error_type": internal_error.error_type,
                            "old_runtime_generation": reattempt.old_runtime_generation,
                            "reattempt_count": self.reattempt_count,
                            "max_reattempts": reattempt.max_reattempts,
                            "plugin_name": reattempt.plugin_name,
                        },
                    )
                    return error_response(internal_error)
                self.reattempt_count += 1
                attempt_number += 1
                self._record_pipeline_event(
                    "pipeline_reattempted_from_ingress",
                    phase="run",
                    checkpoint=reattempt.checkpoint,
                    details={
                        "old_runtime_generation": reattempt.old_runtime_generation,
                        "reattempt_count": self.reattempt_count,
                        "max_reattempts": reattempt.max_reattempts,
                        "plugin_name": reattempt.plugin_name,
                    },
                )
                continue

    async def _execute_single_attempt(
        self, *, attempt_number: int, ingress_body: bytes
    ):
        active_run: ActiveRun | None = None
        try:
            try:
                active_run = await self.runtime_manager.enter_run()
            except RuntimeReconfiguringError as error:
                self.final_status = "error"
                self.finished_at = utc_now()
                self._record_pipeline_event(
                    "pipeline_run_completed",
                    phase="run",
                    elapsed_ms=self._elapsed_ms(),
                    details=self._completion_details(),
                )
                return error_response(error)
            runtime = active_run.runtime
            self.runtime = runtime
            if attempt_number == 1:
                self.logger.info(
                    "Starting pipeline run: request_id=%s pipeline_run_id=%s runtime_generation=%s",
                    self.request_id,
                    self.pipeline_run_id,
                    runtime.generation,
                )
                self._record_pipeline_event("pipeline_run_started", phase="run")
            attempt = PipelineAttempt(
                request=self.request,
                runtime=runtime,
                runtime_manager=self.runtime_manager,
                ingress_body=ingress_body,
                request_id=self.request_id,
                pipeline_run_id=self.pipeline_run_id,
                attempt_number=attempt_number,
                replay_count=self.replay_count,
                reattempt_count=self.reattempt_count,
                pipeline_run=self,
                logger=self.logger,
                event_recorder=self._event_recorder,
            )
            self.attempts.append(attempt)
        except Exception:
            self.final_status = "failed"
            self.finished_at = utc_now()
            self._record_pipeline_event(
                "pipeline_run_completed",
                phase="run",
                elapsed_ms=self._elapsed_ms(),
                details=self._completion_details(),
            )
            if active_run is not None:
                await active_run.release()
            raise
        assert active_run is not None
        if attempt.ingress_requests_streaming():
            if not runtime.plugins_enabled():
                return await self._execute_raw_attempt(
                    attempt=attempt, active_run=active_run
                )
            return await self._execute_streaming_attempt(
                attempt=attempt, active_run=active_run
            )
        return await self._execute_plugin_attempt(
            attempt=attempt, active_run=active_run
        )

    async def _execute_plugin_attempt(
        self, *, attempt: PipelineAttempt, active_run: ActiveRun
    ):
        attempt_restart_requested = False
        async with active_run:
            try:
                response = await attempt.execute_plugin_pipeline()
                self.final_status = self._status_from_response(response)
                return response
            except (PreBackendReplayRequested, PostBackendGenerationReattemptRequested):
                attempt_restart_requested = True
                raise
            except Exception:
                self.final_status = "failed"
                raise
            finally:
                if not attempt_restart_requested:
                    self.finished_at = utc_now()
                    self._record_pipeline_event(
                        "pipeline_run_completed",
                        phase="run",
                        elapsed_ms=self._elapsed_ms(),
                        details=self._completion_details(),
                    )

    async def _execute_raw_attempt(
        self, *, attempt: PipelineAttempt, active_run: ActiveRun
    ):
        try:
            _, backend_response = await attempt.open_raw_passthrough()
        except Exception:
            self.final_status = "failed"
            self.finished_at = utc_now()
            self._record_pipeline_event(
                "pipeline_run_completed",
                phase="run",
                elapsed_ms=self._elapsed_ms(),
                details=self._completion_details(),
            )
            await active_run.release()
            raise

        async def cleanup_raw_stream():
            try:
                await attempt.cleanup_raw_passthrough()
                self.final_status = self._status_from_code(backend_response.status_code)
            except Exception:
                self.final_status = "failed"
                self.logger.exception(
                    "Raw passthrough cleanup failed: request_id=%s pipeline_run_id=%s",
                    self.request_id,
                    self.pipeline_run_id,
                )
                raise
            finally:
                self.finished_at = utc_now()
                self._record_pipeline_event(
                    "pipeline_run_completed",
                    phase="run",
                    elapsed_ms=self._elapsed_ms(),
                    details={
                        **self._completion_details(),
                        "status_code": backend_response.status_code,
                    },
                )
                await active_run.release()

        return StreamingResponse(
            backend_response.aiter_bytes(),
            status_code=backend_response.status_code,
            headers=filter_response_headers(backend_response.headers),
            media_type=backend_response.headers.get("content-type"),
            background=BackgroundTask(cleanup_raw_stream),
        )

    async def _execute_streaming_attempt(
        self, *, attempt: PipelineAttempt, active_run: ActiveRun
    ):
        try:
            opened = await attempt.open_streaming_plugin_passthrough()
        except (PreBackendReplayRequested, PostBackendGenerationReattemptRequested):
            await active_run.release()
            raise
        except Exception:
            self.final_status = "failed"
            self.finished_at = utc_now()
            self._record_pipeline_event(
                "pipeline_run_completed",
                phase="run",
                elapsed_ms=self._elapsed_ms(),
                details=self._completion_details(),
            )
            await active_run.release()
            raise
        if not isinstance(opened, tuple):
            self.final_status = self._status_from_response(opened)
            self.finished_at = utc_now()
            self._record_pipeline_event(
                "pipeline_run_completed",
                phase="run",
                elapsed_ms=self._elapsed_ms(),
                details=self._completion_details(),
            )
            await active_run.release()
            return opened
        _, backend_response = opened

        async def cleanup_streaming_response():
            try:
                await attempt.cleanup_raw_passthrough(phase="streaming")
                self.final_status = self._status_from_code(backend_response.status_code)
            except Exception:
                self.final_status = "failed"
                self.logger.exception(
                    "Streaming passthrough cleanup failed: request_id=%s pipeline_run_id=%s",
                    self.request_id,
                    self.pipeline_run_id,
                )
                raise
            finally:
                self.finished_at = utc_now()
                self._record_pipeline_event(
                    "pipeline_run_completed",
                    phase="run",
                    elapsed_ms=self._elapsed_ms(),
                    details={
                        **self._completion_details(),
                        "status_code": backend_response.status_code,
                    },
                )
                await active_run.release()

        return StreamingResponse(
            backend_response.aiter_bytes(),
            status_code=backend_response.status_code,
            headers=filter_response_headers(backend_response.headers),
            media_type=backend_response.headers.get(
                "content-type", "text/event-stream"
            ),
            background=BackgroundTask(cleanup_streaming_response),
        )

    @staticmethod
    def _status_from_response(response: Any) -> str:
        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int):
            return PipelineRun._status_from_code(status_code)
        return "succeeded"

    @staticmethod
    def _status_from_code(status_code: int) -> str:
        if status_code >= 400:
            return "error"
        return "succeeded"


class PipelineAttempt:
    def __init__(
        self,
        *,
        request: Request,
        runtime: ShimRuntime,
        runtime_manager: RuntimeManager,
        ingress_body: bytes,
        request_id: str,
        pipeline_run_id: str,
        attempt_number: int,
        replay_count: int,
        reattempt_count: int,
        pipeline_run: PipelineRun,
        logger: Logger,
        event_recorder: EventRecorder | None = None,
    ):
        self.request = request
        self.runtime = runtime
        self.runtime_manager = runtime_manager
        self.ingress_body = ingress_body
        self.request_id = request_id
        self.pipeline_run_id = pipeline_run_id
        self.attempt_number = attempt_number
        self.replay_count = replay_count
        self.reattempt_count = reattempt_count
        self.pipeline_run = pipeline_run
        self.logger = logger
        (self._event_recorder): EventRecorder | None = event_recorder
        (self.ctx): ShimContext | None = None
        (self.working_request_json): Any = None
        (self.working_response_json): Any = None
        self.checkpoint = "created"
        self.backend_committed = False
        (self.backend_response): Any = None
        self.started_at = utc_now()
        self._started_perf = time.perf_counter()
        (self.finished_at): str | None = None
        (self.raw_client): Any = None
        (self.raw_backend_response): Any = None

    def _elapsed_ms(self) -> float:
        return (time.perf_counter() - self._started_perf) * 1000

    def _record_attempt_event(
        self,
        event_type: str,
        *,
        level: str = "detailed",
        phase: str | None = None,
        checkpoint: str | None = None,
        elapsed_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if self._event_recorder is None:
            return
        event_details: dict[str, Any] = {"backend_committed": self.backend_committed}
        if details is not None:
            event_details.update(details)
        try:
            self._event_recorder.record(
                event_type,
                level=level,
                request_id=self.request_id,
                pipeline_run_id=self.pipeline_run_id,
                attempt_number=self.attempt_number,
                runtime_generation=self.runtime.generation,
                runtime_fingerprint=self.runtime.runtime_fingerprint,
                phase=phase,
                checkpoint=checkpoint or self.checkpoint,
                elapsed_ms=elapsed_ms,
                details=event_details,
            )
        except Exception:
            return

    def _create_context(self) -> ShimContext:
        ctx = ShimContext(request=self.request)
        self.ctx = ctx
        ctx.metadata["request_id"] = self.request_id
        ctx.metadata["pipeline_run_id"] = self.pipeline_run_id
        ctx.metadata["attempt_number"] = self.attempt_number
        ctx.metadata["runtime_generation"] = self.runtime.generation
        ctx.metadata["backend_url"] = self.runtime.backend_url
        ctx.metadata["replay_count"] = self.replay_count
        ctx.metadata["reattempt_count"] = self.reattempt_count
        return ctx

    def _parse_ingress_json(self) -> Any:
        try:
            return json.loads(self.ingress_body)
        except ValueError as error:
            raise BadRequestError("Request body must be valid JSON") from error

    async def _execute_upstream_pipeline(self, ctx: ShimContext) -> None:
        self.checkpoint = "before_upstream_pipeline"
        await self._maybe_replay_pre_backend(checkpoint=self.checkpoint)
        for plugin_runtime in self.runtime.upstream_plugins:
            plugin_name = plugin_runtime.name
            self.checkpoint = "before_upstream_plugin"
            await self._maybe_replay_pre_backend(
                checkpoint=self.checkpoint, plugin_name=plugin_name
            )
            self.working_request_json = await run_plugin_chain_async(
                payload=self.working_request_json,
                plugins=(plugin_runtime,),
                hook_name="on_request",
                phase="request",
                ctx=ctx,
            )
            self.checkpoint = "after_upstream_plugin"
            await self._maybe_replay_pre_backend(
                checkpoint=self.checkpoint, plugin_name=plugin_name
            )
        self.checkpoint = "upstream_done"

    def ingress_requests_streaming(self) -> bool:
        try:
            request_json = json.loads(self.ingress_body)
        except ValueError:
            return False
        if not isinstance(request_json, dict):
            return False
        return request_json.get("stream") is True

    def _mark_backend_committed(self) -> None:
        self.backend_committed = True
        self.checkpoint = "backend_committed"

    async def _forward_request_with_backend_retry(self) -> dict[str, Any]:
        policy = self.runtime.pipeline_policy
        max_retries = policy.max_retries_per_request
        while True:
            try:
                return await asyncio.to_thread(
                    forward_request, self.working_request_json, self.runtime.backend_url
                )
            except BackendRequestError:
                if self.pipeline_run.retry_count >= max_retries:
                    self._record_attempt_event(
                        "pipeline_backend_retry_limit_reached",
                        level="detailed",
                        phase="plugin",
                        checkpoint=self.checkpoint,
                        details={
                            "reason": "max_retries_reached",
                            "retry_count": self.pipeline_run.retry_count,
                            "max_retries": max_retries,
                            "backend_committed": self.backend_committed,
                        },
                    )
                    raise
                self.pipeline_run.retry_count += 1
                self._record_attempt_event(
                    "pipeline_backend_retry_scheduled",
                    level="detailed",
                    phase="plugin",
                    checkpoint=self.checkpoint,
                    details={
                        "reason": "backend_request_error",
                        "retry_count": self.pipeline_run.retry_count,
                        "max_retries": max_retries,
                        "backend_committed": self.backend_committed,
                    },
                )

    async def _maybe_replay_pre_backend(
        self, *, checkpoint: str, plugin_name: str | None = None
    ) -> None:
        policy = self.runtime.pipeline_policy
        if policy.in_flight_policy != "pre_backend_replay":
            return
        if self.backend_committed:
            return
        if self.replay_count >= policy.max_replays_per_request:
            self._record_attempt_event(
                "pipeline_pre_backend_replay_limit_reached",
                level="detailed",
                phase="plugin",
                checkpoint=checkpoint,
                details={
                    "reason": "max_replays_reached",
                    "replay_count": self.replay_count,
                    "max_replays": policy.max_replays_per_request,
                    "plugin_name": plugin_name,
                },
            )
            return
        gate_open = await self.runtime_manager.runtime_gate_open()
        if gate_open:
            return
        self._record_attempt_event(
            "pipeline_pre_backend_replay_requested",
            level="detailed",
            phase="plugin",
            checkpoint=checkpoint,
            details={
                "reason": "runtime_gate_closed",
                "replay_count": self.replay_count,
                "max_replays": policy.max_replays_per_request,
                "plugin_name": plugin_name,
            },
        )
        raise PreBackendReplayRequested(
            checkpoint=checkpoint,
            old_runtime_generation=self.runtime.generation,
            replay_count=self.replay_count,
            max_replays=policy.max_replays_per_request,
            plugin_name=plugin_name,
        )

    async def _maybe_reattempt_post_backend_generation(
        self, *, checkpoint: str, plugin_name: str | None = None
    ) -> None:
        policy = self.runtime.pipeline_policy
        if policy.in_flight_policy != "post_backend_generation_reattempt":
            return
        if not self.backend_committed:
            return
        if self.reattempt_count >= policy.max_reattempts_per_request:
            self._record_attempt_event(
                "pipeline_post_backend_generation_reattempt_limit_reached",
                level="detailed",
                phase="plugin",
                checkpoint=checkpoint,
                details={
                    "reason": "max_reattempts_reached",
                    "reattempt_count": self.reattempt_count,
                    "max_reattempts": policy.max_reattempts_per_request,
                    "plugin_name": plugin_name,
                },
            )
            return
        gate_open = await self.runtime_manager.runtime_gate_open()
        if gate_open:
            return
        self._record_attempt_event(
            "pipeline_post_backend_generation_reattempt_requested",
            level="detailed",
            phase="plugin",
            checkpoint=checkpoint,
            details={
                "reason": "runtime_gate_closed",
                "reattempt_count": self.reattempt_count,
                "max_reattempts": policy.max_reattempts_per_request,
                "plugin_name": plugin_name,
            },
        )
        raise PostBackendGenerationReattemptRequested(
            checkpoint=checkpoint,
            old_runtime_generation=self.runtime.generation,
            reattempt_count=self.reattempt_count,
            max_reattempts=policy.max_reattempts_per_request,
            plugin_name=plugin_name,
        )

    async def _execute_downstream_pipeline(self, ctx: ShimContext) -> Any:
        self.working_response_json = self.backend_response
        self.checkpoint = "before_downstream_pipeline"
        await self._maybe_reattempt_post_backend_generation(checkpoint=self.checkpoint)
        for plugin_runtime in self.runtime.downstream_plugins:
            plugin_name = plugin_runtime.name
            self.checkpoint = "before_downstream_plugin"
            await self._maybe_reattempt_post_backend_generation(
                checkpoint=self.checkpoint, plugin_name=plugin_name
            )
            self.working_response_json = await run_plugin_chain_async(
                payload=self.working_response_json,
                plugins=(plugin_runtime,),
                hook_name="on_response",
                phase="response",
                ctx=ctx,
            )
            self.checkpoint = "after_downstream_plugin"
            await self._maybe_reattempt_post_backend_generation(
                checkpoint=self.checkpoint, plugin_name=plugin_name
            )
        self.checkpoint = "before_response_return"
        await self._maybe_reattempt_post_backend_generation(checkpoint=self.checkpoint)
        return self.working_response_json

    async def execute_plugin_pipeline(self):
        self.logger.info(
            "Executing plugin pipeline: request_id=%s pipeline_run_id=%s attempt_number=%s runtime_generation=%s",
            self.request_id,
            self.pipeline_run_id,
            self.attempt_number,
            self.runtime.generation,
        )
        self._record_attempt_event("pipeline_attempt_started", phase="plugin")
        attempt_status = "succeeded"
        ctx = self._create_context()
        try:
            self.working_request_json = self._parse_ingress_json()
            self.checkpoint = "parsed"
            await self._execute_upstream_pipeline(ctx)
            self.checkpoint = "before_backend"
            await self._maybe_replay_pre_backend(checkpoint=self.checkpoint)
            self._mark_backend_committed()
            self.backend_response = await self._forward_request_with_backend_retry()
            ctx.set_backend_response(self.backend_response)
            self.checkpoint = "backend_done"
            await self._maybe_reattempt_post_backend_generation(
                checkpoint=self.checkpoint
            )
            response = await self._execute_downstream_pipeline(ctx)
            self.checkpoint = "downstream_done"
            log_plugin_summary(self.logger, ctx)
            return response
        except PreBackendReplayRequested:
            attempt_status = "replay_requested"
            log_plugin_summary(self.logger, ctx)
            raise
        except PostBackendGenerationReattemptRequested:
            attempt_status = "generation_reattempt_requested"
            log_plugin_summary(self.logger, ctx)
            raise
        except PluginExecutionError as error:
            attempt_status = "error"
            log_plugin_summary(self.logger, ctx)
            self.logger.error(
                "Plugin execution aborted request: plugin=%s phase=%s error=%s",
                error.plugin_name,
                error.phase,
                error.original_error,
            )
            return JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "type": "plugin_execution_error",
                        "plugin": error.plugin_name,
                        "phase": error.phase,
                        "message": str(error.original_error),
                    }
                },
            )
        except LocalShimError as error:
            attempt_status = "error"
            log_plugin_summary(self.logger, ctx)
            self.logger.error(
                "LocalShim error: type=%s message=%s details=%s",
                error.error_type,
                error.message,
                error.details,
            )
            self._record_attempt_event(
                "localshim_error",
                level="basic",
                phase="plugin",
                checkpoint=self.checkpoint,
                details={"error_type": error.error_type},
            )
            return error_response(error)
        except Exception as error:
            attempt_status = "error"
            log_plugin_summary(self.logger, ctx)
            internal_error = InternalServerError("Unexpected internal error")
            self.logger.exception("Unexpected internal error: %s", error)
            self._record_attempt_event(
                "localshim_error",
                level="basic",
                phase="plugin",
                checkpoint=self.checkpoint,
                details={"error_type": internal_error.error_type},
            )
            return error_response(internal_error)
        finally:
            self.finished_at = utc_now()
            self._record_attempt_event(
                "pipeline_attempt_completed",
                phase="plugin",
                checkpoint=self.checkpoint,
                elapsed_ms=self._elapsed_ms(),
                details={"final_status": attempt_status},
            )

    async def open_streaming_plugin_passthrough(self):
        self.logger.info(
            "Executing streaming plugin passthrough: request_id=%s pipeline_run_id=%s attempt_number=%s runtime_generation=%s",
            self.request_id,
            self.pipeline_run_id,
            self.attempt_number,
            self.runtime.generation,
        )
        self._record_attempt_event("pipeline_attempt_started", phase="streaming")
        attempt_status = "succeeded"
        ctx = self._create_context()
        ctx.metadata["streaming"] = True
        try:
            self.working_request_json = self._parse_ingress_json()
            self.checkpoint = "parsed"
            if self.runtime.downstream_plugins:
                raise StreamingDownstreamPluginsUnsupportedError()
            await self._execute_upstream_pipeline(ctx)
            self.checkpoint = "before_backend"
            await self._maybe_replay_pre_backend(checkpoint=self.checkpoint)
            self._mark_backend_committed()
            (
                self.raw_client,
                self.raw_backend_response,
            ) = await forward_streaming_json_request(
                self.request, self.runtime.backend_url, self.working_request_json
            )
            self.checkpoint = "backend_streaming"
            log_plugin_summary(self.logger, ctx)
            return self.raw_client, self.raw_backend_response
        except PreBackendReplayRequested:
            attempt_status = "replay_requested"
            log_plugin_summary(self.logger, ctx)
            raise
        except PluginExecutionError as error:
            attempt_status = "error"
            log_plugin_summary(self.logger, ctx)
            self.logger.error(
                "Plugin execution aborted streaming request: plugin=%s phase=%s error=%s",
                error.plugin_name,
                error.phase,
                error.original_error,
            )
            return JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "type": "plugin_execution_error",
                        "plugin": error.plugin_name,
                        "phase": error.phase,
                        "message": str(error.original_error),
                    }
                },
            )
        except LocalShimError as error:
            attempt_status = "error"
            log_plugin_summary(self.logger, ctx)
            self.logger.error(
                "LocalShim streaming error: type=%s message=%s details=%s",
                error.error_type,
                error.message,
                error.details,
            )
            self._record_attempt_event(
                "localshim_error",
                level="basic",
                phase="streaming",
                checkpoint=self.checkpoint,
                details={"error_type": error.error_type},
            )
            return error_response(error)
        except Exception as error:
            attempt_status = "error"
            log_plugin_summary(self.logger, ctx)
            internal_error = InternalServerError("Unexpected internal error")
            self.logger.exception("Unexpected streaming internal error: %s", error)
            self._record_attempt_event(
                "localshim_error",
                level="basic",
                phase="streaming",
                checkpoint=self.checkpoint,
                details={"error_type": internal_error.error_type},
            )
            return error_response(internal_error)
        finally:
            if self.raw_backend_response is None:
                self.finished_at = utc_now()
                self._record_attempt_event(
                    "pipeline_attempt_completed",
                    phase="streaming",
                    checkpoint=self.checkpoint,
                    elapsed_ms=self._elapsed_ms(),
                    details={"final_status": attempt_status},
                )

    async def open_raw_passthrough(self):
        self.logger.info(
            "Executing raw passthrough: request_id=%s pipeline_run_id=%s attempt_number=%s runtime_generation=%s",
            self.request_id,
            self.pipeline_run_id,
            self.attempt_number,
            self.runtime.generation,
        )
        self._record_attempt_event("pipeline_attempt_started", phase="raw")
        try:
            self._mark_backend_committed()
            self.raw_client, self.raw_backend_response = await forward_raw_body(
                self.request, self.runtime.backend_url, self.ingress_body
            )
            self.checkpoint = "backend_streaming"
            return self.raw_client, self.raw_backend_response
        except Exception:
            self.finished_at = utc_now()
            self._record_attempt_event(
                "pipeline_attempt_completed",
                phase="raw",
                checkpoint=self.checkpoint,
                elapsed_ms=self._elapsed_ms(),
                details={"final_status": "failed"},
            )
            raise

    async def cleanup_raw_passthrough(self, *, phase: str = "raw") -> None:
        cleanup_status = "succeeded"
        try:
            try:
                if self.raw_backend_response is not None:
                    await self.raw_backend_response.aclose()
            except Exception:
                cleanup_status = "failed"
                raise
            finally:
                if self.raw_client is not None:
                    await self.raw_client.aclose()
        except Exception:
            cleanup_status = "failed"
            raise
        finally:
            self.checkpoint = "finished"
            self.finished_at = utc_now()
            self._record_attempt_event(
                "pipeline_attempt_completed",
                phase=phase,
                checkpoint=self.checkpoint,
                elapsed_ms=self._elapsed_ms(),
                details={"final_status": cleanup_status},
            )
