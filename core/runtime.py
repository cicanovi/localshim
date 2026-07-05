from __future__ import annotations
import asyncio
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from core.errors import RuntimeDrainTimeoutError, RuntimeReconfiguringError
from core.plugins import PluginRuntime
from core.policy import PipelinePolicy

DEFAULT_GATE_TIMEOUT_SECONDS = 1e1


@dataclass(frozen=True)
class ShimRuntime:
    generation: int
    config_path: str
    config: dict[str, Any]
    backend_url: str
    runtime_fingerprint: str = "sha256:unavailable"
    upstream_plugins: tuple[PluginRuntime, ...] = field(default_factory=tuple)
    downstream_plugins: tuple[PluginRuntime, ...] = field(default_factory=tuple)
    pipeline_policy: PipelinePolicy = field(default_factory=PipelinePolicy)
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def plugins_enabled(self) -> bool:
        return bool(self.upstream_plugins or self.downstream_plugins)

    def plugin_summary(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "upstream": [
                summarize_plugin_runtime(plugin) for plugin in self.upstream_plugins
            ],
            "downstream": [
                summarize_plugin_runtime(plugin) for plugin in self.downstream_plugins
            ],
        }


@dataclass
class ActiveRun:
    manager: RuntimeManager
    runtime: ShimRuntime
    released: bool = False

    async def release(self) -> None:
        if self.released:
            return
        self.released = True
        await self.manager.exit_run()

    async def __aenter__(self) -> ActiveRun:
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.release()


class RuntimeManager:
    def __init__(
        self,
        initial_runtime: ShimRuntime,
        *,
        gate_timeout_seconds: float = DEFAULT_GATE_TIMEOUT_SECONDS,
    ):
        self._active_runtime = initial_runtime
        self._active_run_count = 0
        self._runtime_gate_open = True
        self._gate_policy = "wait"
        self._gate_timeout_seconds = gate_timeout_seconds
        self._next_generation = initial_runtime.generation + 1
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def enter_run(
        self, *, gate_timeout_seconds: float | None = None
    ) -> ActiveRun:
        timeout_seconds = self._resolve_gate_timeout(gate_timeout_seconds)
        async with self._condition:
            await self._wait_for_gate_locked(timeout_seconds)
            runtime = self._active_runtime
            self._active_run_count += 1
        return ActiveRun(manager=self, runtime=runtime)

    async def exit_run(self) -> None:
        async with self._condition:
            if self._active_run_count <= 0:
                return
            self._active_run_count -= 1
            self._condition.notify_all()

    async def active_run_count(self) -> int:
        async with self._condition:
            return self._active_run_count

    async def wait_for_drain(self, *, timeout_seconds: float | None = None) -> None:
        async with self._condition:
            if timeout_seconds is None:
                while self._active_run_count > 0:
                    await self._condition.wait()
                return
            if timeout_seconds <= 0:
                if self._active_run_count > 0:
                    raise RuntimeDrainTimeoutError()
                return
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout_seconds
            while self._active_run_count > 0:
                remaining_seconds = deadline - loop.time()
                if remaining_seconds <= 0:
                    raise RuntimeDrainTimeoutError()
                try:
                    await asyncio.wait_for(
                        self._condition.wait(), timeout=remaining_seconds
                    )
                except asyncio.TimeoutError as error:
                    raise RuntimeDrainTimeoutError() from error

    async def get_active_runtime(self) -> ShimRuntime:
        async with self._condition:
            return self._active_runtime

    async def reserve_generation(self) -> int:
        async with self._condition:
            generation = self._next_generation
            self._next_generation += 1
            return generation

    async def swap_runtime(self, runtime: ShimRuntime) -> ShimRuntime:
        async with self._condition:
            if runtime.generation <= self._active_runtime.generation:
                raise ValueError(
                    "Candidate runtime generation must be greater than active runtime generation"
                )
            previous_runtime = self._active_runtime
            self._active_runtime = runtime
            self._condition.notify_all()
            return previous_runtime

    async def close_gate(self) -> None:
        async with self._condition:
            self._runtime_gate_open = False

    async def open_gate(self) -> None:
        async with self._condition:
            self._runtime_gate_open = True
            self._condition.notify_all()

    async def runtime_gate_open(self) -> bool:
        async with self._condition:
            return self._runtime_gate_open

    async def wait_for_gate(self, timeout_seconds: float | None = None) -> None:
        resolved_timeout_seconds = self._resolve_gate_timeout(timeout_seconds)
        async with self._condition:
            await self._wait_for_gate_locked(resolved_timeout_seconds)

    def _resolve_gate_timeout(self, timeout_seconds: float | None) -> float:
        if timeout_seconds is None:
            return self._gate_timeout_seconds
        return timeout_seconds

    async def _wait_for_gate_locked(self, timeout_seconds: float) -> None:
        if self._runtime_gate_open:
            return
        if timeout_seconds <= 0:
            raise RuntimeReconfiguringError()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while not self._runtime_gate_open:
            remaining_seconds = deadline - loop.time()
            if remaining_seconds <= 0:
                raise RuntimeReconfiguringError()
            try:
                await asyncio.wait_for(
                    self._condition.wait(), timeout=remaining_seconds
                )
            except asyncio.TimeoutError as error:
                raise RuntimeReconfiguringError() from error


def summarize_plugin_runtime(runtime: PluginRuntime) -> dict[str, Any]:
    return {
        "name": runtime.name,
        "source": runtime.source,
        "entrypoint": runtime.entrypoint,
        "fail_mode": runtime.fail_mode,
        "timeout_seconds": runtime.timeout_seconds,
        "params": copy.deepcopy(runtime.params),
    }
