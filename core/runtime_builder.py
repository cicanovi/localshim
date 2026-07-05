from __future__ import annotations
import copy
from typing import Any, cast
from core.errors import RuntimeBuildError
from core.fingerprint import compute_runtime_fingerprint
from core.plugins import load_plugins
from core.policy import (
    DEFAULT_IN_FLIGHT_POLICY,
    DEFAULT_MAX_REPLAYS_PER_REQUEST,
    DEFAULT_MAX_RETRIES_PER_REQUEST,
    DEFAULT_MAX_REATTEMPTS_PER_REQUEST,
    SUPPORTED_IN_FLIGHT_POLICIES,
    InFlightPolicy,
    PipelinePolicy,
)
from core.runtime import ShimRuntime

DEFAULT_CONTROL_ENABLED = True
DEFAULT_CONFIG_APPLY_MODE = "late_gate"
DEFAULT_GATE_POLICY = "wait"
DEFAULT_GATE_TIMEOUT_SECONDS = 1e1
DEFAULT_QUEUE_POLICY = "latest_wins"
DEFAULT_EVENTS_ENABLED = True
DEFAULT_EVENTS_LEVEL = "basic"
DEFAULT_MAX_EVENTS = 500


def build_runtime(
    config_path: str, config: dict[str, Any], generation: int
) -> ShimRuntime:
    try:
        if not isinstance(config, dict):
            raise RuntimeBuildError("Runtime config must be an object/dict")
        config_snapshot = copy.deepcopy(config)
        validate_and_normalize_runtime_config(config_snapshot)
        backend_url = get_backend_url(config_snapshot)
        plugin_config = get_plugin_config(config_snapshot)
        pipeline_policy = get_pipeline_policy(config_snapshot)
        runtime_fingerprint = compute_runtime_fingerprint(config_snapshot)
        upstream_specs = plugin_config["upstream"]
        downstream_specs = plugin_config["downstream"]
        upstream_plugins = tuple(load_plugins(upstream_specs))
        downstream_plugins = tuple(load_plugins(downstream_specs))
        return ShimRuntime(
            generation=generation,
            config_path=config_path,
            config=config_snapshot,
            backend_url=backend_url,
            runtime_fingerprint=runtime_fingerprint,
            upstream_plugins=upstream_plugins,
            downstream_plugins=downstream_plugins,
            pipeline_policy=pipeline_policy,
        )
    except RuntimeBuildError:
        raise
    except Exception as error:
        raise RuntimeBuildError(f"Failed to build runtime: {error}") from error


def create_initial_runtime(
    *, config_path: str, config: dict[str, Any], generation: int = 1
) -> ShimRuntime:
    return build_runtime(config_path=config_path, config=config, generation=generation)


def validate_and_normalize_runtime_config(config: Any) -> None:
    if not isinstance(config, dict):
        raise RuntimeBuildError("Runtime config must be an object/dict")
    validate_and_normalize_backend(config)
    validate_and_normalize_plugins(config)
    validate_and_normalize_control(config)
    validate_and_normalize_pipeline(config)
    validate_and_normalize_observability(config)


def validate_and_normalize_backend(config: dict[str, Any]) -> None:
    backend = config.get("backend")
    if not isinstance(backend, dict):
        raise RuntimeBuildError("Runtime config backend must be an object/dict")
    backend_url = backend.get("url")
    if not isinstance(backend_url, str) or not backend_url.strip():
        raise RuntimeBuildError("Runtime config backend.url must be a non-empty string")
    backend["url"] = backend_url.strip()


def validate_and_normalize_plugins(config: dict[str, Any]) -> None:
    plugins = config.setdefault("plugins", {})
    if not isinstance(plugins, dict):
        raise RuntimeBuildError("Runtime config plugins must be an object/dict")
    upstream = plugins.setdefault("upstream", [])
    downstream = plugins.setdefault("downstream", [])
    if not isinstance(upstream, list):
        raise RuntimeBuildError("Runtime config plugins.upstream must be a list")
    if not isinstance(downstream, list):
        raise RuntimeBuildError("Runtime config plugins.downstream must be a list")


def validate_and_normalize_control(config: dict[str, Any]) -> None:
    control = config.setdefault("control", {})
    if not isinstance(control, dict):
        raise RuntimeBuildError("Runtime config control must be an object/dict")
    enabled = control.setdefault("enabled", DEFAULT_CONTROL_ENABLED)
    if not isinstance(enabled, bool):
        raise RuntimeBuildError("Runtime config control.enabled must be a boolean")
    config_apply = control.setdefault("config_apply", {})
    if not isinstance(config_apply, dict):
        raise RuntimeBuildError(
            "Runtime config control.config_apply must be an object/dict"
        )
    mode = config_apply.setdefault("mode", DEFAULT_CONFIG_APPLY_MODE)
    if mode != "late_gate":
        raise RuntimeBuildError(
            "Runtime config control.config_apply.mode must be 'late_gate' for now"
        )
    gate_policy = config_apply.setdefault("gate_policy", DEFAULT_GATE_POLICY)
    if gate_policy != "wait":
        raise RuntimeBuildError(
            "Runtime config control.config_apply.gate_policy must be 'wait' for now"
        )
    gate_timeout_seconds = config_apply.setdefault(
        "gate_timeout_seconds", DEFAULT_GATE_TIMEOUT_SECONDS
    )
    if not is_positive_number(gate_timeout_seconds):
        raise RuntimeBuildError(
            "Runtime config control.config_apply.gate_timeout_seconds must be a positive number"
        )
    queue_policy = config_apply.setdefault("queue_policy", DEFAULT_QUEUE_POLICY)
    if queue_policy != "latest_wins":
        raise RuntimeBuildError(
            "Runtime config control.config_apply.queue_policy must be 'latest_wins' for now"
        )


def validate_and_normalize_pipeline(config: dict[str, Any]) -> None:
    pipeline = config.setdefault("pipeline", {})
    if not isinstance(pipeline, dict):
        raise RuntimeBuildError("Runtime config pipeline must be an object/dict")
    in_flight_policy = pipeline.setdefault("in_flight_policy", DEFAULT_IN_FLIGHT_POLICY)
    if not isinstance(in_flight_policy, str):
        raise RuntimeBuildError(
            "Runtime config pipeline.in_flight_policy must be a string"
        )
    if in_flight_policy not in SUPPORTED_IN_FLIGHT_POLICIES:
        supported = ", ".join(SUPPORTED_IN_FLIGHT_POLICIES)
        raise RuntimeBuildError(
            f"Runtime config pipeline.in_flight_policy must be one of: {supported}"
        )
    max_replays = pipeline.setdefault(
        "max_replays_per_request", DEFAULT_MAX_REPLAYS_PER_REQUEST
    )
    if not is_non_negative_int(max_replays):
        raise RuntimeBuildError(
            "Runtime config pipeline.max_replays_per_request must be a non-negative integer"
        )
    max_retries = pipeline.setdefault(
        "max_retries_per_request", DEFAULT_MAX_RETRIES_PER_REQUEST
    )
    if not is_non_negative_int(max_retries):
        raise RuntimeBuildError(
            "Runtime config pipeline.max_retries_per_request must be a non-negative integer"
        )
    max_reattempts = pipeline.setdefault(
        "max_reattempts_per_request", DEFAULT_MAX_REATTEMPTS_PER_REQUEST
    )
    if not is_non_negative_int(max_reattempts):
        raise RuntimeBuildError(
            "Runtime config pipeline.max_reattempts_per_request must be a non-negative integer"
        )


def validate_and_normalize_observability(config: dict[str, Any]) -> None:
    observability = config.setdefault("observability", {})
    if not isinstance(observability, dict):
        raise RuntimeBuildError("Runtime config observability must be an object/dict")
    events = observability.setdefault("events", {})
    if not isinstance(events, dict):
        raise RuntimeBuildError(
            "Runtime config observability.events must be an object/dict"
        )
    enabled = events.setdefault("enabled", DEFAULT_EVENTS_ENABLED)
    if not isinstance(enabled, bool):
        raise RuntimeBuildError(
            "Runtime config observability.events.enabled must be a boolean"
        )
    level = events.setdefault("level", DEFAULT_EVENTS_LEVEL)
    if level not in {"basic", "detailed"}:
        raise RuntimeBuildError(
            "Runtime config observability.events.level must be 'basic' or 'detailed'"
        )
    max_events = events.setdefault("max_events", DEFAULT_MAX_EVENTS)
    if not is_positive_int(max_events):
        raise RuntimeBuildError(
            "Runtime config observability.events.max_events must be a positive integer"
        )


def get_backend_url(config: dict[str, Any]) -> str:
    return config["backend"]["url"]


def get_pipeline_policy(config: dict[str, Any]) -> PipelinePolicy:
    pipeline = config["pipeline"]
    return PipelinePolicy(
        in_flight_policy=cast(InFlightPolicy, pipeline["in_flight_policy"]),
        max_replays_per_request=pipeline["max_replays_per_request"],
        max_retries_per_request=pipeline["max_retries_per_request"],
        max_reattempts_per_request=pipeline["max_reattempts_per_request"],
    )


def get_plugin_config(config: dict[str, Any]) -> dict[str, Any]:
    return config["plugins"]


def is_positive_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return value > 0


def is_positive_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, int):
        return False
    return value > 0


def is_non_negative_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, int):
        return False
    return value >= 0
