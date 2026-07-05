from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

InFlightPolicy = Literal[
    "drain", "pre_backend_replay", "post_backend_generation_reattempt"
]
SUPPORTED_IN_FLIGHT_POLICIES: tuple[InFlightPolicy, ...] = (
    "drain",
    "pre_backend_replay",
    "post_backend_generation_reattempt",
)
DEFAULT_IN_FLIGHT_POLICY: InFlightPolicy = "drain"
DEFAULT_MAX_REPLAYS_PER_REQUEST = 0
DEFAULT_MAX_RETRIES_PER_REQUEST = 0
DEFAULT_MAX_REATTEMPTS_PER_REQUEST = 0


@dataclass(frozen=True)
class PipelinePolicy:
    in_flight_policy: InFlightPolicy = DEFAULT_IN_FLIGHT_POLICY
    max_replays_per_request: int = DEFAULT_MAX_REPLAYS_PER_REQUEST
    max_retries_per_request: int = DEFAULT_MAX_RETRIES_PER_REQUEST
    max_reattempts_per_request: int = DEFAULT_MAX_REATTEMPTS_PER_REQUEST
