from __future__ import annotations
import copy
import threading
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Literal, cast

EventLevel = Literal["basic", "detailed", "debug"]
_LEVEL_RANK: dict[EventLevel, int] = {"basic": 0, "detailed": 1, "debug": 2}
_UNSAFE_DETAIL_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "body",
    "full_request",
    "full_response",
    "header",
    "headers",
    "message",
    "messages",
    "password",
    "prompt",
    "prompts",
    "request",
    "request_body",
    "response",
    "response_body",
    "secret",
    "token",
}
_UNSAFE_DETAIL_KEY_FRAGMENTS = (
    "authorization",
    "api_key",
    "apikey",
    "access_token",
    "bearer",
    "body",
    "cookie",
    "header",
    "message",
    "password",
    "prompt",
    "request",
    "response",
    "secret",
    "token",
)
_MAX_DETAIL_DEPTH = 3
_MAX_DETAIL_ITEMS = 20
_MAX_DETAIL_STRING_LENGTH = 256


@dataclass(frozen=True)
class EventRecord:
    id: int
    timestamp: datetime
    event_type: str
    level: EventLevel
    request_id: str | None = None
    pipeline_run_id: str | None = None
    attempt_number: int | None = None
    runtime_generation: int | None = None
    runtime_fingerprint: str | None = None
    apply_id: int | None = None
    phase: str | None = None
    plugin: str | None = None
    checkpoint: str | None = None
    elapsed_ms: float | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class EventRecorderStats:
    enabled: bool
    level: EventLevel
    max_events: int
    retained_events: int
    total_recorded: int
    evicted_events: int
    dropped_events: int


class EventRecorder:
    def __init__(
        self, *, max_events: int = 500, enabled: bool = True, level: str = "basic"
    ) -> None:
        if max_events < 1:
            raise ValueError("max_events must be at least 1")
        validated_level = self._validate_level(level)
        (self._max_events): int = max_events
        (self._enabled): bool = enabled
        (self._level): EventLevel = validated_level
        (self._events): deque[EventRecord] = deque(maxlen=max_events)
        (self._next_event_id): int = 1
        (self._total_recorded): int = 0
        (self._evicted_events): int = 0
        (self._dropped_events): int = 0
        self._lock = threading.Lock()

    def record(
        self,
        event_type: str,
        *,
        level: str = "basic",
        request_id: str | None = None,
        pipeline_run_id: str | None = None,
        attempt_number: int | None = None,
        runtime_generation: int | None = None,
        runtime_fingerprint: str | None = None,
        apply_id: int | None = None,
        phase: str | None = None,
        plugin: str | None = None,
        checkpoint: str | None = None,
        elapsed_ms: float | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> EventRecord | None:
        acquired = False
        try:
            validated_level = self._validate_level(level)
            if not self._enabled:
                return None
            if not self._should_record_level(validated_level):
                return None
            acquired = self._lock.acquire(blocking=False)
            if not acquired:
                self._dropped_events += 1
                return None
            if not self._enabled:
                return None
            if not self._should_record_level(validated_level):
                return None
            if len(self._events) == self._max_events:
                self._evicted_events += 1
            event = EventRecord(
                id=self._next_event_id,
                timestamp=datetime.now(timezone.utc),
                event_type=str(event_type),
                level=validated_level,
                request_id=request_id,
                pipeline_run_id=pipeline_run_id,
                attempt_number=attempt_number,
                runtime_generation=runtime_generation,
                runtime_fingerprint=runtime_fingerprint,
                apply_id=apply_id,
                phase=phase,
                plugin=plugin,
                checkpoint=checkpoint,
                elapsed_ms=elapsed_ms,
                details=self._sanitize_details(details),
            )
            self._events.append(event)
            self._next_event_id += 1
            self._total_recorded += 1
            return self._copy_record(event)
        except Exception:
            try:
                self._dropped_events += 1
            except Exception:
                pass
            return None
        finally:
            if acquired:
                self._lock.release()

    def list_events(
        self,
        *,
        limit: int | None = None,
        event_type: str | None = None,
        since_id: int | None = None,
        level: str | None = None,
    ) -> list[EventRecord]:
        validated_level = self._validate_level(level) if level is not None else None
        with self._lock:
            events = list(self._events)
        if since_id is not None:
            events = [event for event in events if event.id > since_id]
        if event_type is not None:
            events = [event for event in events if event.event_type == event_type]
        if validated_level is not None:
            events = [event for event in events if event.level == validated_level]
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be non-negative")
            if limit == 0:
                events = []
            else:
                events = events[-limit:]
        return [self._copy_record(event) for event in events]

    def clear(self) -> None:
        with self._lock:
            self._events.clear()

    def get_stats(self) -> EventRecorderStats:
        with self._lock:
            return EventRecorderStats(
                enabled=self._enabled,
                level=self._level,
                max_events=self._max_events,
                retained_events=len(self._events),
                total_recorded=self._total_recorded,
                evicted_events=self._evicted_events,
                dropped_events=self._dropped_events,
            )

    def set_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._enabled = enabled

    def set_level(self, level: str) -> None:
        validated_level = self._validate_level(level)
        with self._lock:
            self._level = validated_level

    @property
    def enabled(self) -> bool:
        with self._lock:
            return self._enabled

    @property
    def level(self) -> EventLevel:
        with self._lock:
            return self._level

    @property
    def max_events(self) -> int:
        return self._max_events

    def _should_record_level(self, event_level: EventLevel) -> bool:
        return _LEVEL_RANK[event_level] <= _LEVEL_RANK[self._level]

    def _sanitize_details(
        self, details: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        if details is None:
            return None
        sanitized = self._sanitize_mapping(details, depth=0)
        if not sanitized:
            return None
        return sanitized

    def _sanitize_mapping(
        self, mapping: Mapping[str, Any], *, depth: int
    ) -> dict[str, Any]:
        if depth >= _MAX_DETAIL_DEPTH:
            return {"_truncated": "max_depth"}
        sanitized: dict[str, Any] = {}
        for index, (key, value) in enumerate(mapping.items()):
            if index >= _MAX_DETAIL_ITEMS:
                sanitized["_truncated_items"] = len(mapping) - _MAX_DETAIL_ITEMS
                break
            key_text = str(key)
            if self._is_unsafe_detail_key(key_text):
                sanitized[f"{key_text}_omitted"] = True
                continue
            sanitized[key_text] = self._sanitize_value(value, depth=depth + 1)
        return sanitized

    def _sanitize_value(self, value: Any, *, depth: int) -> Any:
        if value is None:
            return None
        if isinstance(value, bool | int | float):
            return value
        if isinstance(value, str):
            return self._truncate_string(value)
        if isinstance(value, bytes | bytearray | memoryview):
            return {"type": "bytes", "length": len(value), "omitted": True}
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, BaseException):
            return {
                "type": value.__class__.__name__,
                "message": self._truncate_string(str(value)),
            }
        if isinstance(value, Mapping):
            return self._sanitize_mapping(value, depth=depth)
        if isinstance(value, Sequence) and not isinstance(value, str):
            return self._sanitize_sequence(value, depth=depth)
        return {
            "type": value.__class__.__name__,
            "repr": self._truncate_string(repr(value)),
        }

    def _sanitize_sequence(self, sequence: Sequence[Any], *, depth: int) -> list[Any]:
        sanitized = [
            self._sanitize_value(item, depth=depth + 1)
            for item in sequence[:_MAX_DETAIL_ITEMS]
        ]
        if len(sequence) > _MAX_DETAIL_ITEMS:
            sanitized.append({"_truncated_items": len(sequence) - _MAX_DETAIL_ITEMS})
        return sanitized

    def _truncate_string(self, value: str) -> str:
        if len(value) <= _MAX_DETAIL_STRING_LENGTH:
            return value
        return value[:_MAX_DETAIL_STRING_LENGTH] + "...<truncated>"

    def _is_unsafe_detail_key(self, key: str) -> bool:
        normalized = key.lower().replace("-", "_").replace(" ", "_")
        if normalized in _UNSAFE_DETAIL_KEYS:
            return True
        return any(fragment in normalized for fragment in _UNSAFE_DETAIL_KEY_FRAGMENTS)

    def _copy_record(self, event: EventRecord) -> EventRecord:
        return replace(event, details=copy.deepcopy(event.details))

    def _validate_level(self, level: str) -> EventLevel:
        if level not in _LEVEL_RANK:
            raise ValueError("level must be one of: basic, detailed, debug")
        return cast(EventLevel, level)
