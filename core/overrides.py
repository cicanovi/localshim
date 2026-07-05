from __future__ import annotations
import copy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AppOverrides:
    host: str | None = None
    port: int | None = None
    backend_url: str | None = None


def apply_app_overrides(
    config: dict[str, Any], overrides: AppOverrides | None = None
) -> dict[str, Any]:
    updated = copy.deepcopy(config)
    if overrides is None:
        return updated
    if overrides.host is not None or overrides.port is not None:
        server = _ensure_mapping_section(updated, "server")
        if overrides.host is not None:
            server["host"] = overrides.host
        if overrides.port is not None:
            server["port"] = overrides.port
    if overrides.backend_url is not None:
        backend = _ensure_mapping_section(updated, "backend")
        backend["url"] = overrides.backend_url
    return updated


def _ensure_mapping_section(
    config: dict[str, Any], section_name: str
) -> dict[str, Any]:
    section = config.setdefault(section_name, {})
    if not isinstance(section, dict):
        raise TypeError(f"Config section '{section_name}' must be an object/dict")
    return section
