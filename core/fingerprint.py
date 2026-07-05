from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

RUNTIME_FINGERPRINT_SCHEMA = "localshim.runtime_fingerprint.v1"


def compute_runtime_fingerprint(config: dict[str, Any]) -> str:
    payload = {
        "schema": RUNTIME_FINGERPRINT_SCHEMA,
        "config": copy.deepcopy(config),
        "plugin_sources": collect_plugin_source_digests(config),
    }
    canonical_payload = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def collect_plugin_source_digests(
    config: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    plugins = config.get("plugins", {})
    if not isinstance(plugins, dict):
        return {"upstream": [], "downstream": []}
    return {
        "upstream": collect_group_source_digests(plugins.get("upstream", [])),
        "downstream": collect_group_source_digests(plugins.get("downstream", [])),
    }


def collect_group_source_digests(specs: Any) -> list[dict[str, Any]]:
    if not isinstance(specs, list):
        return []
    source_digests: list[dict[str, Any]] = []
    for index, spec in enumerate(specs):
        if not isinstance(spec, dict):
            continue
        if not spec.get("enabled", True):
            continue
        source = spec.get("source")
        entrypoint = spec.get("entrypoint")
        name = spec.get("name", f"plugin_{index}")
        if not isinstance(source, str) or not source:
            raise TypeError(f"Plugin '{name}' source must be a non-empty string")
        if not isinstance(entrypoint, str) or not entrypoint:
            raise TypeError(f"Plugin '{name}' entrypoint must be a non-empty string")
        source_digests.append(
            {
                "index": index,
                "name": name,
                "source": source,
                "entrypoint": entrypoint,
                "source_sha256": sha256_file(source),
            }
        )
    return source_digests


def sha256_file(path_text: str) -> str:
    path = Path(path_text)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
