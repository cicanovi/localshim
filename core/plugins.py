import copy
import importlib.util
from pathlib import Path
from core.logging import get_logger

logger = get_logger("localshim.plugins")


class ShimPlugin:
    def __init__(self, **params):
        self.params = params

    def on_request(self, req, ctx=None):
        return req

    def on_response(self, res, ctx=None):
        return res


class PluginRuntime:
    def __init__(
        self,
        name,
        plugin,
        source=None,
        entrypoint=None,
        fail_mode="abort",
        timeout_seconds=None,
        params=None,
    ):
        self.name = name
        self.plugin = plugin
        self.source = source
        self.entrypoint = entrypoint
        self.fail_mode = fail_mode
        self.timeout_seconds = timeout_seconds
        self.params = params or {}


def load_plugin_from_spec(spec):
    if not isinstance(spec, dict):
        raise TypeError("Plugin spec must be an object/dict")
    if not spec.get("enabled", True):
        plugin_name = spec.get("name", "unnamed_plugin")
        logger.info("Skipping disabled plugin: name=%s", plugin_name)
        return None
    plugin_name = spec.get("name", "unnamed_plugin")
    source = spec.get("source")
    if not isinstance(source, str) or not source:
        raise TypeError(f"Plugin '{plugin_name}' source must be a non-empty string")
    entrypoint = spec.get("entrypoint")
    if not isinstance(entrypoint, str) or not entrypoint:
        raise TypeError(f"Plugin '{plugin_name}' entrypoint must be a non-empty string")
    fail_mode = spec.get("fail_mode", "abort")
    timeout_seconds = spec.get("timeout_seconds", None)
    raw_params = spec["params"] if "params" in spec else {}
    if not isinstance(raw_params, dict):
        raise TypeError(f"Plugin '{plugin_name}' params must be an object/dict")
    params = copy.deepcopy(raw_params)
    if fail_mode not in {"abort", "continue"}:
        raise ValueError(f"Plugin '{plugin_name}' has invalid fail_mode: {fail_mode}")
    if timeout_seconds is not None:
        if not isinstance(timeout_seconds, (int, float)):
            raise TypeError(
                f"Plugin '{plugin_name}' timeout_seconds must be a number or null"
            )
        if timeout_seconds <= 0:
            raise ValueError(
                f"Plugin '{plugin_name}' timeout_seconds must be positive or null"
            )
    source_path = Path(source)
    module_name = f"localshim_plugin_{source_path.stem}_{entrypoint}"
    module_spec = importlib.util.spec_from_file_location(module_name, source_path)
    if module_spec is None or module_spec.loader is None:
        raise ImportError(f"Could not load plugin '{plugin_name}' from {source}")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    plugin_class = getattr(module, entrypoint, None)
    if plugin_class is None:
        raise AttributeError(
            f"Plugin '{plugin_name}' entrypoint '{entrypoint}' not found in {source}"
        )
    if not isinstance(plugin_class, type):
        raise TypeError(
            f"Plugin '{plugin_name}' entrypoint '{entrypoint}' must be a class for now"
        )
    if not issubclass(plugin_class, ShimPlugin):
        raise TypeError(
            f"Plugin '{plugin_name}' entrypoint '{entrypoint}' must inherit from ShimPlugin"
        )
    try:
        plugin = plugin_class(**params)
    except TypeError as error:
        raise TypeError(
            f"Invalid params for plugin '{plugin_name}': {error}"
        ) from error
    runtime = PluginRuntime(
        name=plugin_name,
        plugin=plugin,
        source=source,
        entrypoint=entrypoint,
        fail_mode=fail_mode,
        timeout_seconds=timeout_seconds,
        params=params,
    )
    logger.info(
        "Loaded plugin: name=%s source=%s entrypoint=%s fail_mode=%s timeout_seconds=%s",
        plugin_name,
        source,
        entrypoint,
        fail_mode,
        timeout_seconds,
    )
    return runtime


def load_plugins(plugin_specs):
    plugins = []
    for spec in plugin_specs:
        plugin = load_plugin_from_spec(spec)
        if plugin is not None:
            plugins.append(plugin)
    return plugins
