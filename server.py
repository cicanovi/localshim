from __future__ import annotations
from core.app import create_app, create_event_recorder_from_config, log_runtime_startup

app = create_app()
CONFIG_PATH = app.state.config_path
INITIAL_RUNTIME = app.state.initial_runtime
RUNTIME_MANAGER = app.state.runtime_manager
EVENT_RECORDER = app.state.event_recorder
CONFIG_APPLY_COORDINATOR = app.state.config_apply_coordinator
__all__ = [
    "create_app",
    "create_event_recorder_from_config",
    "log_runtime_startup",
    "app",
    "CONFIG_PATH",
    "INITIAL_RUNTIME",
    "RUNTIME_MANAGER",
    "EVENT_RECORDER",
    "CONFIG_APPLY_COORDINATOR",
]
