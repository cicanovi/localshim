class ShimContext:
    def __init__(self, request=None):
        self.request = request
        self.metadata = {}
        self.events = []
        self.errors = []
        self.plugin_runs = []
        self.backend_response = None

    def add_event(self, event_type, **details):
        self.events.append({"type": event_type, **details})

    def add_error(self, plugin, phase, error, fail_mode, **details):
        self.errors.append(
            {
                "plugin": plugin,
                "phase": phase,
                "error": str(error),
                "fail_mode": fail_mode,
                **details,
            }
        )

    def add_plugin_run(
        self,
        index,
        plugin,
        phase,
        hook,
        success,
        elapsed_ms,
        fail_mode,
        timeout_seconds,
        timed_out,
        error=None,
        **details,
    ):
        run = {
            "index": index,
            "plugin": plugin,
            "phase": phase,
            "hook": hook,
            "success": success,
            "elapsed_ms": elapsed_ms,
            "fail_mode": fail_mode,
            "timeout_seconds": timeout_seconds,
            "timed_out": timed_out,
            **details,
        }
        if error is not None:
            run["error"] = str(error)
        self.plugin_runs.append(run)

    def set_backend_response(self, response):
        self.backend_response = response
