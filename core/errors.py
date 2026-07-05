from fastapi.responses import JSONResponse


class LocalShimError(Exception):
    status_code = 500
    error_type = "localshim_error"

    def __init__(self, message, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_response_body(self):
        return {
            "error": {"type": self.error_type, "message": self.message, **self.details}
        }


class BadRequestError(LocalShimError):
    status_code = 400
    error_type = "bad_request"


class StreamingDownstreamPluginsUnsupportedError(LocalShimError):
    status_code = 400
    error_type = "streaming_downstream_plugins_unsupported"

    def __init__(self):
        super().__init__(
            "Streaming downstream plugins are not supported yet. Remove downstream plugins or set stream=false."
        )


class BackendRequestError(LocalShimError):
    status_code = 502
    error_type = "backend_request_error"


class BackendResponseError(LocalShimError):
    status_code = 502
    error_type = "backend_response_error"


class RuntimeBuildError(LocalShimError):
    status_code = 400
    error_type = "runtime_build_error"


class RuntimeDrainTimeoutError(LocalShimError):
    status_code = 503
    error_type = "runtime_drain_timeout"

    def __init__(self):
        super().__init__("Timed out waiting for active runtime requests to drain")


class RuntimeReconfiguringError(LocalShimError):
    status_code = 503
    error_type = "runtime_reconfiguring"

    def __init__(self):
        super().__init__("LocalShim is applying a configuration update")


class InternalServerError(LocalShimError):
    status_code = 500
    error_type = "internal_error"


def error_response(error):
    return JSONResponse(status_code=error.status_code, content=error.to_response_body())
