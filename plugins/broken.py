from core.plugins import ShimPlugin


class BrokenPlugin(ShimPlugin):
    def on_request(self, req, ctx=None):
        raise RuntimeError("intentional request failure")

    def on_response(self, res, ctx=None):
        raise RuntimeError("intentional response failure")
