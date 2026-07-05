from core.plugins import ShimPlugin


class DebugPlugin(ShimPlugin):
    def on_request(self, req, ctx=None):
        if ctx is not None:
            ctx.add_event("plugin_action", plugin="debug", action="request_observed")
        return req

    def on_response(self, res, ctx=None):
        if ctx is not None:
            ctx.add_event("plugin_action", plugin="debug", action="response_observed")
        res["choices"][0]["message"]["content"] += " [debug downstream]"
        return res
