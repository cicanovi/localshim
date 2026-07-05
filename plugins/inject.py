from core.plugins import ShimPlugin


class InjectSystemPrompt(ShimPlugin):
    def __init__(self, content="Respond concisely.", skip_if_system_exists=False):
        super().__init__(content=content, skip_if_system_exists=skip_if_system_exists)
        self.content = content
        self.skip_if_system_exists = skip_if_system_exists

    def on_request(self, req, ctx=None):
        messages = req.setdefault("messages", [])
        if self.skip_if_system_exists:
            if messages and messages[0].get("role") == "system":
                return req
        messages.insert(0, {"role": "system", "content": self.content})
        if ctx is not None:
            ctx.add_event(
                "plugin_action",
                plugin="inject_system_prompt",
                action="system_prompt_injected",
            )
        return req
