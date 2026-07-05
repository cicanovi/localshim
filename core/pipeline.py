import asyncio
import time


class PluginExecutionError(Exception):
    def __init__(self, plugin_name, phase, original_error):
        self.plugin_name = plugin_name
        self.phase = phase
        self.original_error = original_error
        super().__init__(
            f"Plugin '{plugin_name}' failed during {phase}: {original_error}"
        )


def elapsed_ms_since(start_time):
    return (time.perf_counter() - start_time) * 1000


def exceeded_timeout(elapsed_ms, timeout_seconds):
    if timeout_seconds is None:
        return False
    return elapsed_ms > timeout_seconds * 1000


def record_plugin_error(ctx, plugin_name, phase, error, fail_mode):
    if ctx is None:
        return
    ctx.add_error(plugin=plugin_name, phase=phase, error=error, fail_mode=fail_mode)


def run_plugin_chain(payload, plugins, hook_name, phase, ctx=None):
    for index, runtime in enumerate(plugins):
        plugin_name = runtime.name
        plugin = runtime.plugin
        fail_mode = runtime.fail_mode
        timeout_seconds = runtime.timeout_seconds
        hook = getattr(plugin, hook_name)
        start_time = time.perf_counter()
        try:
            payload = hook(payload, ctx)
        except Exception as error:
            elapsed_ms = elapsed_ms_since(start_time)
            timed_out = exceeded_timeout(elapsed_ms, timeout_seconds)
            if ctx is not None:
                ctx.add_plugin_run(
                    index=index,
                    plugin=plugin_name,
                    phase=phase,
                    hook=hook_name,
                    success=False,
                    elapsed_ms=elapsed_ms,
                    fail_mode=fail_mode,
                    timeout_seconds=timeout_seconds,
                    timed_out=timed_out,
                    error=error,
                )
            record_plugin_error(
                ctx=ctx,
                plugin_name=plugin_name,
                phase=phase,
                error=error,
                fail_mode=fail_mode,
            )
            if fail_mode == "continue":
                continue
            raise PluginExecutionError(
                plugin_name=plugin_name, phase=phase, original_error=error
            ) from error
        elapsed_ms = elapsed_ms_since(start_time)
        timed_out = exceeded_timeout(elapsed_ms, timeout_seconds)
        if ctx is not None:
            ctx.add_plugin_run(
                index=index,
                plugin=plugin_name,
                phase=phase,
                hook=hook_name,
                success=True,
                elapsed_ms=elapsed_ms,
                fail_mode=fail_mode,
                timeout_seconds=timeout_seconds,
                timed_out=timed_out,
            )
    return payload


async def run_plugin_chain_async(payload, plugins, hook_name, phase, ctx=None):
    return await asyncio.to_thread(
        run_plugin_chain, payload, plugins, hook_name, phase, ctx
    )


def process_request(req, plugins, ctx=None):
    return run_plugin_chain(
        payload=req, plugins=plugins, hook_name="on_request", phase="request", ctx=ctx
    )


def process_response(res, plugins, ctx=None):
    return run_plugin_chain(
        payload=res, plugins=plugins, hook_name="on_response", phase="response", ctx=ctx
    )
