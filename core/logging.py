import logging


def configure_logging(level=logging.INFO):
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )


def get_logger(name):
    return logging.getLogger(name)


def log_plugin_summary(logger, ctx):
    for run in ctx.plugin_runs:
        logger.info(
            "Plugin run: index=%s plugin=%s phase=%s hook=%s success=%s elapsed_ms=%.3f fail_mode=%s timeout_seconds=%s timed_out=%s",
            run["index"],
            run["plugin"],
            run["phase"],
            run["hook"],
            run["success"],
            run["elapsed_ms"],
            run["fail_mode"],
            run["timeout_seconds"],
            run["timed_out"],
        )
    for error in ctx.errors:
        logger.error(
            "Plugin error: plugin=%s phase=%s fail_mode=%s error=%s",
            error["plugin"],
            error["phase"],
            error["fail_mode"],
            error["error"],
        )
