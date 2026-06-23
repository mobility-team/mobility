import logging


TRACE_LEVEL = 5


def register_trace_level() -> None:
    """Register the TRACE logging level used for expensive diagnostics."""
    logging.addLevelName(TRACE_LEVEL, "TRACE")


def is_trace_enabled() -> bool:
    """Return True when very detailed diagnostics should be computed."""
    register_trace_level()
    return logging.root.isEnabledFor(TRACE_LEVEL)
