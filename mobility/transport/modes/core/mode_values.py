from typing import Any


def get_mode_values(modes: list[Any], *extra_values: str) -> list[str]:
    """Return configured mode names plus any synthetic runtime-only values."""
    return [mode.inputs["parameters"].name for mode in modes] + list(extra_values)
