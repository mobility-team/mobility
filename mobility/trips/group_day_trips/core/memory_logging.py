import logging
from typing import Any

import polars as pl
import psutil


def format_bytes(value: int | None) -> str:
    """Return a human-readable byte count."""
    if value is None:
        return "unknown"

    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.2f}{unit}"
        size /= 1024.0
    return f"{size:.2f}TB"


def frame_summary(frame: pl.DataFrame | pl.LazyFrame | None) -> str:
    """Return a compact rows/cols/estimated-size summary for one dataframe-like object."""
    if frame is None:
        return "none"

    if isinstance(frame, pl.LazyFrame):
        try:
            cols = len(frame.collect_schema().names())
        except Exception:
            cols = "unknown"
        return f"lazy cols={cols}"

    try:
        estimated_size = frame.estimated_size()
    except Exception:
        estimated_size = None

    size_text = format_bytes(estimated_size) if estimated_size is not None else "unknown"
    return f"rows={frame.height}, cols={frame.width}, est={size_text}"


def log_memory_checkpoint(
    label: str,
    **objects: Any,
) -> None:
    """Log process memory plus cheap summaries of already-available objects."""
    if not logging.root.isEnabledFor(logging.DEBUG):
        return

    memory_info = psutil.Process().memory_info()
    parts = [
        f"rss={format_bytes(memory_info.rss)}",
        f"vms={format_bytes(memory_info.vms)}",
    ]
    private = getattr(memory_info, "private", None)
    if private is not None:
        parts.append(f"private={format_bytes(private)}")

    for name, obj in objects.items():
        if isinstance(obj, (pl.DataFrame, pl.LazyFrame)) or obj is None:
            parts.append(f"{name}={frame_summary(obj)}")
        elif isinstance(obj, dict):
            parts.append(f"{name}=entries={len(obj)}")
        elif isinstance(obj, (list, tuple, set)):
            parts.append(f"{name}=len={len(obj)}")
        else:
            parts.append(f"{name}={type(obj).__name__}")

    logging.debug("Memory checkpoint %s | %s", label, " | ".join(parts))
