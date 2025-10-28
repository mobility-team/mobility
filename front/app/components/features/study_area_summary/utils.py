from __future__ import annotations
from typing import Tuple
import numpy as np
import pandas as pd

def fmt_num(v, nd: int = 1) -> str:
    try:
        return f"{round(float(v), nd):.{nd}f}"
    except Exception:
        return "N/A"

def fmt_pct(v, nd: int = 1) -> str:
    try:
        return f"{round(float(v) * 100.0, nd):.{nd}f} %"
    except Exception:
        return "N/A"

def safe_mean(series) -> float:
    if series is None:
        return float("nan")
    s = pd.to_numeric(series, errors="coerce")
    return float(np.nanmean(s)) if s.size else float("nan")

def safe_min_max(series) -> Tuple[float, float]:
    if series is None:
        return float("nan"), float("nan")
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return float("nan"), float("nan")
    return float(s.min()), float(s.max())

def colorize_from_range(value, vmin, vmax):
    """Même rampe que la carte : r=255*z ; g=64+128*(1-z) ; b=255*(1-z)"""
    if value is None or pd.isna(value) or vmin is None or vmax is None or (vmax - vmin) <= 1e-9:
        return (200, 200, 200)
    rng = max(vmax - vmin, 1e-9)
    z = (float(value) - vmin) / rng
    z = max(0.0, min(1.0, z))
    r = int(255 * z)
    g = int(64 + 128 * (1 - z))
    b = int(255 * (1 - z))
    return (r, g, b)

def rgb_str(rgb) -> str:
    r, g, b = rgb
    return f"rgb({r},{g},{b})"
