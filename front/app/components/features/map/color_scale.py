from dataclasses import dataclass
import numpy as np
import pandas as pd

def _interp_color(c1, c2, t):
    return (
        int(c1[0] + (c2[0] - c1[0]) * t),
        int(c1[1] + (c2[1] - c1[1]) * t),
        int(c1[2] + (c2[2] - c1[2]) * t),
    )

def _build_legend_palette(n=256):
    """
    Dégradé bleu -> gris -> orange pour coller à la légende :
      - bleu "accès rapide"
      - gris "accès moyen"
      - orange "accès lent"
    """
    blue   = ( 74, 160, 205)  
    grey   = (147, 147, 147)  
    orange = (228,  86,  43)  

    mid = n // 2
    first  = [_interp_color(blue, grey, i / max(1, mid - 1)) for i in range(mid)]
    second = [_interp_color(grey, orange, i / max(1, n - mid - 1)) for i in range(n - mid)]
    return first + second

@dataclass
class ColorScale:
    vmin: float
    vmax: float
    colors: list[tuple[int, int, int]]
    alpha: int = 102  # ~0.4 d’opacité

    def rgba(self, v) -> list[int]:
        if v is None or pd.isna(v):
            return [200, 200, 200, 40]
        if self.vmax <= self.vmin:
            idx = 0
        else:
            t = (float(v) - self.vmin) / (self.vmax - self.vmin)
            t = max(0.0, min(1.0, t))
            idx = int(t * (len(self.colors) - 1))
        r, g, b = self.colors[idx]
        return [int(r), int(g), int(b), self.alpha]

    def legend(self, v) -> str:
        if v is None or pd.isna(v):
            return "N/A"
        return f"{float(v):.1f} min"

def fit_color_scale(series: pd.Series) -> ColorScale:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s):
        vmin = float(np.nanpercentile(s, 5))
        vmax = float(np.nanpercentile(s, 95))
        if vmin == vmax:
            vmin, vmax = float(s.min()), float(s.max() or 1.0)
    else:
        vmin, vmax = 0.0, 1.0
    return ColorScale(vmin=vmin, vmax=vmax, colors=_build_legend_palette(256), alpha=102)
