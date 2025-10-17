from dataclasses import dataclass
import numpy as np
import pandas as pd

@dataclass(frozen=True)
class ColorScale:
    vmin: float
    vmax: float

    def _rng(self) -> float:
        r = self.vmax - self.vmin
        return r if r > 1e-9 else 1.0

    def legend(self, v) -> str:
        if pd.isna(v):
            return "Donnée non disponible"
        rng = self._rng()
        t1 = self.vmin + rng / 3.0
        t2 = self.vmin + 2 * rng / 3.0
        v = float(v)
        if v <= t1:
            return "Accès rapide"
        if v <= t2:
            return "Accès moyen"
        return "Accès lent"

    def rgba(self, v) -> list[int]:
        if pd.isna(v):
            return [200, 200, 200, 140]
        z = (float(v) - self.vmin) / self._rng()
        z = max(0.0, min(1.0, z))
        r = int(255 * z)
        g = int(64 + 128 * (1 - z))
        b = int(255 * (1 - z))
        return [r, g, b, 180]

def fit_color_scale(series: pd.Series) -> ColorScale:
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    vmin, vmax = (float(s.min()), float(s.max())) if not s.empty else (0.0, 1.0)
    return ColorScale(vmin=vmin, vmax=vmax)
