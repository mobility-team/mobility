"""
color_scale.py
===============

Échelle de couleurs pour la carte des temps moyens de déplacement.

Ce module fournit :
- une palette **bleu → gris → orange** cohérente avec la légende qualitative
  (*Accès rapide* / *Accès moyen* / *Accès lent*) ;
- une dataclass `ColorScale` permettant de convertir une valeur numérique
  en couleur RGBA (0–255) et d’obtenir un libellé de légende lisible ;
- une fonction d’ajustement `fit_color_scale()` qui calibre automatiquement
  `vmin` / `vmax` à partir d’une série de données (percentiles).

Fonctionnalités principales
---------------------------
- `_interp_color(c1, c2, t)` : interpolation linéaire entre deux couleurs RGB.
- `_build_legend_palette(n)` : construit la palette bleu→gris→orange.
- `ColorScale.rgba(v)` : mappe une valeur à un tuple `[R, G, B, A]`.
- `ColorScale.legend(v)` : rend un libellé humain (ex. `"12.3 min"`).
- `fit_color_scale(series)` : ajuste l’échelle à une série pandas (P5–P95).
"""

from dataclasses import dataclass
import numpy as np
import pandas as pd


def _interp_color(c1, c2, t):
    """Interpole linéairement entre deux couleurs RGB.

    Args:
        c1 (Tuple[int, int, int]): Couleur de départ (R, G, B).
        c2 (Tuple[int, int, int]): Couleur d’arrivée (R, G, B).
        t (float): Paramètre d’interpolation dans [0, 1].

    Returns:
        Tuple[int, int, int]: Couleur RGB interpolée.
    """
    return (
        int(c1[0] + (c2[0] - c1[0]) * t),
        int(c1[1] + (c2[1] - c1[1]) * t),
        int(c1[2] + (c2[2] - c1[2]) * t),
    )


def _build_legend_palette(n=256):
    """Construit une palette bleu → gris → orange pour la légende.

    Conçue pour coller à la sémantique :
      - bleu  : accès rapide (valeurs basses)
      - gris  : accès moyen (valeurs médianes)
      - orange: accès lent (valeurs hautes)

    La palette est générée par interpolation linéaire entre
    (bleu→gris) puis (gris→orange).

    Args:
        n (int, optional): Nombre total de couleurs dans la palette.
            Par défaut `256`.

    Returns:
        List[Tuple[int, int, int]]: Liste de couleurs RGB.
    """
    blue   = ( 74, 160, 205)  # accès rapide
    grey   = (147, 147, 147)  # accès moyen
    orange = (228,  86,  43)  # accès lent

    mid = n // 2
    first  = [_interp_color(blue, grey, i / max(1, mid - 1)) for i in range(mid)]
    second = [_interp_color(grey, orange, i / max(1, n - mid - 1)) for i in range(n - mid)]
    return first + second


@dataclass
class ColorScale:
    """Échelle de couleurs continue basée sur des bornes min/max.

    Attributs:
        vmin (float): Valeur minimale du domaine.
        vmax (float): Valeur maximale du domaine.
        colors (List[Tuple[int, int, int]]): Palette RGB ordonnée bas→haut.
        alpha (int): Canal alpha (0–255). Par défaut `102` (~0.4 d’opacité).

    Méthodes:
        rgba(v): Convertit une valeur en `[R, G, B, A]` (uint8).
        legend(v): Produit un libellé humain (ex. `"12.1 min"`).
    """
    vmin: float
    vmax: float
    colors: list[tuple[int, int, int]]
    alpha: int = 102  # ~0.4 d’opacité

    def rgba(self, v) -> list[int]:
        """Mappe une valeur numérique à une couleur RGBA.

        Si `v` est manquante ou si `vmax <= vmin`, retourne une valeur par défaut.

        Args:
            v (float | Any): Valeur à convertir.

        Returns:
            List[int]: Couleur `[R, G, B, A]` (chaque canal 0–255).
        """
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
        """Retourne un libellé de légende lisible pour la valeur.

        Args:
            v (float | Any): Valeur à afficher.

        Returns:
            str: Libellé, ex. `"12.3 min"`, ou `"N/A"` si manquant.
        """
        if v is None or pd.isna(v):
            return "N/A"
        return f"{float(v):.1f} min"


def fit_color_scale(series: pd.Series) -> ColorScale:
    """Ajuste automatiquement une échelle de couleurs à partir d’une série.

    Utilise les percentiles **P5** et **P95** pour définir `vmin` et `vmax`,
    afin de diminuer l’influence des valeurs extrêmes.  
    Si la série est dégénérée (vmin == vmax), retombe sur `(min, max or 1.0)`.
    Si la série est vide/invalide, retombe sur le domaine `(0.0, 1.0)`.

    Args:
        series (pd.Series): Série de valeurs numériques.

    Returns:
        ColorScale: Échelle prête à l’emploi (palette 256 couleurs, alpha 102).
    """
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s):
        vmin = float(np.nanpercentile(s, 5))
        vmax = float(np.nanpercentile(s, 95))
        if vmin == vmax:
            vmin, vmax = float(s.min()), float(s.max() or 1.0)
    else:
        vmin, vmax = 0.0, 1.0
    return ColorScale(vmin=vmin, vmax=vmax, colors=_build_legend_palette(256), alpha=102)
