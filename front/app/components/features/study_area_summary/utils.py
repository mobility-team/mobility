"""
utils.py
=========

Module utilitaire regroupant des fonctions de formatage, de calculs statistiques
et de génération de couleurs.  
Ces fonctions sont utilisées dans différents composants de l’application
(panneaux de résumé, cartes, indicateurs, etc.) pour assurer une cohérence
visuelle et numérique des données affichées.

Fonctionnalités principales :
- Formatage des nombres et pourcentages (`fmt_num`, `fmt_pct`)
- Calculs robustes de moyennes et d’extrema (`safe_mean`, `safe_min_max`)
- Génération de couleurs selon une rampe continue (`colorize_from_range`)
- Conversion des couleurs RGB au format CSS (`rgb_str`)
"""

from __future__ import annotations
from typing import Tuple
import numpy as np
import pandas as pd


def fmt_num(v, nd: int = 1) -> str:
    """Formate un nombre flottant avec un nombre fixe de décimales.

    Convertit une valeur numérique en chaîne de caractères formatée,
    arrondie à `nd` décimales. Si la conversion échoue (valeur None,
    non numérique, etc.), renvoie `"N/A"`.
    """
    try:
        return f"{round(float(v), nd):.{nd}f}"
    except Exception:
        return "N/A"


def fmt_pct(v, nd: int = 1) -> str:
    """Formate une valeur en pourcentage avec arrondi.

    Multiplie la valeur par 100, puis la formate avec `nd` décimales.
    En cas d’erreur ou de valeur invalide, renvoie `"N/A"`.
    """
    try:
        return f"{round(float(v) * 100.0, nd):.{nd}f} %"
    except Exception:
        return "N/A"


def safe_mean(series) -> float:
    """Calcule la moyenne d'une série de valeurs de manière sécurisée.

    Convertit la série en valeurs numériques, ignore les NaN et les
    erreurs de conversion. Retourne `NaN` si la série est vide ou None.
    """
    if series is None:
        return float("nan")
    s = pd.to_numeric(series, errors="coerce")
    return float(np.nanmean(s)) if s.size else float("nan")


def safe_min_max(series) -> Tuple[float, float]:
    """Renvoie les valeurs minimale et maximale d'une série en toute sécurité.

    Ignore les valeurs non numériques, infinies ou manquantes. Retourne `(NaN, NaN)`
    si la série est vide ou invalide.
    """
    if series is None:
        return float("nan"), float("nan")
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return float("nan"), float("nan")
    return float(s.min()), float(s.max())


def colorize_from_range(value, vmin, vmax):
    """Convertit une valeur numérique en couleur RGB selon une rampe de dégradé.

    La rampe est la même que celle utilisée pour la carte :
    - rouge augmente avec la valeur (`r = 255 * z`)
    - vert diminue légèrement (`g = 64 + 128 * (1 - z)`)
    - bleu diminue avec la valeur (`b = 255 * (1 - z)`)

    Si la valeur ou les bornes sont invalides, renvoie un gris neutre `(200, 200, 200)`.
    """
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
    """Convertit un tuple RGB en chaîne CSS utilisable.

    Args:
        rgb (Tuple[int, int, int]): Triplet de composantes (R, G, B).

    Returns:
        str: Chaîne formatée `"rgb(r,g,b)"`.
    """
    r, g, b = rgb
    return f"rgb({r},{g},{b})"
