"""
legend.py
==========

Composants d’affichage pour la légende compacte associée à la carte des temps moyens.

Ce module permet de visualiser la correspondance entre les valeurs de temps
de déplacement moyen (`average_travel_time`) et leur codage couleur.  
La légende est construite avec trois classes qualitatives :
- **Accès rapide**  
- **Accès moyen**  
- **Accès lent**

Ainsi qu’un **dégradé continu** allant du bleu (temps faible) au rouge (temps élevé).

Fonctionnalités principales :
- `_chip()`: Génère un mini-bloc coloré avec son libellé.
- `LegendCompact()`: Construit la légende complète avec les bornes, la barre de dégradé,
  et une explication textuelle.
"""

from dash import html
import dash_mantine_components as dmc
import pandas as pd
from .utils import safe_min_max, colorize_from_range, rgb_str, fmt_num


def _chip(color_rgb, label: str):
    """Crée un petit bloc coloré (chip) avec un label descriptif.

    Ce composant est utilisé pour représenter chaque classe de la légende,
    associant une couleur à une plage de valeurs (par exemple : “Accès rapide — 12–18 min”).

    Args:
        color_rgb (Tuple[int, int, int]): Triplet RGB de la couleur du bloc.
        label (str): Texte associé à la couleur.

    Returns:
        dmc.Group: Composant contenant un carré coloré et son libellé.
    """
    r, g, b = color_rgb
    return dmc.Group(
        [
            html.Div(
                style={
                    "width": "14px",
                    "height": "14px",
                    "borderRadius": "3px",
                    "background": f"rgb({r},{g},{b})",
                    "border": "1px solid rgba(0,0,0,0.2)",
                    "flexShrink": 0,
                }
            ),
            dmc.Text(label, size="sm"),
        ],
        gap="xs",
        align="center",
        wrap="nowrap",
    )


def LegendCompact(avg_series):
    """Construit une légende compacte pour les temps moyens de déplacement.

    La légende affiche :
      - Trois classes qualitatives : *Accès rapide*, *Accès moyen* et *Accès lent*.
      - Une barre de dégradé continue (du bleu au rouge).
      - Les valeurs numériques min/max correspondantes.
      - Un court texte explicatif sur l’interprétation des couleurs.

    Si les valeurs sont manquantes ou uniformes, une alerte grisée est affichée.

    Args:
        avg_series (pandas.Series | list | ndarray): Série numérique contenant
            les temps moyens de déplacement (en minutes).

    Returns:
        dmc.Stack | dmc.Alert:  
        - Une pile verticale (`dmc.Stack`) avec les couleurs, la barre de dégradé
          et les libellés si les données sont valides.  
        - Un message `dmc.Alert` indiquant l’absence de données sinon.

    Example:
        >>> LegendCompact(zones_gdf["average_travel_time"])
        # Renvoie un composant Dash contenant la légende complète
    """
    vmin, vmax = safe_min_max(avg_series)
    if pd.isna(vmin) or pd.isna(vmax) or vmax - vmin <= 1e-9:
        return dmc.Alert(
            "Légende indisponible (valeurs manquantes).",
            color="gray",
            variant="light",
            radius="sm",
            styles={"root": {"padding": "8px"}},
        )

    rng = vmax - vmin
    t1 = vmin + rng / 3.0
    t2 = vmin + 2.0 * rng / 3.0

    # Couleurs représentatives des trois classes
    c1 = colorize_from_range((vmin + t1) / 2.0, vmin, vmax)
    c2 = colorize_from_range((t1 + t2) / 2.0, vmin, vmax)
    c3 = colorize_from_range((t2 + vmax) / 2.0, vmin, vmax)

    # Couleurs pour le dégradé continu
    left = rgb_str(colorize_from_range(vmin + 1e-6, vmin, vmax))
    mid = rgb_str(colorize_from_range((vmin + vmax) / 2.0, vmin, vmax))
    right = rgb_str(colorize_from_range(vmax - 1e-6, vmin, vmax))

    return dmc.Stack(
        [
            dmc.Text("Légende — temps moyen (min)", fw=600, size="sm"),
            _chip(c1, f"Accès rapide — {fmt_num(vmin, 1)}–{fmt_num(t1, 1)} min"),
            _chip(c2, f"Accès moyen — {fmt_num(t1, 1)}–{fmt_num(t2, 1)} min"),
            _chip(c3, f"Accès lent — {fmt_num(t2, 1)}–{fmt_num(vmax, 1)} min"),
            html.Div(
                style={
                    "height": "10px",
                    "width": "100%",
                    "borderRadius": "6px",
                    "background": f"linear-gradient(to right, {left}, {mid}, {right})",
                    "border": "1px solid rgba(0,0,0,0.15)",
                    "marginTop": "6px",
                }
            ),
            dmc.Group(
                [
                    dmc.Text(f"{fmt_num(vmin, 1)}", size="xs", style={"opacity": 0.8}),
                    dmc.Text("→", size="xs", style={"opacity": 0.6}),
                    dmc.Text(f"{fmt_num(vmax, 1)}", size="xs", style={"opacity": 0.8}),
                ],
                justify="space-between",
                align="center",
                gap="xs",
            ),
            dmc.Text(
                "Plus la teinte est chaude, plus le déplacement moyen est long.",
                size="xs",
                style={"opacity": 0.75},
            ),
        ],
        gap="xs",
    )
