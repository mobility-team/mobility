from dash import html
import dash_mantine_components as dmc
import pandas as pd
from .utils import safe_min_max, colorize_from_range, rgb_str, fmt_num

def _chip(color_rgb, label: str):
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
    """
    Légende compacte :
      - 3 classes : Accès rapide / moyen / lent (mêmes seuils que la carte)
      - barre de dégradé continue + libellés min/max
    """
    vmin, vmax = safe_min_max(avg_series)
    if pd.isna(vmin) or pd.isna(vmax) or vmax - vmin <= 1e-9:
        return dmc.Alert(
            "Légende indisponible (valeurs manquantes).",
            color="gray", variant="light", radius="sm",
            styles={"root": {"padding": "8px"}}
        )

    rng = vmax - vmin
    t1 = vmin + rng / 3.0
    t2 = vmin + 2.0 * rng / 3.0

    # couleurs représentatives au milieu de chaque classe
    c1 = colorize_from_range((vmin + t1) / 2.0, vmin, vmax)
    c2 = colorize_from_range((t1 + t2) / 2.0, vmin, vmax)
    c3 = colorize_from_range((t2 + vmax) / 2.0, vmin, vmax)

    # dégradé continu
    left  = rgb_str(colorize_from_range(vmin + 1e-6, vmin, vmax))
    mid   = rgb_str(colorize_from_range((vmin + vmax) / 2.0, vmin, vmax))
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
                size="xs", style={"opacity": 0.75},
            ),
        ],
        gap="xs",
    )
