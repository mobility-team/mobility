from dash import html
import dash_mantine_components as dmc
import pandas as pd
import numpy as np


def _fmt_num(v, nd=1):
    try:
        return f"{round(float(v), nd):.{nd}f}"
    except Exception:
        return "N/A"


def _fmt_pct(v, nd=1):
    try:
        return f"{round(float(v) * 100.0, nd):.{nd}f} %"
    except Exception:
        return "N/A"


def _safe_mean(series):
    if series is None:
        return float("nan")
    s = pd.to_numeric(series, errors="coerce")
    return float(np.nanmean(s)) if s.size else float("nan")


def _safe_min_max(series):
    if series is None:
        return float("nan"), float("nan")
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return float("nan"), float("nan")
    return float(s.min()), float(s.max())


def _colorize_from_range(value, vmin, vmax):
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


def _rgb_str(rgb):
    r, g, b = rgb
    return f"rgb({r},{g},{b})"


def _legend_section(avg_series):
    """
    Légende compacte :
      - 3 classes : Accès rapide / moyen / lent (mêmes seuils que la carte)
      - barre de dégradé continue + libellés min/max
    """
    vmin, vmax = _safe_min_max(avg_series)
    if pd.isna(vmin) or pd.isna(vmax) or vmax - vmin <= 1e-9:
        return dmc.Alert(
            "Légende indisponible (valeurs manquantes).",
            color="gray", variant="light", radius="sm",
            styles={"root": {"padding": "8px"}}
        )

    rng = vmax - vmin
    t1 = vmin + rng / 3.0
    t2 = vmin + 2.0 * rng / 3.0

    # couleurs représentatives des 3 classes (au milieu de chaque intervalle)
    c1 = _colorize_from_range((vmin + t1) / 2.0, vmin, vmax)
    c2 = _colorize_from_range((t1 + t2) / 2.0, vmin, vmax)
    c3 = _colorize_from_range((t2 + vmax) / 2.0, vmin, vmax)

    # dégradé continu (gauche→droite)
    left  = _rgb_str(_colorize_from_range(vmin + 1e-6, vmin, vmax))
    mid   = _rgb_str(_colorize_from_range((vmin + vmax) / 2.0, vmin, vmax))
    right = _rgb_str(_colorize_from_range(vmax - 1e-6, vmin, vmax))

    def chip(color_rgb, label):
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

    return dmc.Stack(
        [
            dmc.Text("Légende — temps moyen (min)", fw=600, size="sm"),

            # 3 classes discrètes (cohérentes avec la carte)
            chip(c1, f"Accès rapide — {_fmt_num(vmin, 1)}–{_fmt_num(t1, 1)} min"),
            chip(c2, f"Accès moyen — {_fmt_num(t1, 1)}–{_fmt_num(t2, 1)} min"),
            chip(c3, f"Accès lent — {_fmt_num(t2, 1)}–{_fmt_num(vmax, 1)} min"),

            # barre de dégradé continue avec min/max
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
                    dmc.Text(f"{_fmt_num(vmin, 1)}", size="xs", style={"opacity": 0.8}),
                    dmc.Text("→", size="xs", style={"opacity": 0.6}),
                    dmc.Text(f"{_fmt_num(vmax, 1)}", size="xs", style={"opacity": 0.8}),
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


def StudyAreaSummary(
    zones_gdf,
    visible: bool = True,
    id_prefix: str = "map",
    header_offset_px: int = 80,
    width_px: int = 340,
):
    """
    Panneau latéral droit affichant les agrégats globaux de la zone d'étude,
    avec légende enrichie (dégradé continu) et contexte (code INSEE/LAU).
    """
    comp_id = f"{id_prefix}-study-summary"

    if zones_gdf is None or getattr(zones_gdf, "empty", True):
        content = dmc.Text(
            "Données globales indisponibles.",
            size="sm",
            style={"fontStyle": "italic", "opacity": 0.8},
        )
    else:
        avg_time = _safe_mean(zones_gdf.get("average_travel_time"))
        avg_dist = _safe_mean(zones_gdf.get("total_dist_km"))
        share_car = _safe_mean(zones_gdf.get("share_car"))
        share_bike = _safe_mean(zones_gdf.get("share_bicycle"))
        share_walk = _safe_mean(zones_gdf.get("share_walk"))

        legend = _legend_section(zones_gdf.get("average_travel_time"))

        content = dmc.Stack(
            [
                dmc.Text("Résumé global de la zone d'étude", fw=700, size="md"),
                dmc.Divider(),

                # KPIs
                dmc.Stack(
                    [
                        dmc.Group(
                            [dmc.Text("Temps moyen de trajet :", size="sm"),
                             dmc.Text(f"{_fmt_num(avg_time, 1)} min/jour", fw=600, size="sm")],
                            gap="xs",
                        ),
                        dmc.Group(
                            [dmc.Text("Distance totale moyenne :", size="sm"),
                             dmc.Text(f"{_fmt_num(avg_dist, 1)} km/jour", fw=600, size="sm")],
                            gap="xs",
                        ),
                    ],
                    gap="xs",
                ),

                dmc.Divider(),

                # Modal split
                dmc.Text("Répartition modale", fw=600, size="sm"),
                dmc.Stack(
                    [
                        dmc.Group([dmc.Text("Voiture :", size="sm"), dmc.Text(_fmt_pct(share_car, 1),  fw=600, size="sm")], gap="xs"),
                        dmc.Group([dmc.Text("Vélo :",    size="sm"), dmc.Text(_fmt_pct(share_bike, 1), fw=600, size="sm")], gap="xs"),
                        dmc.Group([dmc.Text("À pied :",  size="sm"), dmc.Text(_fmt_pct(share_walk, 1), fw=600, size="sm")], gap="xs"),
                    ],
                    gap="xs",
                ),

                dmc.Divider(),

                # Legend (same thresholds/colors as map) + gradient
                legend,
            ],
            gap="md",
        )

    return html.Div(
        id=comp_id,
        children=dmc.Paper(
            content,
            withBorder=True,
            shadow="md",
            radius="md",
            p="md",
            style={
                "width": "100%",
                "height": "100%",
                "overflowY": "auto",
                "overflowX": "hidden",
                "background": "#ffffffee",
                "boxSizing": "border-box",
            },
        ),
        style={
            "display": "block" if visible else "none",
            "position": "absolute",
            "top": f"{header_offset_px}px",  
            "right": "0px",
            "bottom": "0px",
            "width": f"{width_px}px",        
            "zIndex": 1200,
            "pointerEvents": "auto",
            "overflow": "hidden",
        },
    )
