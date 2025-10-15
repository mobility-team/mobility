# app/components/features/study_area_summary/study_area_summary.py
from dash import html
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


def StudyAreaSummary(zones_gdf, visible=True, id_prefix="map"):
    """
    Panneau d’agrégats globaux de la zone d’étude.
    - visible: True -> affiché, False -> masqué (utile pour futur toggle)
    - id_prefix: pour éviter les collisions si plusieurs cartes
    """
    comp_id = f"{id_prefix}-study-summary"
    close_id = f"{id_prefix}-summary-close"

    if zones_gdf is None or getattr(zones_gdf, "empty", True):
        content = [
            html.Div(
                "Données globales indisponibles.",
                style={"fontStyle": "italic", "opacity": 0.8},
            )
        ]
    else:
        avg_time = _safe_mean(zones_gdf.get("average_travel_time"))
        avg_dist = _safe_mean(zones_gdf.get("total_dist_km"))
        share_car = _safe_mean(zones_gdf.get("share_car"))
        share_bike = _safe_mean(zones_gdf.get("share_bicycle"))
        share_walk = _safe_mean(zones_gdf.get("share_walk"))

        content = [
            html.Div(
                [
                    html.Div(
                        [
                            html.Span("Temps moyen de trajet : "),
                            html.B(_fmt_num(avg_time, 1)),
                            html.Span(" min/jour"),
                        ]
                    ),
                    html.Div(
                        [
                            html.Span("Distance totale moyenne : "),
                            html.B(_fmt_num(avg_dist, 1)),
                            html.Span(" km/jour"),
                        ],
                        style={"marginBottom": "6px"},
                    ),
                ]
            ),
            html.Div(
                "Répartition modale",
                style={"fontWeight": "600", "margin": "6px 0 4px"},
            ),
            html.Div([html.Span("Voiture : "), html.B(_fmt_pct(share_car, 1))]),
            html.Div([html.Span("Vélo : "), html.B(_fmt_pct(share_bike, 1))]),
            html.Div([html.Span("À pied : "), html.B(_fmt_pct(share_walk, 1))]),
        ]

    return html.Div(
        id=comp_id,
        children=[
            html.Div(
                [
                    html.Div("Résumé global de la zone d'étude", style={"fontWeight": 700}),
                    html.Button(
                        "×",
                        id=close_id,
                        n_clicks=0,
                        title="Fermer",
                        style={
                            "border": "none",
                            "background": "transparent",
                            "fontSize": "18px",
                            "lineHeight": "18px",
                            "cursor": "pointer",
                            "padding": "0",
                            "margin": "0 0 0 8px",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "marginBottom": "8px",
                },
            ),
            html.Div(content, style={"fontSize": "13px"}),
        ],
        style={
            "display": "block" if visible else "none",
            "position": "absolute",
            "top": "100px",
            "right": "12px",
            "width": "280px",
            "zIndex": 1000,
            "background": "rgba(255,255,255,0.95)",
            "backdropFilter": "blur(2px)",
            "color": "#111",
            "padding": "10px 12px",
            "borderRadius": "8px",
            "boxShadow": "0 4px 12px rgba(0,0,0,0.18)",
            "border": "1px solid rgba(0,0,0,0.08)",
        },
    )
