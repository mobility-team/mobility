# app/components/features/map/map.py
from __future__ import annotations

import json
import pydeck as pdk
import dash_deck
from dash import html, Input, Output, State, callback
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
import dash_mantine_components as dmc

from app.scenario.scenario_001_from_docs import load_scenario
from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.scenario import ScenarioControls  # 👈 les contrôles Mantine


# ---------- CONSTANTES ----------
CARTO_POSITRON_GL = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
FALLBACK_CENTER = (1.4442, 43.6045)  # Toulouse


# ---------- HELPERS ----------
def _centroids_lonlat(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    g = gdf.copy()
    if g.crs is None:
        g = g.set_crs(4326, allow_override=True)
    g_m = g.to_crs(3857)
    pts_m = g_m.geometry.centroid
    pts_ll = gpd.GeoSeries(pts_m, crs=g_m.crs).to_crs(4326)
    g["lon"] = pts_ll.x.astype("float64")
    g["lat"] = pts_ll.y.astype("float64")
    return g


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


def _polygons_for_layer(zones_gdf: gpd.GeoDataFrame):
    g = zones_gdf
    if g.crs is None or getattr(g.crs, "to_epsg", lambda: None)() != 4326:
        g = g.to_crs(4326)

    polygons = []
    for _, row in g.iterrows():
        geom = row.geometry
        zone_id = row.get("transport_zone_id", "Zone inconnue")
        insee = row.get("local_admin_unit_id", "N/A")
        travel_time = _fmt_num(row.get("average_travel_time", np.nan), 1)
        legend = row.get("__legend", "")

        total_dist_km = _fmt_num(row.get("total_dist_km", np.nan), 1)
        total_time_min = _fmt_num(row.get("total_time_min", np.nan), 1)

        share_car = _fmt_pct(row.get("share_car", np.nan), 1)
        share_bicycle = _fmt_pct(row.get("share_bicycle", np.nan), 1)
        share_walk = _fmt_pct(row.get("share_walk", np.nan), 1)

        color = row.get("__color", [180, 180, 180, 160])

        if isinstance(geom, Polygon):
            rings = [list(geom.exterior.coords)]
        elif isinstance(geom, MultiPolygon):
            rings = [list(p.exterior.coords) for p in geom.geoms]
        else:
            continue

        for ring in rings:
            polygons.append({
                "geometry": [[float(x), float(y)] for x, y in ring],
                "fill_rgba": color,
                "Unité INSEE": str(insee),
                "Identifiant de zone": str(zone_id),
                "Temps moyen de trajet (minutes)": travel_time,
                "Niveau d’accessibilité": legend,
                "Distance totale parcourue (km/jour)": total_dist_km,
                "Temps total de déplacement (min/jour)": total_time_min,
                "Part des trajets en voiture (%)": share_car,
                "Part des trajets à vélo (%)": share_bicycle,
                "Part des trajets à pied (%)": share_walk,
            })
    return polygons


# ---------- DECK FACTORY ----------
def _deck_json(scn: dict | None = None) -> str:
    layers = []
    lon_center, lat_center = FALLBACK_CENTER

    try:
        if scn is None:
            scn = load_scenario()

        zones_gdf = scn["zones_gdf"].copy()
        flows_df = scn["flows_df"].copy()
        zones_lookup = scn["zones_lookup"].copy()

        if not zones_gdf.empty:
            zvalid = zones_gdf[zones_gdf.geometry.notnull() & zones_gdf.geometry.is_valid]
            if not zvalid.empty:
                c = zvalid.to_crs(4326).geometry.unary_union.centroid
                lon_center, lat_center = float(c.x), float(c.y)

        at = pd.to_numeric(zones_gdf.get("average_travel_time", pd.Series(dtype="float64")), errors="coerce")
        zones_gdf["average_travel_time"] = at
        finite_at = at.replace([np.inf, -np.inf], np.nan).dropna()
        vmin, vmax = (finite_at.min(), finite_at.max()) if not finite_at.empty else (0.0, 1.0)
        rng = vmax - vmin if (vmax - vmin) > 1e-9 else 1.0
        t1, t2 = vmin + rng / 3.0, vmin + 2 * rng / 3.0

        def _legend(v):
            if pd.isna(v):
                return "Donnée non disponible"
            if v <= t1:
                return "Accès rapide"
            elif v <= t2:
                return "Accès moyen"
            return "Accès lent"

        def _colorize(v):
            if pd.isna(v):
                return [200, 200, 200, 140]
            z = (float(v) - vmin) / rng
            z = max(0.0, min(1.0, z))
            r = int(255 * z)
            g = int(64 + 128 * (1 - z))
            b = int(255 * (1 - z))
            return [r, g, b, 180]

        zones_gdf["__legend"] = zones_gdf["average_travel_time"].map(_legend)
        zones_gdf["__color"] = zones_gdf["average_travel_time"].map(_colorize)

        polys = []
        for p, v in zip(_polygons_for_layer(zones_gdf), zones_gdf["average_travel_time"].tolist() or []):
            p["fill_rgba"] = _colorize(v)
            polys.append(p)

        if polys:
            zones_layer = pdk.Layer(
                "PolygonLayer",
                data=polys,
                get_polygon="geometry",
                get_fill_color="fill_rgba",
                pickable=True,
                filled=True,
                stroked=True,
                get_line_color=[0, 0, 0, 80],
                lineWidthMinPixels=1.5,
                elevation_scale=0,
                opacity=0.4,
                auto_highlight=True,
            )
            layers.append(zones_layer)

        lookup_ll = _centroids_lonlat(zones_lookup)
        flows_df["flow_volume"] = pd.to_numeric(flows_df["flow_volume"], errors="coerce").fillna(0.0)
        flows_df = flows_df[flows_df["flow_volume"] > 0]
        flows = flows_df.merge(
            lookup_ll[["transport_zone_id", "lon", "lat"]],
            left_on="from", right_on="transport_zone_id", how="left"
        ).rename(columns={"lon": "lon_from", "lat": "lat_from"}).drop(columns=["transport_zone_id"])
        flows = flows.merge(
            lookup_ll[["transport_zone_id", "lon", "lat"]],
            left_on="to", right_on="transport_zone_id", how="left"
        ).rename(columns={"lon": "lon_to", "lat": "lat_to"}).drop(columns=["transport_zone_id"])
        flows = flows.dropna(subset=["lon_from", "lat_from", "lon_to", "lat_to"])
        flows["flow_width"] = (1.0 + np.log1p(flows["flow_volume"])).astype("float64").clip(0.5, 6.0)

        arcs_layer = pdk.Layer(
            "ArcLayer",
            data=flows,
            get_source_position=["lon_from", "lat_from"],
            get_target_position=["lon_to", "lat_to"],
            get_source_color=[255, 140, 0, 180],
            get_target_color=[0, 128, 255, 180],
            get_width="flow_width",
            pickable=True,
        )
        layers.append(arcs_layer)

    except Exception as e:
        print("Overlay scénario désactivé (erreur):", e)

    view_state = pdk.ViewState(
        longitude=lon_center,
        latitude=lat_center,
        zoom=10,
        pitch=35,
        bearing=-15,
    )

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_provider="carto",
        map_style=CARTO_POSITRON_GL,
        views=[pdk.View(type="MapView", controller=True)],
    )

    return deck.to_json()


# ---------- DASH COMPONENT ----------
def Map(id_prefix: str = "map"):
    """
    Carte avec:
    - Deck.gl
    - Panel résumé global (overlay)
    - Panel contrôles 'Rayon' (overlay), 100px sous le header
    """
    scn = load_scenario()
    zones_gdf = scn["zones_gdf"]

    deckgl = dash_deck.DeckGL(
        id=f"{id_prefix}-deck-map",
        data=_deck_json(scn),
        tooltip={
            "html": (
                "<div style='font-family:Arial, sans-serif;'>"
                "<b style='font-size:14px;'>Zone d’étude</b><br>"
                "<b>Unité INSEE :</b> {Unité INSEE}<br/>"
                "<b>Identifiant de zone :</b> {Identifiant de zone}<br/><br/>"
                "<b style='font-size:13px;'>Mobilité moyenne</b><br>"
                "Temps moyen de trajet : <b>{Temps moyen de trajet (minutes)}</b> min/jour<br>"
                "Distance totale parcourue : <b>{Distance totale parcourue (km/jour)}</b> km/jour<br>"
                "Niveau d’accessibilité : <b>{Niveau d’accessibilité}</b><br/><br/>"
                "<b style='font-size:13px;'>Répartition modale</b><br>"
                "Part des trajets en voiture : <b>{Part des trajets en voiture (%)}</b><br>"
                "Part des trajets à vélo : <b>{Part des trajets à vélo (%)}</b><br>"
                "Part des trajets à pied : <b>{Part des trajets à pied (%)}</b>"
                "</div>"
            ),
            "style": {
                "backgroundColor": "rgba(255,255,255,0.9)",
                "color": "#111",
                "fontSize": "12px",
                "padding": "8px",
                "borderRadius": "6px",
            },
        },
        mapboxKey="",
        style={"position": "absolute", "inset": 0},
    )

    # Panel résumé (overlay)
    summary_wrapper = html.Div(
        id=f"{id_prefix}-summary-wrapper",
        children=StudyAreaSummary(zones_gdf, visible=True, id_prefix=id_prefix),
        style={},  # style géré dans le composant lui-même (absolute, top/right)
    )

    # Panel contrôles Rayon (overlay dans la carte, ~100px sous le header)
    controls_overlay = html.Div(
        dmc.Paper(
            children=[
                ScenarioControls(id_prefix=id_prefix, min_radius=15, max_radius=50, step=1, default=40),
            ],
            withBorder=True,
            shadow="sm",
            radius="md",
            p="sm",
            style={"width": "fit-content"},
        ),
        id=f"{id_prefix}-controls-overlay",
        style={
            "position": "absolute",
            "top": "100px",     # 👈 100 px sous le header
            "left": "12px",
            "zIndex": 1100,
            "pointerEvents": "auto",
        },
    )

    return html.Div(
        [deckgl, summary_wrapper, controls_overlay],
        style={
            "position": "relative",
            "width": "100%",
            "height": "100%",
            "background": "#fff",
        },
    )


# ---------- CALLBACKS ----------
# Ferme le panneau de synthèse au clic sur la croix
@callback(
    Output("map-study-summary", "style"),
    Input("map-summary-close", "n_clicks"),
    State("map-study-summary", "style"),
    prevent_initial_call=True,
)
def _close_summary(n_clicks, style):
    style = dict(style or {})
    style["display"] = "none"
    return style
