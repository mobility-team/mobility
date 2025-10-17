from typing import List, Dict
import pandas as pd
import numpy as np
import pydeck as pdk
import geopandas as gpd

from .geo_utils import ensure_wgs84, as_polygon_rings, fmt_num, fmt_pct, centroids_lonlat
from .color_scale import ColorScale

def _polygons_records(zones_gdf: gpd.GeoDataFrame, scale: ColorScale) -> List[Dict]:
    g = ensure_wgs84(zones_gdf)
    out = []
    for _, row in g.iterrows():
        rings = as_polygon_rings(row.geometry)
        if not rings:
            continue

        zone_id = row.get("transport_zone_id", "Zone inconnue")
        insee = row.get("local_admin_unit_id", "N/A")
        avg_tt = pd.to_numeric(row.get("average_travel_time", np.nan), errors="coerce")
        total_dist_km = pd.to_numeric(row.get("total_dist_km", np.nan), errors="coerce")
        total_time_min = pd.to_numeric(row.get("total_time_min", np.nan), errors="coerce")
        share_car = pd.to_numeric(row.get("share_car", np.nan), errors="coerce")
        share_bicycle = pd.to_numeric(row.get("share_bicycle", np.nan), errors="coerce")
        share_walk = pd.to_numeric(row.get("share_walk", np.nan), errors="coerce")

        for ring in rings:
            out.append({
                "geometry": [[float(x), float(y)] for x, y in ring],
                "fill_rgba": scale.rgba(avg_tt),
                "Unité INSEE": str(insee),
                "Identifiant de zone": str(zone_id),
                "Temps moyen de trajet (minutes)": fmt_num(avg_tt, 1),
                "Niveau d’accessibilité": scale.legend(avg_tt),
                "Distance totale parcourue (km/jour)": fmt_num(total_dist_km, 1),
                "Temps total de déplacement (min/jour)": fmt_num(total_time_min, 1),
                "Part des trajets en voiture (%)": fmt_pct(share_car, 1),
                "Part des trajets à vélo (%)": fmt_pct(share_bicycle, 1),
                "Part des trajets à pied (%)": fmt_pct(share_walk, 1),
            })
    return out

def build_zones_layer(zones_gdf: gpd.GeoDataFrame, scale: ColorScale) -> pdk.Layer | None:
    polys = _polygons_records(zones_gdf, scale)
    if not polys:
        return None
    return pdk.Layer(
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

def build_flows_layer(flows_df: pd.DataFrame, zones_lookup: gpd.GeoDataFrame) -> pdk.Layer | None:
    if flows_df is None or flows_df.empty:
        return None

    lookup_ll = centroids_lonlat(zones_lookup)
    f = flows_df.copy()
    f["flow_volume"] = pd.to_numeric(f["flow_volume"], errors="coerce").fillna(0.0)
    f = f[f["flow_volume"] > 0]

    f = f.merge(
        lookup_ll[["transport_zone_id", "lon", "lat"]],
        left_on="from", right_on="transport_zone_id", how="left"
    ).rename(columns={"lon": "lon_from", "lat": "lat_from"}).drop(columns=["transport_zone_id"])
    f = f.merge(
        lookup_ll[["transport_zone_id", "lon", "lat"]],
        left_on="to", right_on="transport_zone_id", how="left"
    ).rename(columns={"lon": "lon_to", "lat": "lat_to"}).drop(columns=["transport_zone_id"])
    f = f.dropna(subset=["lon_from", "lat_from", "lon_to", "lat_to"])
    if f.empty:
        return None

    f["flow_width"] = (1.0 + np.log1p(f["flow_volume"])).astype("float64").clip(0.5, 6.0)

    return pdk.Layer(
        "ArcLayer",
        data=f,
        get_source_position=["lon_from", "lat_from"],
        get_target_position=["lon_to", "lat_to"],
        get_source_color=[255, 140, 0, 180],
        get_target_color=[0, 128, 255, 180],
        get_width="flow_width",
        pickable=True,
    )
