import logging
from typing import Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon

logger = logging.getLogger(__name__)

def ensure_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assure EPSG:4326 pour la sortie."""
    g = gdf.copy()
    if g.crs is None:
        g = g.set_crs(4326, allow_override=True)
    elif getattr(g.crs, "to_epsg", lambda: None)() != 4326:
        g = g.to_crs(4326)
    return g

def centroids_lonlat(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ajoute colonnes lon/lat calculées en mètres (EPSG:3857) puis reprojetées en 4326."""
    g = gdf.copy()
    if g.crs is None:
        g = g.set_crs(4326, allow_override=True)
    g_m = g.to_crs(3857)
    pts_m = g_m.geometry.centroid
    pts_ll = gpd.GeoSeries(pts_m, crs=g_m.crs).to_crs(4326)
    g["lon"] = pts_ll.x.astype("float64")
    g["lat"] = pts_ll.y.astype("float64")
    return g

def safe_center(gdf: gpd.GeoDataFrame) -> Optional[Tuple[float, float]]:
    """Calcule un centroïde global robuste en WGS84, sinon None."""
    try:
        zvalid = gdf[gdf.geometry.notnull() & gdf.geometry.is_valid]
        if zvalid.empty:
            return None
        centroid = ensure_wgs84(zvalid).geometry.unary_union.centroid
        return float(centroid.x), float(centroid.y)
    except Exception as e:
        logger.warning("safe_center failed: %s", e)
        return None

def fmt_num(v, nd=1):
    try:
        return f"{round(float(v), nd):.{nd}f}"
    except Exception:
        return "N/A"

def fmt_pct(v, nd=1):
    try:
        return f"{round(float(v) * 100.0, nd):.{nd}f} %"
    except Exception:
        return "N/A"

def as_polygon_rings(geom):
    """Retourne les anneaux extérieurs d’un Polygon/MultiPolygon sous forme de liste de coordonnées."""
    if isinstance(geom, Polygon):
        return [list(geom.exterior.coords)]
    if isinstance(geom, MultiPolygon):
        return [list(p.exterior.coords) for p in geom.geoms]
    return []
