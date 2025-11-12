from __future__ import annotations
from functools import lru_cache
from typing import Dict, Any, Tuple
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

# ------------------------------------------------------------
# Helpers & fallback
# ------------------------------------------------------------
def _to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(4326, allow_override=True)
    try:
        epsg = gdf.crs.to_epsg()
    except Exception:
        epsg = None
    return gdf if epsg == 4326 else gdf.to_crs(4326)


def _fallback_scenario() -> Dict[str, Any]:
    """Scénario de secours (Toulouse–Blagnac) avec toutes les colonnes de parts (y compris TC)."""
    toulouse = (1.4442, 43.6047)
    blagnac = (1.3903, 43.6350)

    pts = gpd.GeoDataFrame(
        {"transport_zone_id": ["toulouse", "blagnac"], "geometry": [Point(*toulouse), Point(*blagnac)]},
        geometry="geometry",
        crs=4326,
    )

    zones = pts.to_crs(3857)
    zones["geometry"] = zones.geometry.buffer(5000)  # 5 km
    zones = zones.to_crs(4326)

    zones["average_travel_time"] = [18.0, 25.0]
    zones["total_dist_km"] = [15.0, 22.0]

    # parts modales "plausibles"
    car_tlse, bike_tlse, walk_tlse = 0.55, 0.19, 0.16
    ptw_tlse, ptc_tlse, ptb_tlse = 0.06, 0.03, 0.02  # sous-modes TC
    carpool_tlse = 0.05

    car_blg,  bike_blg,  walk_blg  = 0.50, 0.20, 0.15
    ptw_blg,  ptc_blg,  ptb_blg   = 0.08, 0.04, 0.03
    carpool_blg = 0.00

    zones["share_car"] = [car_tlse, car_blg]
    zones["share_bicycle"] = [bike_tlse, bike_blg]
    zones["share_walk"] = [walk_tlse, walk_blg]
    zones["share_carpool"] = [carpool_tlse, carpool_blg]

    zones["share_pt_walk"] = [ptw_tlse, ptw_blg]
    zones["share_pt_car"] = [ptc_tlse, ptc_blg]
    zones["share_pt_bicycle"] = [ptb_tlse, ptb_blg]
    zones["share_public_transport"] = zones[["share_pt_walk", "share_pt_car", "share_pt_bicycle"]].sum(axis=1)

    # normalisation pour s’assurer que la somme = 1
    cols_all = [
        "share_car", "share_bicycle", "share_walk", "share_carpool",
        "share_pt_walk", "share_pt_car", "share_pt_bicycle"
    ]
    total = zones[cols_all].sum(axis=1)
    zones[cols_all] = zones[cols_all].div(total.replace(0, np.nan), axis=0).fillna(0)
    zones["share_public_transport"] = zones[["share_pt_walk", "share_pt_car", "share_pt_bicycle"]].sum(axis=1)

    zones["local_admin_unit_id"] = ["fr-31555", "fr-31069"]

    empty_flows = pd.DataFrame(columns=["from", "to", "flow_volume"])
    return {"zones_gdf": _to_wgs84(zones), "flows_df": empty_flows, "zones_lookup": _to_wgs84(pts)}


def _normalize_lau_code(code: str) -> str:
    s = str(code).strip().lower()
    if s.startswith("fr-"):
        return s
    if s.isdigit() and len(s) == 5:
        return f"fr-{s}"
    return s


# ------------------------------------------------------------
# Param helpers
# ------------------------------------------------------------
def _safe_cost_of_time(v_per_hour: float):
    # On garde la présence de cette fonction pour compatibilité,
    # mais on n’instancie pas de modèles lourds ici.
    class _COT:
        def __init__(self, v): self.value_per_hour = float(v)
    return _COT(v_per_hour)


def _extract_vars(d: Dict[str, Any], defaults: Dict[str, float]) -> Dict[str, float]:
    """Récupère cost_constant / cost_of_time_eur_per_h / cost_of_distance_eur_per_km avec défauts."""
    return {
        "cost_constant": float((d or {}).get("cost_constant", defaults["cost_constant"])),
        "cost_of_time_eur_per_h": float((d or {}).get("cost_of_time_eur_per_h", defaults["cost_of_time_eur_per_h"])),
        "cost_of_distance_eur_per_km": float((d or {}).get("cost_of_distance_eur_per_km", defaults["cost_of_distance_eur_per_km"])),
    }


def _mode_cost_to_weight(vars_: Dict[str, float], base_minutes: float) -> float:
    """
    Convertit les variables de coût d’un mode en un poids temps synthétique (minutes).
    Plus les coûts sont élevés, plus le "poids" est haut (=> augmente average_travel_time si la part du mode est forte).
    On garde une transformation simple, stable et déterministe.
    """
    cc = vars_["cost_constant"]                # €
    cot = vars_["cost_of_time_eur_per_h"]      # €/h
    cod = vars_["cost_of_distance_eur_per_km"] # €/km

    # pondérations simples mais sensibles :
    # - le coût du temps influe beaucoup (rapport heures→minutes)
    # - la distance influe modérément
    # - la constante donne un petit offset
    return (
        base_minutes
        + 0.6 * (cot)          # €/h → ~impact direct
        + 4.0 * (cod)          # €/km → faible
        + 0.8 * (cc)           # €
    )


# ------------------------------------------------------------
# Core computation (robuste aux modes manquants)
# ------------------------------------------------------------
def _compute_scenario(
    local_admin_unit_id: str = "31555",
    radius: float = 40.0,
    transport_modes_params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Calcule un scénario. Crée toujours toutes les colonnes de parts :
    - share_car, share_bicycle, share_walk, share_carpool
    - share_pt_walk, share_pt_car, share_pt_bicycle, share_public_transport
    Renormalise sur les seuls modes actifs (zéro si décoché).
    Recalcule average_travel_time (Option B) avec influence des variables par mode.
    """
    try:
        import mobility
    except Exception as e:
        print(f"[SCENARIO] fallback (mobility indisponible): {e}")
        return _fallback_scenario()

    p = transport_modes_params or {}
    # états d’activation des modes principaux
    active = {
        "car": bool(p.get("car", {}).get("active", True)),
        "bicycle": bool(p.get("bicycle", {}).get("active", True)),
        "walk": bool(p.get("walk", {}).get("active", True)),
        "carpool": bool(p.get("carpool", {}).get("active", True)),
        "public_transport": bool(p.get("public_transport", {}).get("active", True)),
    }
    # états des sous-modes TC
    pt_sub = {
        "walk_pt": bool((p.get("public_transport", {}) or {}).get("pt_walk", True)),
        "car_pt": bool((p.get("public_transport", {}) or {}).get("pt_car", True)),
        "bicycle_pt": bool((p.get("public_transport", {}) or {}).get("pt_bicycle", True)),
    }

    # Variables des modes (avec défauts souhaités : 12€/h ; 0.01€/km ; 1€)
    defaults = {"cost_constant": 1.0, "cost_of_time_eur_per_h": 12.0, "cost_of_distance_eur_per_km": 0.01}
    vars_car      = _extract_vars(p.get("car"), defaults)
    vars_bicycle  = _extract_vars(p.get("bicycle"), defaults)
    vars_walk     = _extract_vars(p.get("walk"), defaults)
    vars_carpool  = _extract_vars(p.get("carpool"), defaults)
    vars_pt       = _extract_vars(p.get("public_transport"), defaults)  # appliqué au bloc TC

    # Zones issues de mobility (géométrie réaliste) — sans lancer de modèles
    lau_norm = _normalize_lau_code(local_admin_unit_id or "31555")
    mobility.set_params(debug=True, r_packages_download_method="wininet")
    tz = mobility.TransportZones(local_admin_unit_id=lau_norm, radius=float(radius), level_of_detail=0)

    zones = tz.get()[["transport_zone_id", "geometry", "local_admin_unit_id"]].copy()
    zones_gdf = gpd.GeoDataFrame(zones, geometry="geometry")
    n = len(zones_gdf)

    # --- Initialisation TOUTES parts à 0
    for col in [
        "share_car", "share_bicycle", "share_walk", "share_carpool",
        "share_pt_walk", "share_pt_car", "share_pt_bicycle", "share_public_transport"
    ]:
        zones_gdf[col] = 0.0

    # --- Assigner des parts uniquement pour ce qui est actif
    rng = np.random.default_rng(42)
    if active["car"]:
        zones_gdf["share_car"] = rng.uniform(0.25, 0.65, n)
    if active["bicycle"]:
        zones_gdf["share_bicycle"] = rng.uniform(0.05, 0.25, n)
    if active["walk"]:
        zones_gdf["share_walk"] = rng.uniform(0.05, 0.30, n)
    if active["carpool"]:
        zones_gdf["share_carpool"] = rng.uniform(0.03, 0.20, n)

    if active["public_transport"]:
        if pt_sub["walk_pt"]:
            zones_gdf["share_pt_walk"] = rng.uniform(0.03, 0.15, n)
        if pt_sub["car_pt"]:
            zones_gdf["share_pt_car"] = rng.uniform(0.02, 0.12, n)
        if pt_sub["bicycle_pt"]:
            zones_gdf["share_pt_bicycle"] = rng.uniform(0.01, 0.08, n)
        zones_gdf["share_public_transport"] = zones_gdf[["share_pt_walk", "share_pt_car", "share_pt_bicycle"]].sum(axis=1)

    # --- Renormalisation : uniquement sur les colonnes présentes/actives
    cols_all = [
        "share_car", "share_bicycle", "share_walk", "share_carpool",
        "share_pt_walk", "share_pt_car", "share_pt_bicycle"
    ]
    active_cols = []
    if active["car"]: active_cols.append("share_car")
    if active["bicycle"]: active_cols.append("share_bicycle")
    if active["walk"]: active_cols.append("share_walk")
    if active["carpool"]: active_cols.append("share_carpool")
    if active["public_transport"] and pt_sub["walk_pt"]: active_cols.append("share_pt_walk")
    if active["public_transport"] and pt_sub["car_pt"]: active_cols.append("share_pt_car")
    if active["public_transport"] and pt_sub["bicycle_pt"]: active_cols.append("share_pt_bicycle")

    if not active_cols:
        # Rien d'actif → fallback
        return _fallback_scenario()

    total = zones_gdf[active_cols].sum(axis=1).replace(0, np.nan)
    for col in cols_all:
        if col in zones_gdf.columns:
            zones_gdf[col] = zones_gdf[col] / total
    zones_gdf = zones_gdf.fillna(0.0)
    zones_gdf["share_public_transport"] = zones_gdf[["share_pt_walk", "share_pt_car", "share_pt_bicycle"]].sum(axis=1)

    # --- Recalcul average_travel_time (Option B) sensible aux variables
    # bases minutes (sans variables)
    base_minutes = {
        "car": 20.0, "bicycle": 15.0, "walk": 25.0, "carpool": 18.0, "public_transport": 22.0
    }
    W = {
        "car": _mode_cost_to_weight(vars_car, base_minutes["car"]),
        "bicycle": _mode_cost_to_weight(vars_bicycle, base_minutes["bicycle"]),
        "walk": _mode_cost_to_weight(vars_walk, base_minutes["walk"]),
        "carpool": _mode_cost_to_weight(vars_carpool, base_minutes["carpool"]),
        "public_transport": _mode_cost_to_weight(vars_pt, base_minutes["public_transport"]),
    }
    zones_gdf["average_travel_time"] = (
        zones_gdf["share_car"] * W["car"]
        + zones_gdf["share_bicycle"] * W["bicycle"]
        + zones_gdf["share_walk"] * W["walk"]
        + zones_gdf["share_carpool"] * W["carpool"]
        + zones_gdf["share_public_transport"] * W["public_transport"]
    )

    # --- Autres indicateurs synthétiques
    zones_gdf["total_dist_km"] = 10.0 + 10.0 * rng.random(n)

    # Types cohérents & WGS84
    zones_gdf["transport_zone_id"] = zones_gdf["transport_zone_id"].astype(str)
    zones_lookup = gpd.GeoDataFrame(
        zones[["transport_zone_id", "geometry"]].astype({"transport_zone_id": str}),
        geometry="geometry",
        crs=zones_gdf.crs,
    )

    return {
        "zones_gdf": _to_wgs84(zones_gdf),
        "flows_df": pd.DataFrame(columns=["from", "to", "flow_volume"]),
        "zones_lookup": _to_wgs84(zones_lookup),
    }


# ------------------------------------------------------------
#  API public + cache
# ------------------------------------------------------------
def _normalized_key(local_admin_unit_id: str, radius: float) -> Tuple[str, float]:
    lau = _normalize_lau_code(local_admin_unit_id or "31555")
    rad = round(float(radius), 4)
    return (lau, rad)

@lru_cache(maxsize=8)
def _get_scenario_cached(lau: str, rad: float) -> Dict[str, Any]:
    return _compute_scenario(local_admin_unit_id=lau, radius=rad, transport_modes_params=None)

def get_scenario(
    local_admin_unit_id: str = "31555",
    radius: float = 40.0,
    transport_modes_params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    lau, rad = _normalized_key(local_admin_unit_id, radius)
    if not transport_modes_params:
        return _get_scenario_cached(lau, rad)
    return _compute_scenario(local_admin_unit_id=lau, radius=rad, transport_modes_params=transport_modes_params)

def clear_scenario_cache() -> None:
    _get_scenario_cached.cache_clear()
