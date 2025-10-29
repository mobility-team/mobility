# app/services/scenario_service.py
from __future__ import annotations
from functools import lru_cache
from typing import Dict, Any, Tuple
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

# --------------------------------------------------------------------
# Helpers & fallback
# --------------------------------------------------------------------
def _to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(4326, allow_override=True)
    try:
        epsg = gdf.crs.to_epsg()
    except Exception:
        epsg = None
    return gdf if epsg == 4326 else gdf.to_crs(4326)


def _fallback_scenario() -> Dict[str, Any]:
    """Scénario minimal de secours (Toulouse – Blagnac) AVEC covoiturage."""
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
    zones["share_car"] = [0.55, 0.50]
    zones["share_bicycle"] = [0.19, 0.20]
    zones["share_walk"] = [0.16, 0.15]
    zones["share_carpool"] = [0.10, 0.15]
    zones["local_admin_unit_id"] = ["fr-31555", "fr-31069"]

    return {
        "zones_gdf": _to_wgs84(zones),
        "flows_df": pd.DataFrame(columns=["from", "to", "flow_volume"]),
        "zones_lookup": _to_wgs84(pts),
    }


def _normalize_lau_code(code: str) -> str:
    s = str(code).strip().lower()
    if s.startswith("fr-"):
        return s
    if s.isdigit() and len(s) == 5:
        return f"fr-{s}"
    return s


# --------------------------------------------------------------------
# Helpers pour paramètres personnalisés
# --------------------------------------------------------------------
def _safe_cost_of_time(v_per_hour: float):
    from mobility.cost_of_time_parameters import CostOfTimeParameters
    cot = CostOfTimeParameters()
    for attr in ("value_per_hour", "hourly_cost", "value"):
        if hasattr(cot, attr):
            setattr(cot, attr, float(v_per_hour))
            return cot
    return cot


def _make_gcp(mobility, params: dict):
    return mobility.GeneralizedCostParameters(
        cost_constant=float(params.get("cost_constant", 0.0)),
        cost_of_time=_safe_cost_of_time(params.get("cost_of_time_eur_per_h", 0.0)),
        cost_of_distance=float(params.get("cost_of_distance_eur_per_km", 0.0)),
    )


def _make_carpool_gcp(params: dict):
    from mobility.transport_modes.carpool.detailed import DetailedCarpoolGeneralizedCostParameters
    return DetailedCarpoolGeneralizedCostParameters(
        cost_constant=float(params.get("cost_constant", 0.0)),
        cost_of_time=_safe_cost_of_time(params.get("cost_of_time_eur_per_h", 0.0)),
        cost_of_distance=float(params.get("cost_of_distance_eur_per_km", 0.0)),
    )


# --------------------------------------------------------------------
# Core computation
# --------------------------------------------------------------------
def _compute_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params=None) -> Dict[str, Any]:
    """
    Calcule un scénario avec la librairie 'mobility'.
    - Gère 'car', 'bicycle', 'walk' + 'carpool' (via mobility.CarpoolMode)
    - Les modes décochés gardent part = 0 ; renormalisation sur les modes actifs
    - Pas de flux (flows_df vide)
    """
    # Import critiques : si 'mobility' absent → fallback
    try:
        import mobility
        from mobility.path_routing_parameters import PathRoutingParameters
        from mobility.transport_modes.carpool.carpool_mode import CarpoolMode
        from mobility.transport_modes.carpool.detailed import DetailedCarpoolRoutingParameters
    except Exception as e:
        print(f"[SCENARIO] mobility indisponible → fallback. Raison: {e}")
        return _fallback_scenario()

    transport_modes_params = transport_modes_params or {}

    BASE = {
        "active": False,
        "cost_constant": 1,
        "cost_of_time_eur_per_h": 12,
        "cost_of_distance_eur_per_km": 0.01,
    }

    if not transport_modes_params:
        # Comportement historique : 3 modes actifs, carpool inactif
        modes_cfg = {
            "car":       {**BASE, "active": True},
            "bicycle":   {**BASE, "active": True},
            "walk":      {**BASE, "active": True},
            "carpool":   {**BASE, "active": True},
        }
    else:
        modes_cfg = {}
        for k in ("car", "bicycle", "walk", "carpool"):
            if k in transport_modes_params:
                user = transport_modes_params[k] or {}
                cfg = {**BASE, **{kk: vv for kk, vv in user.items() if vv is not None}}
                if "active" not in user:
                    cfg["active"] = True
                modes_cfg[k] = cfg

    lau_norm = _normalize_lau_code(local_admin_unit_id)
    mobility.set_params(debug=True, r_packages_download_method="wininet")
    transport_zones = mobility.TransportZones(local_admin_unit_id=lau_norm, radius=float(radius), level_of_detail=0)

    # Instanciation des modes (pas de try global pour éviter fallback intempestif)
    modes = []

    # Car (base) — requis par CarpoolMode
    car_base = mobility.CarMode(
        transport_zones=transport_zones,
        generalized_cost_parameters=_make_gcp(mobility, modes_cfg.get("car", BASE)),
    )
    if modes_cfg.get("car", {}).get("active"):
        modes.append(car_base)

    # Bicycle
    if modes_cfg.get("bicycle", {}).get("active"):
        modes.append(
            mobility.BicycleMode(
                transport_zones=transport_zones,
                generalized_cost_parameters=_make_gcp(mobility, modes_cfg["bicycle"]),
            )
        )

    # Walk
    if modes_cfg.get("walk", {}).get("active"):
        walk_params = PathRoutingParameters(filter_max_time=2.0, filter_max_speed=5.0)
        modes.append(
            mobility.WalkMode(
                transport_zones=transport_zones,
                routing_parameters=walk_params,
                generalized_cost_parameters=_make_gcp(mobility, modes_cfg["walk"]),
            )
        )

    # Carpool (covoiturage) — on essaie, et si ça échoue on continue sans casser la simu
    if modes_cfg.get("carpool", {}).get("active"):
        try:
            routing_params = DetailedCarpoolRoutingParameters()
            gcp_carpool = _make_carpool_gcp(modes_cfg["carpool"])
            modes.append(
                CarpoolMode(
                    car_mode=car_base,
                    routing_parameters=routing_params,
                    generalized_cost_parameters=gcp_carpool,
                    intermodal_transfer=None,
                )
            )
            print("[SCENARIO] CarpoolMode activé.")
        except Exception as e:
            print(f"[SCENARIO] CarpoolMode indisponible, on continue sans : {e}")

    if not modes:
        raise ValueError("Aucun mode de transport actif. Activez au moins un mode.")

    # Calcul principal
    work_choice_model = mobility.WorkDestinationChoiceModel(transport_zones, modes=modes)
    work_choice_model.get()

    zones = transport_zones.get()[["transport_zone_id", "geometry", "local_admin_unit_id"]].copy()
    zones_gdf = gpd.GeoDataFrame(zones, geometry="geometry")

    # Indicateurs génériques (mock cohérents)
    zones_gdf["average_travel_time"] = np.random.uniform(10, 30, len(zones_gdf))
    zones_gdf["total_dist_km"] = np.random.uniform(5, 25, len(zones_gdf))

    # Parts modales initialisées à 0
    zones_gdf["share_car"] = 0.0
    zones_gdf["share_bicycle"] = 0.0
    zones_gdf["share_walk"] = 0.0
    zones_gdf["share_carpool"] = 0.0

    # Assigner des parts seulement pour les modes actifs (puis renormaliser)
    if modes_cfg.get("car", {}).get("active"):
        zones_gdf["share_car"] = np.random.uniform(0.35, 0.7, len(zones_gdf))
    if modes_cfg.get("bicycle", {}).get("active"):
        zones_gdf["share_bicycle"] = np.random.uniform(0.05, 0.3, len(zones_gdf))
    if modes_cfg.get("walk", {}).get("active"):
        zones_gdf["share_walk"] = np.random.uniform(0.05, 0.3, len(zones_gdf))
    if modes_cfg.get("carpool", {}).get("active"):
        zones_gdf["share_carpool"] = np.random.uniform(0.05, 0.25, len(zones_gdf))

    # Renormalisation ligne à ligne sur les modes actifs
    cols = ["share_car", "share_bicycle", "share_walk", "share_carpool"]
    total = zones_gdf[cols].sum(axis=1)
    nonzero = total.replace(0, np.nan)
    for col in cols:
        zones_gdf[col] = zones_gdf[col] / nonzero
    zones_gdf = zones_gdf.fillna(0.0)

    # Types/CRS cohérents
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


# --------------------------------------------------------------------
# Public API avec cache (inchangé)
# --------------------------------------------------------------------
def _normalized_key(local_admin_unit_id: str, radius: float) -> Tuple[str, float]:
    lau = _normalize_lau_code(local_admin_unit_id or "31555")
    rad = round(float(radius if radius is not None else 40.0), 4)
    return (lau, rad)


@lru_cache(maxsize=8)
def _get_scenario_cached(lau: str, rad: float) -> Dict[str, Any]:
    # Cache uniquement quand aucun paramètre UI n'est passé
    return _compute_scenario(local_admin_unit_id=lau, radius=rad, transport_modes_params=None)


def get_scenario(
    local_admin_unit_id: str = "31555",
    radius: float = 40.0,
    transport_modes_params: Dict[str, Dict[str, float]] | None = None,
) -> Dict[str, Any]:
    lau, rad = _normalized_key(local_admin_unit_id, radius)
    if not transport_modes_params:
        return _get_scenario_cached(lau, rad)
    # Avec paramètres UI → recalcul sans cache pour refléter les choix
    return _compute_scenario(local_admin_unit_id=lau, radius=rad, transport_modes_params=transport_modes_params)


def clear_scenario_cache() -> None:
    _get_scenario_cached.cache_clear()
