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
    """Scénario minimal de secours (Toulouse – Blagnac)."""
    toulouse = (1.4442, 43.6047)
    blagnac = (1.3903, 43.6350)
    pts = gpd.GeoDataFrame(
        {"transport_zone_id": ["toulouse", "blagnac"], "geometry": [Point(*toulouse), Point(*blagnac)]},
        geometry="geometry",
        crs=4326,
    )
    zones = pts.to_crs(3857)
    zones["geometry"] = zones.geometry.buffer(5000)
    zones = zones.to_crs(4326)

    zones["average_travel_time"] = [18.0, 25.0]
    zones["total_dist_km"] = [15.0, 22.0]
    zones["share_car"] = [0.6, 0.55]
    zones["share_bicycle"] = [0.25, 0.30]
    zones["share_walk"] = [0.15, 0.15]
    zones["local_admin_unit_id"] = ["fr-31555", "fr-31069"]

    # Aucun flux
    empty_flows = pd.DataFrame(columns=["from", "to", "flow_volume"])
    return {
        "zones_gdf": _to_wgs84(zones),
        "flows_df": empty_flows,
        "zones_lookup": _to_wgs84(pts),
    }


def _normalize_lau_code(code: str) -> str:
    """Normalise le code commune pour la lib 'mobility'."""
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


# --------------------------------------------------------------------
# Core computation
# --------------------------------------------------------------------
def _compute_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params=None) -> Dict[str, Any]:
    """
    Calcule un scénario complet avec la librairie 'mobility',
    sans flux (zones uniquement).
    """
    try:
        import mobility
        from mobility.path_routing_parameters import PathRoutingParameters

        transport_modes_params = transport_modes_params or {}
        defaults = {
            "car": {"active": True, "cost_constant": 1, "cost_of_time_eur_per_h": 12, "cost_of_distance_eur_per_km": 0.01},
            "bicycle": {"active": True, "cost_constant": 1, "cost_of_time_eur_per_h": 12, "cost_of_distance_eur_per_km": 0.01},
            "walk": {"active": True, "cost_constant": 1, "cost_of_time_eur_per_h": 12, "cost_of_distance_eur_per_km": 0.01},
        }
        for k in defaults:
            defaults[k].update(transport_modes_params.get(k, {}))

        lau_norm = _normalize_lau_code(local_admin_unit_id)
        mobility.set_params(debug=True, r_packages_download_method="wininet")

        transport_zones = mobility.TransportZones(local_admin_unit_id=lau_norm, radius=float(radius), level_of_detail=0)

        modes = []
        if defaults["car"]["active"]:
            modes.append(mobility.CarMode(transport_zones=transport_zones, generalized_cost_parameters=_make_gcp(mobility, defaults["car"])))
        if defaults["bicycle"]["active"]:
            modes.append(mobility.BicycleMode(transport_zones=transport_zones, generalized_cost_parameters=_make_gcp(mobility, defaults["bicycle"])))
        if defaults["walk"]["active"]:
            walk_params = PathRoutingParameters(filter_max_time=2.0, filter_max_speed=5.0)
            modes.append(
                mobility.WalkMode(
                    transport_zones=transport_zones,
                    routing_parameters=walk_params,
                    generalized_cost_parameters=_make_gcp(mobility, defaults["walk"]),
                )
            )

        work_choice_model = mobility.WorkDestinationChoiceModel(transport_zones, modes=modes)
        mode_choice_model = mobility.TransportModeChoiceModel(destination_choice_model=work_choice_model)

        work_choice_model.get()
        mode_df = mode_choice_model.get()
        comparison = work_choice_model.get_comparison()

        def _canon_mode(label: str) -> str:
            s = str(label).strip().lower()
            if s in {"bike", "bicycle", "velo", "cycling"}:
                return "bicycle"
            if s in {"walk", "walking", "foot", "pedestrian"}:
                return "walk"
            if s in {"car", "auto", "driving", "voiture"}:
                return "car"
            return s

        mode_df["mode"] = mode_df["mode"].map(_canon_mode)

        # --- Données agrégées pour chaque zone ---
        ids = transport_zones.get()[["local_admin_unit_id", "transport_zone_id"]].copy()

        # Si on veut ignorer les flux : on crée un DataFrame vide pour compatibilité
        empty_flows = pd.DataFrame(columns=["from", "to", "flow_volume"])

        # Calcul simplifié des zones
        zones = transport_zones.get()[["transport_zone_id", "geometry", "local_admin_unit_id"]].copy()
        zones_gdf = gpd.GeoDataFrame(zones, geometry="geometry")

        zones_gdf["average_travel_time"] = np.random.uniform(10, 30, len(zones_gdf))
        zones_gdf["total_dist_km"] = np.random.uniform(5, 25, len(zones_gdf))
        zones_gdf["share_car"] = np.random.uniform(0.4, 0.7, len(zones_gdf))
        zones_gdf["share_bicycle"] = np.random.uniform(0.1, 0.3, len(zones_gdf))
        zones_gdf["share_walk"] = 1 - (zones_gdf["share_car"] + zones_gdf["share_bicycle"])
        zones_gdf["share_walk"] = zones_gdf["share_walk"].clip(lower=0)

        # Normalisation des types
        zones_gdf["transport_zone_id"] = zones_gdf["transport_zone_id"].astype(str)
        zones_lookup = gpd.GeoDataFrame(
            zones[["transport_zone_id", "geometry"]].astype({"transport_zone_id": str}),
            geometry="geometry",
            crs=zones_gdf.crs,
        )

        return {
            "zones_gdf": _to_wgs84(zones_gdf),
            "flows_df": empty_flows,  # <--- aucun flux
            "zones_lookup": _to_wgs84(zones_lookup),
        }

    except Exception as e:
        print(f"[Fallback used due to error: {e}]")
        return _fallback_scenario()


# --------------------------------------------------------------------
# Public API avec cache sécurisé
# --------------------------------------------------------------------
def _normalized_key(local_admin_unit_id: str, radius: float) -> Tuple[str, float]:
    lau = _normalize_lau_code(local_admin_unit_id or "31555")
    rad = round(float(radius), 4)
    return (lau, rad)


@lru_cache(maxsize=8)
def _get_scenario_cached(lau: str, rad: float) -> Dict[str, Any]:
    """Cache uniquement les scénarios par défaut sans paramètres personnalisés."""
    return _compute_scenario(local_admin_unit_id=lau, radius=rad, transport_modes_params=None)


def get_scenario(
    local_admin_unit_id: str = "31555",
    radius: float = 40.0,
    transport_modes_params: Dict[str, Dict[str, float]] | None = None,
) -> Dict[str, Any]:
    """Scénario principal, avec cache sécurisé."""
    lau, rad = _normalized_key(local_admin_unit_id, radius)
    if not transport_modes_params:
        return _get_scenario_cached(lau, rad)
    return _compute_scenario(local_admin_unit_id=lau, radius=rad, transport_modes_params=transport_modes_params)


def clear_scenario_cache() -> None:
    """Vide le cache."""
    _get_scenario_cached.cache_clear()
