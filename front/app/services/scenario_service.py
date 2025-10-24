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
    """
    Scénario minimal de secours (Paris–Lyon), utile si la lib échoue.
    """
    paris = (2.3522, 48.8566)
    lyon = (4.8357, 45.7640)

    pts = gpd.GeoDataFrame(
        {"transport_zone_id": ["paris", "lyon"], "geometry": [Point(*paris), Point(*lyon)]},
        geometry="geometry", crs=4326
    )
    zones = pts.to_crs(3857)
    zones["geometry"] = zones.geometry.buffer(5000)  # 5 km
    zones = zones.to_crs(4326)

    # Indicateurs d'exemple (minutes, km/personne/jour)
    zones["average_travel_time"] = [18.0, 25.0]
    zones["total_dist_km"] = [15.0, 22.0]
    zones["share_car"] = [0.6, 0.55]
    zones["share_bicycle"] = [0.25, 0.30]
    zones["share_walk"] = [0.15, 0.15]
    zones["local_admin_unit_id"] = ["N/A", "N/A"]

    flows_df = pd.DataFrame({"from": ["lyon"], "to": ["paris"], "flow_volume": [120.0]})

    return {
        "zones_gdf": _to_wgs84(zones),
        "flows_df": flows_df,
        "zones_lookup": _to_wgs84(pts),
    }


def _normalize_lau_code(code: str) -> str:
    """
    Normalise le code commune pour la lib 'mobility' :
      - '31555'  -> 'fr-31555'
      - 'fr-31555' -> inchangé
      - sinon -> str(trim)
    """
    s = str(code).strip().lower()
    if s.startswith("fr-"):
        return s
    if s.isdigit() and len(s) == 5:
        return f"fr-{s}"
    return s


# --------------------------------------------------------------------
# Core computation (extracted & hardened)
# --------------------------------------------------------------------
def _compute_scenario(local_admin_unit_id: str = "31555", radius: float = 40.0) -> Dict[str, Any]:
    """
    Calcule un scénario pour une commune (INSEE/LAU) et un rayon (km).
    Retourne un dict: { zones_gdf, flows_df, zones_lookup } en WGS84.
    """
    try:
        import mobility
        from mobility.path_routing_parameters import PathRoutingParameters

        mobility.set_params(debug=True, r_packages_download_method="wininet")

        # Normalise le code pour la lib
        lau_norm = _normalize_lau_code(local_admin_unit_id)

        # Certaines versions exigent cache_path=None; d'autres non.
        def _safe_instantiate(cls, *args, **kwargs):
            try:
                return cls(*args, **kwargs)
            except TypeError as e:
                if "missing 1 required positional argument: 'cache_path'" in str(e):
                    return cls(*args, cache_path=None, **kwargs)
                raise

        # --- Transport zones (study area) ---
        transport_zones = _safe_instantiate(
            mobility.TransportZones,
            local_admin_unit_id=lau_norm,   # e.g., "fr-31555"
            radius=float(radius),
            level_of_detail=0,
        )

        # --- Modes ---
        car = _safe_instantiate(
            mobility.CarMode,
            transport_zones=transport_zones,
            generalized_cost_parameters=mobility.GeneralizedCostParameters(cost_of_distance=0.1),
        )
        bicycle = _safe_instantiate(
            mobility.BicycleMode,
            transport_zones=transport_zones,
            generalized_cost_parameters=mobility.GeneralizedCostParameters(cost_of_distance=0.0),
        )

        # Walk mode: nom variable selon version
        walk = None
        for cls_name in ("WalkMode", "PedestrianMode", "WalkingMode", "Pedestrian"):
            if walk is None and hasattr(mobility, cls_name):
                walk_params = PathRoutingParameters(filter_max_time=2.0, filter_max_speed=5.0)
                walk = _safe_instantiate(
                    getattr(mobility, cls_name),
                    transport_zones=transport_zones,
                    routing_parameters=walk_params,
                    generalized_cost_parameters=mobility.GeneralizedCostParameters(cost_of_distance=0.0),
                )

        modes = [m for m in (car, bicycle, walk) if m is not None]

        # --- Models ---
        work_choice_model = _safe_instantiate(mobility.WorkDestinationChoiceModel, transport_zones, modes=modes)
        mode_choice_model = _safe_instantiate(
            mobility.TransportModeChoiceModel, destination_choice_model=work_choice_model
        )

        # Fetch results
        work_choice_model.get()
        mode_df = mode_choice_model.get()              # columns: from, to, mode, prob
        comparison = work_choice_model.get_comparison()

        # Canonicalise les labels de modes
        def _canon_mode(label: str) -> str:
            s = str(label).strip().lower()
            if s in {"bike", "bicycle", "velo", "cycling"}:
                return "bicycle"
            if s in {"walk", "walking", "foot", "pedestrian", "pedestrianmode"}:
                return "walk"
            if s in {"car", "auto", "driving", "voiture"}:
                return "car"
            return s

        if "mode" in mode_df.columns:
            mode_df["mode"] = mode_df["mode"].map(_canon_mode)

        # Travel costs by mode
        def _get_costs(m, label):
            df = m.travel_costs.get().copy()
            df["mode"] = label
            return df

        costs_list = [_get_costs(car, "car"), _get_costs(bicycle, "bicycle")]
        if walk is not None:
            costs_list.append(_get_costs(walk, "walk"))

        travel_costs = pd.concat(costs_list, ignore_index=True)
        travel_costs["mode"] = travel_costs["mode"].map(_canon_mode)

        # Normalisation des unités
        if "time" in travel_costs.columns:
            t_hours = pd.to_numeric(travel_costs["time"], errors="coerce")
            travel_costs["time_min"] = t_hours * 60.0
        else:
            travel_costs["time_min"] = np.nan

        if "distance" in travel_costs.columns:
            d_raw = pd.to_numeric(travel_costs["distance"], errors="coerce")
            d_max = d_raw.replace([np.inf, -np.inf], np.nan).max()
            travel_costs["dist_km"] = d_raw / 1000.0 if (pd.notna(d_max) and d_max > 200) else d_raw
        else:
            travel_costs["dist_km"] = np.nan

        # ID joins
        ids = transport_zones.get()[["local_admin_unit_id", "transport_zone_id"]].copy()

        ori_dest_counts = (
            comparison.merge(ids, left_on="local_admin_unit_id_from", right_on="local_admin_unit_id", how="left")
                      .merge(ids, left_on="local_admin_unit_id_to",   right_on="local_admin_unit_id", how="left")
                      [["transport_zone_id_x", "transport_zone_id_y", "flow_volume"]]
                      .rename(columns={"transport_zone_id_x": "from", "transport_zone_id_y": "to"})
        )
        ori_dest_counts["flow_volume"] = pd.to_numeric(ori_dest_counts["flow_volume"], errors="coerce").fillna(0.0)
        ori_dest_counts = ori_dest_counts[ori_dest_counts["flow_volume"] > 0]

        # Parts modales OD
        modal_shares = mode_df.merge(ori_dest_counts, on=["from", "to"], how="inner")
        modal_shares["prob"] = pd.to_numeric(modal_shares["prob"], errors="coerce").fillna(0.0)
        modal_shares["flow_volume"] *= modal_shares["prob"]

        # Join travel costs
        costs_cols = ["from", "to", "mode", "time_min", "dist_km"]
        available = [c for c in costs_cols if c in travel_costs.columns]
        travel_costs_norm = travel_costs[available].copy()

        od_mode = modal_shares.merge(travel_costs_norm, on=["from", "to", "mode"], how="left")
        od_mode["time_min"] = pd.to_numeric(od_mode.get("time_min", np.nan), errors="coerce")
        od_mode["dist_km"] = pd.to_numeric(od_mode.get("dist_km", np.nan), errors="coerce")

        # Agrégats par origine ("from")
        den = od_mode.groupby("from", as_index=True)["flow_volume"].sum().replace(0, np.nan)
        num_time  = (od_mode["time_min"] * od_mode["flow_volume"]).groupby(od_mode["from"]).sum(min_count=1)
        num_dist  = (od_mode["dist_km"] * od_mode["flow_volume"]).groupby(od_mode["from"]).sum(min_count=1)

        avg_time_min        = (num_time / den).rename("average_travel_time")
        per_person_dist_km  = (num_dist / den).rename("total_dist_km")

        mode_flow_by_from = od_mode.pivot_table(
            index="from", columns="mode", values="flow_volume", aggfunc="sum", fill_value=0.0
        )
        for col in ("car", "bicycle", "walk"):
            if col not in mode_flow_by_from.columns:
                mode_flow_by_from[col] = 0.0

        share_car     = (mode_flow_by_from["car"] / den).rename("share_car")
        share_bicycle = (mode_flow_by_from["bicycle"] / den).rename("share_bicycle")
        share_walk    = (mode_flow_by_from["walk"] / den).rename("share_walk")

        # Zones GeoDataFrame
        zones = transport_zones.get()[["transport_zone_id", "geometry", "local_admin_unit_id"]].copy()
        zones_gdf = gpd.GeoDataFrame(zones, geometry="geometry")

        agg = pd.concat(
            [avg_time_min, per_person_dist_km, share_car, share_bicycle, share_walk],
            axis=1
        ).reset_index().rename(columns={"from": "transport_zone_id"})

        zones_gdf = zones_gdf.merge(agg, on="transport_zone_id", how="left")
        zones_gdf = _to_wgs84(zones_gdf)

        zones_lookup = gpd.GeoDataFrame(zones[["transport_zone_id", "geometry"]], geometry="geometry", crs=zones_gdf.crs)
        flows_df = ori_dest_counts.groupby(["from", "to"], as_index=False)["flow_volume"].sum()

        # Log utile
        print(
            f"SCENARIO_META: source=mobility lau={lau_norm} radius={radius} "
            f"zones={len(zones_gdf)} flows={len(flows_df)} time_unit=minutes distance_unit=kilometers"
        )

        return {"zones_gdf": zones_gdf, "flows_df": flows_df, "zones_lookup": _to_wgs84(zones_lookup)}

    except Exception as e:
        print(f"[Fallback used due to error: {e}]")
        return _fallback_scenario()


# --------------------------------------------------------------------
# Public API with LRU cache
# --------------------------------------------------------------------
def _normalized_key(local_admin_unit_id: str, radius: float) -> Tuple[str, float]:
    """
    Normalise la clé de cache :
    - INSEE/LAU -> 'fr-XXXXX'
    - radius arrondi (évite 40.0000001 vs 40.0)
    """
    lau = _normalize_lau_code(local_admin_unit_id)
    rad = round(float(radius), 4)
    return (lau, rad)


@lru_cache(maxsize=8)
def get_scenario(local_admin_unit_id: str = "31555", radius: float = 40.0) -> Dict[str, Any]:
    """
    Récupère un scénario avec cache LRU (jusqu’à 8 combinaisons récentes).
    - Utilise (local_admin_unit_id, radius) normalisés.
    - Retourne { zones_gdf, flows_df, zones_lookup } en WGS84.
    """
    lau, rad = _normalized_key(local_admin_unit_id, radius)
    # On passe les normalisés à la compute pour cohérence des logs et appels.
    return _compute_scenario(local_admin_unit_id=lau, radius=rad)


def clear_scenario_cache() -> None:
    """Vide le cache LRU (utile si les données sous-jacentes changent)."""
    get_scenario.cache_clear()
