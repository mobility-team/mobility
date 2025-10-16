from __future__ import annotations
import os
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point


def _to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(4326, allow_override=True)
    try:
        epsg = gdf.crs.to_epsg()
    except Exception:
        epsg = None
    return gdf if epsg == 4326 else gdf.to_crs(4326)


def _normalize_lau_id(raw: str | None) -> str:
    """
    Accepte '31555' (INSEE) ou 'fr-31555' et renvoie toujours 'fr-31555'.
    Si invalide/None -> Toulouse 'fr-31555'.
    """
    if not raw:
        return "fr-31555"
    s = str(raw).strip().lower()
    if s.startswith("fr-"):
        code = s[3:]
    else:
        code = s
    code = "".join(ch for ch in code if ch.isdigit())[:5]
    return f"fr-{code}" if len(code) == 5 else "fr-31555"


def _fallback_scenario() -> dict:
    """Scénario minimal de secours (Paris–Lyon)."""
    paris = (2.3522, 48.8566)
    lyon = (4.8357, 45.7640)

    pts = gpd.GeoDataFrame(
        {"transport_zone_id": ["paris", "lyon"], "geometry": [Point(*paris), Point(*lyon)]},
        geometry="geometry", crs=4326
    )
    zones = pts.to_crs(3857)
    zones["geometry"] = zones.geometry.buffer(5000)  # 5 km
    zones = zones.to_crs(4326)
    # Indicateurs d'exemple
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


def load_scenario(radius: int | float = 40, local_admin_unit_id: str | None = "fr-31555") -> dict:
    """
    Charge un scénario de mobilité avec rayon et commune paramétrables.
    - local_admin_unit_id : code INSEE (ex '31555') ou 'fr-31555'
    Retourne:
      zones_gdf (WGS84) avec average_travel_time, total_dist_km, shares...
      flows_df (from, to, flow_volume)
      zones_lookup (centroïdes / géom pour arcs)
    """
    try:
        import mobility
        from mobility.path_routing_parameters import PathRoutingParameters

        mobility.set_params(debug=True, r_packages_download_method="wininet")

        def _safe_instantiate(cls, *args, **kwargs):
            try:
                return cls(*args, **kwargs)
            except TypeError as e:
                if "takes 2 positional arguments but 3 were given" in str(e):
                    raise
                elif "missing 1 required positional argument: 'cache_path'" in str(e):
                    return cls(*args, cache_path=None, **kwargs)
                else:
                    raise

        lau = _normalize_lau_id(local_admin_unit_id)

        # --- Création des assets ---
        transport_zones = _safe_instantiate(
            mobility.TransportZones,
            local_admin_unit_id=lau,
            radius=float(radius),
            level_of_detail=0,
        )

        # Modes
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

        # Marche si dispo
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

        work_choice_model = _safe_instantiate(mobility.WorkDestinationChoiceModel, transport_zones, modes=modes)
        mode_choice_model = _safe_instantiate(mobility.TransportModeChoiceModel, destination_choice_model=work_choice_model)

        work_choice_model.get()
        mode_df = mode_choice_model.get()
        comparison = work_choice_model.get_comparison()

        def _canon_mode(label: str) -> str:
            s = str(label).strip().lower()
            if s in {"bike", "bicycle", "velo", "cycling"}: return "bicycle"
            if s in {"walk", "walking", "foot", "pedestrian", "pedestrianmode"}: return "walk"
            if s in {"car", "auto", "driving", "voiture"}: return "car"
            return s

        if "mode" in mode_df.columns:
            mode_df["mode"] = mode_df["mode"].map(_canon_mode)

        def _get_costs(m, label):
            df = m.travel_costs.get().copy()
            df["mode"] = label
            return df

        costs_list = [_get_costs(car, "car"), _get_costs(bicycle, "bicycle")]
        if walk is not None:
            costs_list.append(_get_costs(walk, "walk"))

        travel_costs = pd.concat(costs_list, ignore_index=True)
        travel_costs["mode"] = travel_costs["mode"].map(_canon_mode)

        # Normalisation unités
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

        # Jointures d'identifiants
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

        costs_cols = ["from", "to", "mode", "time_min", "dist_km"]
        available = [c for c in costs_cols if c in travel_costs.columns]
        travel_costs_norm = travel_costs[available].copy()

        od_mode = modal_shares.merge(travel_costs_norm, on=["from", "to", "mode"], how="left")
        od_mode["time_min"] = pd.to_numeric(od_mode.get("time_min", np.nan), errors="coerce")
        od_mode["dist_km"] = pd.to_numeric(od_mode.get("dist_km", np.nan), errors="coerce")

        # Agrégats par origine
        den = od_mode.groupby("from", as_index=True)["flow_volume"].sum().replace(0, np.nan)
        num_time = (od_mode["time_min"] * od_mode["flow_volume"]).groupby(od_mode["from"]).sum(min_count=1)
        avg_time_min = (num_time / den).rename("average_travel_time")
        num_dist = (od_mode["dist_km"] * od_mode["flow_volume"]).groupby(od_mode["from"]).sum(min_count=1)
        per_person_dist_km = (num_dist / den).rename("total_dist_km")

        mode_flow_by_from = od_mode.pivot_table(index="from", columns="mode", values="flow_volume", aggfunc="sum", fill_value=0.0)
        for col in ("car", "bicycle", "walk"):
            if col not in mode_flow_by_from.columns:
                mode_flow_by_from[col] = 0.0

        share_car = (mode_flow_by_from["car"] / den).rename("share_car")
        share_bicycle = (mode_flow_by_from["bicycle"] / den).rename("share_bicycle")
        share_walk = (mode_flow_by_from["walk"] / den).rename("share_walk")

        # Construction zones
        zones = transport_zones.get()[["transport_zone_id", "geometry", "local_admin_unit_id"]].copy()
        zones_gdf = gpd.GeoDataFrame(zones, geometry="geometry")

        agg = pd.concat([avg_time_min, per_person_dist_km, share_car, share_bicycle, share_walk], axis=1)\
                .reset_index().rename(columns={"from": "transport_zone_id"})
        zones_gdf = zones_gdf.merge(agg, on="transport_zone_id", how="left")
        zones_gdf = _to_wgs84(zones_gdf)

        zones_lookup = gpd.GeoDataFrame(
            zones[["transport_zone_id", "geometry"]], geometry="geometry", crs=zones_gdf.crs
        )
        flows_df = ori_dest_counts.groupby(["from", "to"], as_index=False)["flow_volume"].sum()

        print(
            f"SCENARIO_META: source=mobility zones={len(zones_gdf)} flows={len(flows_df)} "
            f"time_unit=minutes distance_unit=kilometers"
        )

        return {"zones_gdf": zones_gdf, "flows_df": flows_df, "zones_lookup": _to_wgs84(zones_lookup)}

    except Exception as e:
        print(f"[Fallback used due to error: {e}]")
        return _fallback_scenario()
