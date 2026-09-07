"""Walking connections and explicit GTFS transfer rules."""

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree


def prepare_transfers(tables):
    """Keep prohibitions and route rules for the graph to resolve by specificity."""
    stops = tables["stops"]
    active = stops.loc[stops.stop_id.isin(tables["stop_times"].stop_id)].copy()
    points = gpd.GeoSeries(gpd.points_from_xy(active.stop_lon, active.stop_lat), crs=4326)
    points = points.to_crs(points.estimate_utm_crs())
    coordinates = np.column_stack([points.x, points.y])
    pairs = cKDTree(coordinates).query_pairs(200, output_type="ndarray")
    pairs = np.concatenate([pairs, pairs[:, ::-1]], axis=0)
    distance = np.linalg.norm(coordinates[pairs[:, 0]] - coordinates[pairs[:, 1]], axis=1)
    ids = active.stop_id.to_numpy()
    positions = dict(zip(ids, coordinates))
    transfers = pd.DataFrame({"from_stop_id": ids[pairs[:, 0]], "to_stop_id": ids[pairs[:, 1]],
                              "min_transfer_time": 31 + 1.125 * distance,
                              "transfer_type": 2, "from_route_id": "", "to_route_id": "",
                              "specificity": -1})

    # Explicit rules override generated walking links, including prohibitions.
    # A station rule applies to its platforms rather than to an unused station node.
    children = {stop: [stop] for stop in active.stop_id}
    if "parent_station" in active:
        for parent, group in active.loc[active.parent_station.ne("")].groupby("parent_station"):
            children[parent] = group.stop_id.tolist()
    explicit = []
    for row in tables["transfers"].to_dict("records"):
        origins, destinations = children.get(row.get("from_stop_id"), []), children.get(row.get("to_stop_id"), [])
        if not origins or not destinations:
            continue
        if row.get("from_trip_id", "") or row.get("to_trip_id", ""):
            raise ValueError("Trip-specific transfer rules are not yet supported by the public transport graph")
        kind = int(row.get("transfer_type", "") or 0)
        if kind not in (0, 1, 2, 3):
            raise ValueError(f"Transfer type {kind} is not supported by the public transport graph")
        minimum = row.get("min_transfer_time", "")
        if kind == 2 and minimum == "":
            raise ValueError("Transfer type 2 requires min_transfer_time")
        if minimum != "" and float(minimum) < 0:
            raise ValueError("Negative min_transfer_time")
        from_route, to_route = row.get("from_route_id", ""), row.get("to_route_id", "")
        for origin in origins:
            for destination in destinations:
                # Recommended connections with no stated minimum still need walking time.
                left, right = positions[origin], positions[destination]
                seconds = float(minimum) if minimum != "" else 31 + 1.125 * np.linalg.norm(left - right)
                explicit.append({"from_stop_id": origin, "to_stop_id": destination,
                                 "min_transfer_time": seconds, "transfer_type": kind,
                                 "from_route_id": from_route, "to_route_id": to_route,
                                 "specificity": int(bool(from_route)) + int(bool(to_route))})
    return pd.concat([transfers, pd.DataFrame(explicit)], ignore_index=True).drop_duplicates()
