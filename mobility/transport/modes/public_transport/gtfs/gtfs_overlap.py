"""Find conservative cross-feed timetable overlaps using space and time."""

from itertools import combinations

import pandas as pd
import polars as pl
from pyproj import Transformer


def find_within_feed_duplicates(trips, stop_times, interval_columns=()):
    """Find identical same-route scheduled trips on the selected service day.

    Exact stop IDs, times and boarding rules are required. One trip is kept
    deterministically for each identical group. Headway approximations are
    excluded because they do not represent exact departures.
    """
    schema = {"source": pl.String, "kept_trip_id": pl.String, "removed_trip_id": pl.String}
    if trips.empty or stop_times.empty:
        return pl.DataFrame(schema=schema)
    eligible = (pl.from_pandas(trips[["trip_id", "route_id", "_frequency_kind", *interval_columns]])
                .filter(pl.col("_frequency_kind") != "headway")
                .select("trip_id", "route_id", *interval_columns))
    visits = (pl.from_pandas(stop_times[["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "pickup_type", "drop_off_type"]])
              .join(eligible, on="trip_id", how="semi")
              .sort(["trip_id", "stop_sequence"]))
    patterns = (visits.group_by("trip_id", maintain_order=True)
                .agg(pl.col("stop_id"), pl.col("arrival_time"), pl.col("departure_time"),
                     pl.col("pickup_type"), pl.col("drop_off_type"))
                .join(eligible, on="trip_id", how="inner")
                .with_columns(pl.col("trip_id").str.split_exact("-", 1).struct.field("field_0").alias("source"))
                .sort("trip_id"))
    duplicates = (patterns.group_by(["source", "route_id", "stop_id", "arrival_time", "departure_time", "pickup_type", "drop_off_type", *interval_columns], maintain_order=True)
                  .agg(pl.col("trip_id"))
                  .filter(pl.col("trip_id").list.len() > 1)
                  .with_columns(pl.col("trip_id").list.first().alias("kept_trip_id"),
                                pl.col("trip_id").list.slice(1).alias("removed_trip_id"))
                  .select("source", "kept_trip_id", "removed_trip_id")
                  .explode("removed_trip_id"))
    return duplicates.select(list(schema)).sort(["source", "kept_trip_id", "removed_trip_id"])


def find_trip_overlaps(
    trips, stop_times, stops, *, distance_m=10.0, time_seconds=0,
    batch_size=5000, intervals=None,
):
    """Return candidate pairs with the same number of ordered stop visits.

    Names and source identifiers do not participate in matching, other than
    requiring different sources. Every departure must be close in time, every
    corresponding stop close in space, and boarding and alighting
    rules identical at every stop. Trips expanded from non-exact frequency
    service are excluded: their departure times are only approximations.
    This function reports pairs; it does not remove service.
    """
    schema = {
        "left_trip_id": pl.String, "right_trip_id": pl.String,
        "left_source": pl.String, "right_source": pl.String,
        "stop_count": pl.UInt32, "max_distance_m": pl.Float64,
        "max_time_difference_s": pl.Float64, "max_arrival_difference_s": pl.Float64,
    }
    if distance_m <= 0 or time_seconds < 0 or batch_size <= 0:
        raise ValueError("distance_m and batch_size must be positive and time_seconds nonnegative")
    if trips.empty or stop_times.empty or stops.empty:
        return pl.DataFrame(schema=schema)

    # EPSG:3035 is suitable for the European study areas supported by Mobility.
    transformer = Transformer.from_crs(4326, 3035, always_xy=True)
    x, y = transformer.transform(stops.stop_lon.to_numpy(), stops.stop_lat.to_numpy())
    locations = pl.DataFrame({"stop_id": stops.stop_id.to_numpy(), "x": x, "y": y})
    eligible = pl.from_pandas(trips[["trip_id", "_frequency_kind"]]).filter(
        pl.col("_frequency_kind") != "headway"
    ).select("trip_id")
    visit_columns = ["trip_id", "stop_id", "stop_sequence", "arrival_time",
                     "departure_time", "pickup_type", "drop_off_type"]
    visits = (
        pl.from_pandas(stop_times[visit_columns])
        .join(eligible, on="trip_id", how="semi")
        .join(locations, on="stop_id", how="inner")
        .sort(["trip_id", "stop_sequence"])
    )
    patterns = (
        visits.group_by("trip_id", maintain_order=True)
        .agg(pl.col("x"), pl.col("y"), pl.col("arrival_time").alias("arr"),
             pl.col("departure_time").alias("dep"), pl.col("pickup_type").alias("pickup"),
             pl.col("drop_off_type").alias("dropoff"))
        .with_columns(
            pl.col("trip_id").str.split_exact("-", 1).struct.field("field_0").alias("source"),
            pl.col("x").list.len().alias("n"),
            pl.col("x").list.first().alias("first_x"),
            pl.col("y").list.first().alias("first_y"),
            pl.col("dep").list.first().alias("first_t"),
        )
        .filter(pl.col("n") >= 2)
        .with_columns(
            (pl.col("first_x") / distance_m).floor().cast(pl.Int64).alias("cell_x"),
            (pl.col("first_y") / distance_m).floor().cast(pl.Int64).alias("cell_y"),
            (pl.col("first_t") / max(time_seconds, 1)).floor().cast(pl.Int64).alias("cell_t"),
        )
    )
    if patterns.is_empty():
        return pl.DataFrame(schema=schema)
    if intervals is not None:
        windows = pl.from_pandas(intervals[["trip_id", "interval_start", "interval_end"]])
        patterns = patterns.join(windows, on="trip_id", how="inner")
        # Matching intervals need at least one common hour. Index each window
        # by its covered hours to avoid joining all-day services together.
        patterns = (patterns.with_columns(
            pl.int_ranges((pl.col("interval_start") / 3600).floor().cast(pl.Int64),
                          (pl.col("interval_end") / 3600).ceil().cast(pl.Int64)).alias("hour_bucket")
        ).explode("hour_bucket").with_columns(pl.col("hour_bucket").alias("cell_t")))
    # Neighbouring cells prevent losing a match at a grid boundary. Processing
    # bounded left batches avoids materialising the full candidate cross join.
    time_offsets = (0,) if intervals is not None else (-1, 0, 1)
    offsets = pl.DataFrame(
        [(x, y, t) for x in (-1, 0, 1) for y in (-1, 0, 1) for t in time_offsets],
        schema=["dx", "dy", "dt"], orient="row",
    )
    # Search using only the first stop and time. Carrying every stop of each
    # trip through this join makes the intermediate tables much larger.
    sequence_columns = ["trip_id", "x", "y", "arr", "dep", "pickup", "dropoff"]
    sequences = patterns.select(sequence_columns)
    if intervals is not None:
        sequences = sequences.unique(subset=["trip_id"], maintain_order=True)
    search_columns = ["trip_id", "source", "n", "first_x", "first_y", "first_t", "cell_x", "cell_y", "cell_t"]
    if intervals is not None:
        search_columns += ["interval_start", "interval_end"]
    batches = []
    source_frames = patterns.select(search_columns).sort("source").partition_by("source", maintain_order=True)
    for left_source, right_source, start in (
        (left, right, start)
        for left, right in combinations(source_frames, 2)
        for start in range(0, left.height, batch_size)
    ):
        left = (left_source.slice(start, batch_size).join(offsets, how="cross")
                .with_columns((pl.col("cell_x") + pl.col("dx")).alias("join_x"),
                              (pl.col("cell_y") + pl.col("dy")).alias("join_y"),
                              (pl.col("cell_t") + pl.col("dt")).alias("join_t")))
        candidate = (left.join(right_source, left_on=["join_x", "join_y", "join_t"],
                               right_on=["cell_x", "cell_y", "cell_t"], suffix="_r")
                     .filter((pl.col("source") < pl.col("source_r")) & (pl.col("n") == pl.col("n_r"))
                             & ((pl.col("first_t") - pl.col("first_t_r")).abs() <= time_seconds)
                             & (((pl.col("first_x") - pl.col("first_x_r")) ** 2
                                 + (pl.col("first_y") - pl.col("first_y_r")) ** 2) <= distance_m ** 2)))
        if intervals is not None:
            candidate = candidate.filter((pl.col("interval_start") < pl.col("interval_end_r")) &
                                         (pl.col("interval_start_r") < pl.col("interval_end"))).unique(
                                             subset=["trip_id", "trip_id_r"], maintain_order=True)
        if candidate.is_empty():
            continue
        # Only candidate pairs need their complete stop sequences.
        candidate = (candidate.join(sequences, on="trip_id")
                     .join(sequences, left_on="trip_id_r", right_on="trip_id", suffix="_r"))
        # Compare corresponding stops. A pair is kept only when every stop
        # satisfies the distance, time and boarding-rule checks.
        position = pl.col("position")
        dx = pl.col("x").list.get(position) - pl.col("x_r").list.get(position)
        dy = pl.col("y").list.get(position) - pl.col("y_r").list.get(position)
        arrival_difference = (pl.col("arr").list.get(position) - pl.col("arr_r").list.get(position)).abs()
        departure_difference = (pl.col("dep").list.get(position) - pl.col("dep_r").list.get(position)).abs()
        same_pickup = pl.col("pickup").list.get(position) == pl.col("pickup_r").list.get(position)
        same_dropoff = pl.col("dropoff").list.get(position) == pl.col("dropoff_r").list.get(position)
        comparisons = (
            candidate.with_columns(pl.int_ranges(0, pl.col("n")).alias("position"))
            .explode("position")
            .with_columns(
                (dx ** 2 + dy ** 2).sqrt().alias("distance"),
                pl.max_horizontal(arrival_difference, departure_difference).alias("time_difference"),
                arrival_difference.alias("arrival_difference"),
                departure_difference.alias("departure_difference"),
                (same_pickup & same_dropoff).alias("same_boarding"),
            )
            .group_by("trip_id", "trip_id_r", "source", "source_r")
            .agg(
                pl.len().alias("stop_count"),
                pl.col("distance").max().alias("max_distance_m"),
                pl.col("time_difference").max().alias("max_time_difference_s"),
                pl.col("arrival_difference").max().alias("max_arrival_difference_s"),
                pl.col("departure_difference").max().alias("max_departure_difference_s"),
                pl.col("same_boarding").all().alias("same_boarding"),
            )
            .filter((pl.col("max_distance_m") <= distance_m)
                    & (pl.col("max_departure_difference_s") <= time_seconds)
                    & pl.col("same_boarding"))
            .rename({"trip_id": "left_trip_id", "trip_id_r": "right_trip_id",
                     "source": "left_source", "source_r": "right_source"})
            .select(list(schema))
        )
        batches.append(comparisons)
    return (pl.concat(batches).unique(subset=["left_trip_id", "right_trip_id"], maintain_order=True)
            .sort(["left_trip_id", "right_trip_id"])) if batches else pl.DataFrame(schema=schema)


def find_headway_overlaps(trips, stop_times, stops):
    """Compare headway windows using relative stop times, not invented departures.

    Return candidate interval pairs, the trips generated from each interval,
    and duplicate intervals within a feed. Windows are measured at the first
    retained stop: a service can enter the area after its original first stop.
    """
    headway = trips.loc[trips["_frequency_kind"] == "headway"].copy()
    if headway.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    headway["source"] = headway.trip_id.str.split("-", n=1).str[0]
    group_columns = ["source", "_frequency_template", "_frequency_start", "_frequency_end", "_frequency_headway"]
    ordered = headway.sort_values(["_frequency_departure", "trip_id"])
    representatives = ordered.drop_duplicates(group_columns).copy()
    first = (stop_times.loc[stop_times.trip_id.isin(representatives.trip_id)]
             .sort_values(["trip_id", "stop_sequence"]).groupby("trip_id").departure_time.first())
    representatives["first_local_time"] = representatives.trip_id.map(first)
    shift = representatives.first_local_time - representatives._frequency_departure
    representatives["interval_start"] = representatives._frequency_start + shift
    representatives["interval_end"] = representatives._frequency_end + shift
    representatives["headway"] = representatives["_frequency_headway"]
    # Keep track of every generated departure so the whole lower-priority
    # interval can be removed when two intervals match.
    interval_ids = representatives[group_columns + ["trip_id"]].rename(
        columns={"trip_id": "representative_trip_id"}
    )
    members = headway[["trip_id", *group_columns]].merge(interval_ids, on=group_columns)
    relative = stop_times.loc[stop_times.trip_id.isin(representatives.trip_id)].copy()
    offset = relative.trip_id.map(representatives.set_index("trip_id").first_local_time)
    relative["arrival_time"] -= offset
    relative["departure_time"] -= offset
    matching_trips = representatives.copy()
    matching_trips["_frequency_kind"] = "exact"
    within = find_within_feed_duplicates(
        matching_trips, relative, interval_columns=("interval_start", "interval_end", "headway")
    ).to_pandas()
    matching_trips = matching_trips.loc[~matching_trips.trip_id.isin(within.removed_trip_id)]
    pairs = find_trip_overlaps(matching_trips, relative, stops, intervals=representatives).to_pandas()
    if not pairs.empty:
        details = representatives.set_index("trip_id")
        for side in ("left", "right"):
            for column in ("interval_start", "interval_end", "headway"):
                pairs[f"{side}_{column}"] = pairs[f"{side}_trip_id"].map(details[column])
        pairs["identical_interval"] = ((pairs.left_interval_start == pairs.right_interval_start) &
                                       (pairs.left_interval_end == pairs.right_interval_end) &
                                       (pairs.left_headway == pairs.right_headway))
    return pairs, members, within
