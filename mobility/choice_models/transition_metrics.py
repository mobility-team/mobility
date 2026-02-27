from typing import Literal
from typing import Any

import plotly.graph_objects as go
import polars as pl

from mobility.choice_models.transition_schema import TRANSITION_EVENT_COLUMNS

Quantity = Literal["distance", "utility", "travel_time", "trip_count"]

def state_waterfall(
    transitions: pl.LazyFrame,
    demand_groups: pl.LazyFrame,
    transport_zones: Any,
    quantity: Quantity = "distance",
    plot: bool = True,
    top_n: int = 5,
    demand_group_ids: list[int] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Compute and optionally plot state-pair waterfall diagnostics.

    Args:
        transitions (pl.LazyFrame): Transition rows with embedded from/to state
            details.
        demand_groups (pl.LazyFrame): Demand-group table including group size
            and segmentation fields.
        transport_zones (Any): Transport-zone container used to format zone
            labels.
        quantity (Quantity): Metric to decompose.
        plot (bool): Whether to render the Plotly waterfall.
        top_n (int): Number of largest absolute state-pair deltas per iteration.
        demand_group_ids (list[int] | None): Optional demand-group filter.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Iteration totals and ranked
            state-pair deltas (plus "other").

    Raises:
        ValueError: If transition or demand-group inputs are missing required
            columns or contain invalid values.
    """
    spec = _METRIC_SPECS[quantity]

    transitions_df = transitions.collect(engine="streaming")
    _validate_transition_inputs(transitions_df)

    demand_group_keys = _load_demand_group_keys(demand_groups, demand_group_ids)
    transitions_df = _filter_transitions_by_demand_group(transitions_df, demand_group_ids)

    total_population = float(demand_group_keys["n_persons"].sum())
    home_zone_labels = _build_home_zone_labels(transport_zones)
    zone_label_map = _build_zone_label_map(home_zone_labels)

    transitions_enriched = _enrich_transitions(
        transitions_df=transitions_df,
        demand_group_keys=demand_group_keys,
        home_zone_labels=home_zone_labels,
    )

    pair_delta = _build_state_pair_delta(transitions_enriched, spec, total_population)
    pair_details = _build_state_pair_details(transitions_enriched)
    iter_totals = _build_iteration_totals(transitions_enriched, spec, total_population)
    ranked_pairs = _rank_state_pairs(pair_delta, pair_details, transitions_enriched, top_n)

    if plot:
        _plot_state_waterfall(
            iter_totals,
            ranked_pairs,
            quantity,
            str(spec["label"]),
            top_n,
            zone_label_map,
        )

    return _format_outputs(iter_totals, ranked_pairs, spec)


_METRIC_SPECS: dict[Quantity, dict[str, str | bool]] = {
    "distance": {
        "label": "distance",
        "from_col": "distance_from",
        "to_col": "distance_to",
        "total_col": "distance_total",
        "per_person_col": "distance_per_person",
        "delta_col": "delta_distance_per_person",
        "include_stay_home": False,
    },
    "utility": {
        "label": "utility",
        "from_col": "utility_from",
        "to_col": "utility_to",
        "total_col": "utility_total",
        "per_person_col": "utility_per_person",
        "delta_col": "delta_utility_per_person",
        "include_stay_home": True,
    },
    "travel_time": {
        "label": "travel time",
        "from_col": "travel_time_from",
        "to_col": "travel_time_to",
        "total_col": "travel_time_total",
        "per_person_col": "travel_time_per_person",
        "delta_col": "delta_travel_time_per_person",
        "include_stay_home": False,
    },
    "trip_count": {
        "label": "trip count",
        "from_col": "trip_count_from",
        "to_col": "trip_count_to",
        "total_col": "trip_count_total",
        "per_person_col": "trip_count_per_person",
        "delta_col": "delta_trip_count_per_person",
        "include_stay_home": False,
    },
}

def _validate_transition_inputs(transitions_df: pl.DataFrame) -> None:
    """Validate transition input columns and utility completeness.

    Args:
        transitions_df (pl.DataFrame): Materialized transition events.

    Raises:
        ValueError: If required columns are missing or utility values are null
            for moved population.
    """
    missing = sorted(set(TRANSITION_EVENT_COLUMNS).difference(set(transitions_df.columns)))
    if missing:
        raise ValueError(
            "Missing required transition columns for `state_waterfall`: "
            + ", ".join(missing)
            + ". Rerun PopulationTrips to regenerate transitions with embedded state details."
        )

    null_utility = transitions_df.filter(
        (pl.col("n_persons_moved") > 0.0)
        & (pl.col("utility_from").is_null() | pl.col("utility_to").is_null())
    )
    if null_utility.height > 0:
        raise ValueError(
            "Found null utilities in transitions used by `state_waterfall` "
            f"({null_utility.height} rows). Rerun PopulationTrips."
        )


def _load_demand_group_keys(
    demand_groups: pl.LazyFrame,
    demand_group_ids: list[int] | None,
) -> pl.DataFrame:
    """Load and cast demand-group columns used by diagnostics.

    Args:
        demand_groups (pl.LazyFrame): Demand-group source table.
        demand_group_ids (list[int] | None): Optional demand-group filter.

    Returns:
        pl.DataFrame: Demand-group keys with normalized dtypes.

    Raises:
        ValueError: If required columns are missing or filtered population is
            empty/non-positive.
    """
    required = ["demand_group_id", "home_zone_id", "csp", "n_cars", "n_persons"]
    missing = [c for c in required if c not in demand_groups.columns]
    if missing:
        raise ValueError(
            "Missing required demand-group columns for `state_waterfall`: "
            + ", ".join(missing)
        )

    keys = (
        demand_groups
        .select(
            pl.col("demand_group_id").cast(pl.UInt32),
            pl.col("home_zone_id").cast(pl.Int32),
            pl.col("csp").cast(pl.String),
            pl.col("n_cars").cast(pl.Int32),
            pl.col("n_persons").cast(pl.Float64),
        )
        .collect(engine="streaming")
    )

    if demand_group_ids:
        keys = keys.filter(pl.col("demand_group_id").is_in([int(x) for x in demand_group_ids]))

    if keys.is_empty() or float(keys["n_persons"].sum()) <= 0.0:
        raise ValueError("Filtered demand-group population is empty. Adjust `demand_group_ids`.")

    return keys


def _filter_transitions_by_demand_group(
    transitions_df: pl.DataFrame,
    demand_group_ids: list[int] | None,
) -> pl.DataFrame:
    """Filter transition rows by demand-group IDs.

    Args:
        transitions_df (pl.DataFrame): Transition events.
        demand_group_ids (list[int] | None): Optional allowed demand-group IDs.

    Returns:
        pl.DataFrame: Filtered transition events.
    """
    if not demand_group_ids:
        return transitions_df

    allowed = [int(x) for x in demand_group_ids]
    return transitions_df.filter(pl.col("demand_group_id").cast(pl.UInt32).is_in(allowed))


def _build_home_zone_labels(transport_zones: Any) -> pl.DataFrame:
    """Build readable home-zone labels for tooltip display.

    Args:
        transport_zones (Any): Transport-zone container exposing `get()` and
            `study_area.get()`.

    Returns:
        pl.DataFrame: Mapping from home zone id (string) to human-readable
            label.
    """
    tz_lookup = pl.DataFrame(transport_zones.get().drop("geometry", axis=1)).select(
        pl.col("transport_zone_id").cast(pl.String).alias("zone_id"),
        pl.col("local_admin_unit_id").cast(pl.String),
    )

    study_area = pl.DataFrame(transport_zones.study_area.get().drop("geometry", axis=1))
    lau_name_col = "local_admin_unit_name" if "local_admin_unit_name" in study_area.columns else "local_admin_unit_id"

    return (
        tz_lookup
        .join(
            study_area.select(
                pl.col("local_admin_unit_id").cast(pl.String),
                pl.col(lau_name_col).cast(pl.String).alias("local_admin_unit_name"),
            ),
            on="local_admin_unit_id",
            how="left",
        )
        .with_columns(
            local_admin_unit_name=pl.col("local_admin_unit_name").fill_null(pl.col("local_admin_unit_id")),
            home_zone_label=pl.format("{} ({})", pl.col("local_admin_unit_name"), pl.col("zone_id")),
        )
        .select(
            pl.col("zone_id").alias("home_zone_id_str"),
            pl.col("home_zone_label"),
        )
    )


def _build_zone_label_map(home_zone_labels: pl.DataFrame) -> dict[str, str]:
    """Build a dictionary lookup for zone labels.

    Args:
        home_zone_labels (pl.DataFrame): Output of `_build_home_zone_labels`.

    Returns:
        dict[str, str]: Mapping `{zone_id: "label (zone_id)"}`.
    """
    return {
        str(row["home_zone_id_str"]): str(row["home_zone_label"])
        for row in home_zone_labels.iter_rows(named=True)
    }


def _enrich_transitions(
    transitions_df: pl.DataFrame,
    demand_group_keys: pl.DataFrame,
    home_zone_labels: pl.DataFrame,
) -> pl.DataFrame:
    """Attach keys and display-friendly fields to transition rows.

    Args:
        transitions_df (pl.DataFrame): Transition events.
        demand_group_keys (pl.DataFrame): Demand-group attributes.
        home_zone_labels (pl.DataFrame): Home-zone label lookup table.

    Returns:
        pl.DataFrame: Enriched transition rows with casted numeric fields and
            state identifiers.

    Raises:
        ValueError: If non-stay-home rows have missing mandatory details.
    """
    enriched = (
        transitions_df
        .with_columns(demand_group_id=pl.col("demand_group_id").cast(pl.UInt32))
        .join(demand_group_keys, on="demand_group_id", how="left")
        .with_columns(
            motive_seq_id=pl.col("motive_seq_id").cast(pl.UInt32),
            dest_seq_id=pl.col("dest_seq_id").cast(pl.UInt32),
            mode_seq_id=pl.col("mode_seq_id").cast(pl.UInt32),
            motive_seq_id_trans=pl.col("motive_seq_id_trans").cast(pl.UInt32),
            dest_seq_id_trans=pl.col("dest_seq_id_trans").cast(pl.UInt32),
            mode_seq_id_trans=pl.col("mode_seq_id_trans").cast(pl.UInt32),
            home_zone_id_str=pl.col("home_zone_id").cast(pl.String),
            utility_from=pl.col("utility_from").cast(pl.Float64),
            utility_to=pl.col("utility_to").cast(pl.Float64),
            utility_prev_from=pl.col("utility_prev_from").cast(pl.Float64),
            utility_prev_to=pl.col("utility_prev_to").cast(pl.Float64),
            steps_from=pl.col("steps_from").cast(pl.String),
            steps_to=pl.col("steps_to").cast(pl.String),
            trip_count_from=pl.col("trip_count_from").cast(pl.Float64),
            trip_count_to=pl.col("trip_count_to").cast(pl.Float64),
            distance_from=pl.col("distance_from").cast(pl.Float64),
            distance_to=pl.col("distance_to").cast(pl.Float64),
            activity_time_from=pl.col("activity_time_from").cast(pl.Float64),
            activity_time_to=pl.col("activity_time_to").cast(pl.Float64),
            travel_time_from=pl.col("travel_time_from").cast(pl.Float64),
            travel_time_to=pl.col("travel_time_to").cast(pl.Float64),
        )
        .join(home_zone_labels, on="home_zone_id_str", how="left")
        .with_columns(
            state_from=pl.format(
                "dg{}-m{}-d{}-mo{}",
                pl.col("demand_group_id"),
                pl.col("motive_seq_id"),
                pl.col("dest_seq_id"),
                pl.col("mode_seq_id"),
            ),
            state_to=pl.format(
                "dg{}-m{}-d{}-mo{}",
                pl.col("demand_group_id"),
                pl.col("motive_seq_id_trans"),
                pl.col("dest_seq_id_trans"),
                pl.col("mode_seq_id_trans"),
            ),
            mode_seq_id_from=pl.col("mode_seq_id"),
            mode_seq_id_to=pl.col("mode_seq_id_trans"),
        )
    )

    # Non-stay-home rows must contain full details for reliable tooltips.
    missing_from = enriched.filter(
        (pl.col("mode_seq_id_from") != 0)
        & (
            pl.col("steps_from").is_null()
            | pl.col("trip_count_from").is_null()
            | pl.col("distance_from").is_null()
            | pl.col("activity_time_from").is_null()
            | pl.col("travel_time_from").is_null()
        )
    ).height

    missing_to = enriched.filter(
        (pl.col("mode_seq_id_to") != 0)
        & (
            pl.col("steps_to").is_null()
            | pl.col("trip_count_to").is_null()
            | pl.col("distance_to").is_null()
            | pl.col("activity_time_to").is_null()
            | pl.col("travel_time_to").is_null()
        )
    ).height

    if missing_from > 0 or missing_to > 0:
        raise ValueError(
            "Transition rows contain missing non-stay-home details "
            f"(from={missing_from}, to={missing_to})."
        )

    return enriched


def _build_state_pair_delta(
    transitions_enriched: pl.DataFrame,
    spec: dict[str, str | bool],
    total_population: float,
) -> pl.DataFrame:
    """Compute per-person deltas by iteration and state pair.

    Args:
        transitions_enriched (pl.DataFrame): Enriched transition rows.
        spec (dict[str, str | bool]): Metric specification.
        total_population (float): Population denominator for per-person values.

    Returns:
        pl.DataFrame: Delta values with absolute magnitudes for ranking.
    """
    delta_expr = pl.col(spec["to_col"]).fill_null(0.0) - pl.col(spec["from_col"]).fill_null(0.0)

    return (
        transitions_enriched
        .with_columns(
            state_pair=pl.format("{} -> {}", pl.col("state_from"), pl.col("state_to")),
            delta_quantity_per_person=(delta_expr * pl.col("n_persons_moved")) / float(total_population),
        )
        .group_by(["iteration", "state_pair"])
        .agg(delta_quantity_per_person=pl.col("delta_quantity_per_person").sum())
        .with_columns(abs_delta=pl.col("delta_quantity_per_person").abs())
        .sort(["iteration", "abs_delta"], descending=[False, True])
    )


def _weighted_avg(col: str) -> pl.Expr:
    """Build weighted-average expression over `n_persons_moved`.

    Args:
        col (str): Column name to average.

    Returns:
        pl.Expr: Weighted-average expression.
    """
    return (
        pl.when(pl.col("n_persons_moved").sum() > 0.0)
        .then((pl.col(col).fill_null(0.0) * pl.col("n_persons_moved")).sum() / pl.col("n_persons_moved").sum())
        .otherwise(None)
    )


def _build_state_pair_details(transitions_enriched: pl.DataFrame) -> pl.DataFrame:
    """Aggregate tooltip metadata for each `(iteration, state_pair)`.

    Args:
        transitions_enriched (pl.DataFrame): Enriched transition rows.

    Returns:
        pl.DataFrame: Metadata used in waterfall hover tooltips.
    """
    return (
        transitions_enriched
        .with_columns(
            state_pair=pl.format("{} -> {}", pl.col("state_from"), pl.col("state_to")),
            from_steps=pl.when(pl.col("mode_seq_id_from") == 0).then(pl.lit("none")).otherwise(pl.col("steps_from")),
            to_steps=pl.when(pl.col("mode_seq_id_to") == 0).then(pl.lit("none")).otherwise(pl.col("steps_to")),
            home_start_desc=pl.format(
                "#0 | from: {}",
                pl.col("home_zone_label").fill_null(pl.format("unknown ({})", pl.col("home_zone_id_str"))),
            ),
        )
        .group_by(["iteration", "state_pair"])
        .agg(
            from_steps=pl.col("from_steps").unique().sort().str.concat("<br><br>"),
            to_steps=pl.col("to_steps").unique().sort().str.concat("<br><br>"),
            home_start_desc=pl.col("home_start_desc").drop_nulls().first(),
            n_persons_moved_total=pl.col("n_persons_moved").sum(),
            demand_group_desc=pl.format(
                "demand_group_id: {} | home_zone_id: {} | csp: {}",
                pl.col("demand_group_id").drop_nulls().first(),
                pl.col("home_zone_id").drop_nulls().first(),
                pl.col("csp").drop_nulls().first(),
            ),
            activity_time_from_avg=_weighted_avg("activity_time_from"),
            travel_time_from_avg=_weighted_avg("travel_time_from"),
            activity_time_to_avg=_weighted_avg("activity_time_to"),
            travel_time_to_avg=_weighted_avg("travel_time_to"),
            utility_from_avg=_weighted_avg("utility_from"),
            utility_to_avg=_weighted_avg("utility_to"),
            utility_prev_from_avg=_weighted_avg("utility_prev_from"),
            utility_prev_to_avg=_weighted_avg("utility_prev_to"),
        )
    )


def _build_iteration_totals(
    transitions_enriched: pl.DataFrame,
    spec: dict[str, str | bool],
    total_population: float,
) -> pl.DataFrame:
    """Compute iteration totals and prepend iteration-0 baseline.

    Args:
        transitions_enriched (pl.DataFrame): Enriched transition rows.
        spec (dict[str, str | bool]): Metric specification.
        total_population (float): Population denominator for per-person totals.

    Returns:
        pl.DataFrame: Totals per iteration with per-person normalization.
    """
    include_stay_home = bool(spec["include_stay_home"])
    mass_expr = pl.col(spec["to_col"]).fill_null(0.0) * pl.col("n_persons_moved")

    totals = (
        transitions_enriched
        .filter(pl.lit(include_stay_home) | (pl.col("mode_seq_id_to") != 0))
        .with_columns(quantity_total=mass_expr)
        .group_by("iteration")
        .agg(quantity_total=pl.col("quantity_total").sum())
        .with_columns(quantity_per_person=pl.col("quantity_total") / float(total_population))
        .sort("iteration")
    )

    # Utility has non-zero baseline at iter 0 (stay-home utility). Other quantities start at 0.
    if spec["label"] == "utility" and not transitions_enriched.is_empty():
        first_iter = int(transitions_enriched["iteration"].min())
        base_total = (
            transitions_enriched
            .filter(pl.col("iteration") == first_iter)
            .select((pl.col("utility_from") * pl.col("n_persons_moved")).sum().alias("base_total"))["base_total"][0]
        )
        base_total = float(base_total) if base_total is not None else 0.0
    else:
        base_total = 0.0

    base = pl.DataFrame(
        {
            "iteration": [0],
            "quantity_total": [base_total],
            "quantity_per_person": [base_total / float(total_population)],
        }
    ).with_columns(pl.col("iteration").cast(pl.UInt32))

    return (
        pl.concat([base, totals], how="vertical")
        .group_by("iteration")
        .agg(
            quantity_total=pl.col("quantity_total").sum(),
            quantity_per_person=pl.col("quantity_per_person").sum(),
        )
        .sort("iteration")
    )


def _rank_state_pairs(
    pair_delta: pl.DataFrame,
    pair_details: pl.DataFrame,
    transitions_enriched: pl.DataFrame,
    top_n: int,
) -> pl.DataFrame:
    """Rank state pairs and aggregate non-top pairs into `other`.

    Args:
        pair_delta (pl.DataFrame): Delta values per state pair.
        pair_details (pl.DataFrame): Tooltip metadata per state pair.
        transitions_enriched (pl.DataFrame): Enriched transition rows.
        top_n (int): Number of pairs to keep per iteration.

    Returns:
        pl.DataFrame: Ranked top pairs plus aggregated `other` row.
    """
    top_pairs = (
        pair_delta
        .with_columns(rank=pl.col("abs_delta").rank(method="ordinal", descending=True).over("iteration"))
        .filter(pl.col("rank") <= int(top_n))
        .select(["iteration", "state_pair", "delta_quantity_per_person", "abs_delta"])
    )

    other_pairs = (
        pair_delta
        .join(top_pairs.select(["iteration", "state_pair"]), on=["iteration", "state_pair"], how="anti")
        .select(["iteration", "state_pair"])
    )

    other_values = (
        pair_delta
        .join(other_pairs, on=["iteration", "state_pair"], how="inner")
        .group_by("iteration")
        .agg(delta_quantity_per_person=pl.col("delta_quantity_per_person").sum())
        .with_columns(
            state_pair=pl.lit("other"),
            abs_delta=pl.col("delta_quantity_per_person").abs(),
        )
        .select(["iteration", "state_pair", "delta_quantity_per_person", "abs_delta"])
    )

    other_details = (
        transitions_enriched
        .with_columns(state_pair=pl.format("{} -> {}", pl.col("state_from"), pl.col("state_to")))
        .join(other_pairs, on=["iteration", "state_pair"], how="inner")
        .group_by("iteration")
        .agg(
            n_persons_moved_total=pl.col("n_persons_moved").sum(),
            activity_time_from_avg=_weighted_avg("activity_time_from"),
            travel_time_from_avg=_weighted_avg("travel_time_from"),
            activity_time_to_avg=_weighted_avg("activity_time_to"),
            travel_time_to_avg=_weighted_avg("travel_time_to"),
            utility_from_avg=_weighted_avg("utility_from"),
            utility_to_avg=_weighted_avg("utility_to"),
            utility_prev_from_avg=_weighted_avg("utility_prev_from"),
            utility_prev_to_avg=_weighted_avg("utility_prev_to"),
        )
        .with_columns(
            state_pair=pl.lit("other"),
            from_steps=pl.lit("Aggregated non-top state pairs"),
            to_steps=pl.lit("Aggregated non-top state pairs"),
            home_start_desc=pl.lit("#0 | from: multiple"),
            demand_group_desc=pl.lit("demand_group_id: multiple | home_zone_id: multiple | csp: multiple"),
            n_persons_moved_total=pl.col("n_persons_moved_total").cast(pl.Float64),
        )
        .select(
            [
                "iteration",
                "state_pair",
                "from_steps",
                "to_steps",
                "home_start_desc",
                "demand_group_desc",
                "n_persons_moved_total",
                "activity_time_from_avg",
                "travel_time_from_avg",
                "activity_time_to_avg",
                "travel_time_to_avg",
                "utility_from_avg",
                "utility_to_avg",
                "utility_prev_from_avg",
                "utility_prev_to_avg",
            ]
        )
    )

    return (
        pl.concat([top_pairs, other_values], how="vertical")
        .sort(["iteration", "abs_delta"], descending=[False, True])
        .join(pair_details, on=["iteration", "state_pair"], how="left")
        .join(other_details, on=["iteration", "state_pair"], how="left", suffix="_other")
        .with_columns(
            from_steps=pl.coalesce([pl.col("from_steps"), pl.col("from_steps_other")]),
            to_steps=pl.coalesce([pl.col("to_steps"), pl.col("to_steps_other")]),
            home_start_desc=pl.coalesce([pl.col("home_start_desc"), pl.col("home_start_desc_other")]),
            demand_group_desc=pl.coalesce([pl.col("demand_group_desc"), pl.col("demand_group_desc_other")]),
            n_persons_moved_total=pl.coalesce([pl.col("n_persons_moved_total"), pl.col("n_persons_moved_total_other")]),
            activity_time_from_avg=pl.coalesce([pl.col("activity_time_from_avg"), pl.col("activity_time_from_avg_other")]),
            travel_time_from_avg=pl.coalesce([pl.col("travel_time_from_avg"), pl.col("travel_time_from_avg_other")]),
            activity_time_to_avg=pl.coalesce([pl.col("activity_time_to_avg"), pl.col("activity_time_to_avg_other")]),
            travel_time_to_avg=pl.coalesce([pl.col("travel_time_to_avg"), pl.col("travel_time_to_avg_other")]),
            utility_from_avg=pl.coalesce([pl.col("utility_from_avg"), pl.col("utility_from_avg_other")]),
            utility_to_avg=pl.coalesce([pl.col("utility_to_avg"), pl.col("utility_to_avg_other")]),
            utility_prev_from_avg=pl.coalesce([pl.col("utility_prev_from_avg"), pl.col("utility_prev_from_avg_other")]),
            utility_prev_to_avg=pl.coalesce([pl.col("utility_prev_to_avg"), pl.col("utility_prev_to_avg_other")]),
        )
        .drop(
            [
                "from_steps_other",
                "to_steps_other",
                "home_start_desc_other",
                "demand_group_desc_other",
                "n_persons_moved_total_other",
                "activity_time_from_avg_other",
                "travel_time_from_avg_other",
                "activity_time_to_avg_other",
                "travel_time_to_avg_other",
                "utility_from_avg_other",
                "utility_to_avg_other",
                "utility_prev_from_avg_other",
                "utility_prev_to_avg_other",
            ]
        )
    )


def _format_steps_with_zone_labels(
    steps_text: str | None,
    zone_label_map: dict[str, str],
) -> str | None:
    """Format step text with readable zone labels and stable token order.

    Args:
        steps_text (str | None): HTML-formatted step text.
        zone_label_map (dict[str, str]): Zone id to label mapping.

    Returns:
        str | None: Formatted step text with aligned fields.
    """
    if steps_text in (None, "", "none", "n/a"):
        return steps_text

    def _tokenize_line(line: str) -> list[str]:
        """Tokenize one step line and normalize field ordering.

        Args:
            line (str): Raw step line.

        Returns:
            list[str]: Ordered tokens for the line.
        """
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) < 2:
            return [line]

        step_id = parts[0]
        fields = {}
        trailing = []
        for part in parts[1:]:
            if ":" not in part:
                trailing.append(part)
                continue
            key, value = part.split(":", 1)
            fields[key.strip()] = value.strip()

        zone_id = fields.get("to")
        if zone_id is not None:
            fields["to"] = zone_label_map.get(zone_id, f"unknown ({zone_id})")

        ordered = [step_id]
        if "to" in fields:
            ordered.append(f"to: {fields['to']}")
        if "motive" in fields:
            ordered.append(f"motive: {fields['motive']}")
        if "mode" in fields:
            ordered.append(f"mode: {fields['mode']}")
        if "dist_km" in fields:
            ordered.append(f"dist_km: {fields['dist_km']}")
        if "time_h" in fields:
            ordered.append(f"time_h: {fields['time_h']}")

        return ordered + trailing

    def _pad_html(token: str, width: int) -> str:
        """Right-pad a token using non-breaking spaces for HTML alignment.

        Args:
            token (str): Token to pad.
            width (int): Target token width.

        Returns:
            str: HTML-padded token.
        """
        # HTML collapses normal spaces, so use non-breaking spaces for alignment.
        pad_len = max(0, width - len(token))
        return token + ("&nbsp;" * pad_len)

    token_rows = [_tokenize_line(line) for line in steps_text.split("<br>")]
    max_cols = max(len(row) for row in token_rows)
    widths = [0] * max_cols
    for row in token_rows:
        for idx, token in enumerate(row):
            widths[idx] = max(widths[idx], len(token))

    aligned_lines = []
    for row in token_rows:
        padded = [_pad_html(token, widths[idx]) for idx, token in enumerate(row)]
        aligned_lines.append(" | ".join(padded))

    return "<br>".join(aligned_lines)


def _plot_state_waterfall(
    iter_totals: pl.DataFrame,
    ranked_pairs: pl.DataFrame,
    quantity: Quantity,
    label: str,
    top_n: int,
    zone_label_map: dict[str, str],
) -> None:
    """Render interactive waterfall chart with transition tooltips.

    Args:
        iter_totals (pl.DataFrame): Iteration total quantities.
        ranked_pairs (pl.DataFrame): Ranked state-pair deltas with metadata.
        quantity (Quantity): Quantity identifier used in labels.
        label (str): Human-readable quantity label.
        top_n (int): Number of top pairs included per iteration.
        zone_label_map (dict[str, str]): Zone id to human-readable label mapping.

    Returns:
        None
    """
    if iter_totals.is_empty():
        return

    labels: list[str] = []
    measures: list[str] = []
    values: list[float] = []
    texts: list[str] = []
    hovertexts: list[str] = []

    def _fmt_prev_utility(value: Any) -> str:
        """Format optional previous-iteration utility for hover output.

        Args:
            value (Any): Utility value or null-like object.

        Returns:
            str: Formatted utility string.
        """
        return "none" if value is None else f"{float(value):.3f}"

    base_val = float(iter_totals.filter(pl.col("iteration") == 0)["quantity_per_person"][0])
    labels.append("iter 0 total")
    measures.append("absolute")
    values.append(base_val)
    texts.append(f"{base_val:.3f}")
    hovertexts.append(f"Iteration: 0<br>Total average {quantity} per person: {base_val:.3f}")

    for iteration in [int(x) for x in iter_totals["iteration"].to_list() if int(x) != 0]:
        changes = ranked_pairs.filter(pl.col("iteration") == iteration).sort("abs_delta", descending=True)

        for row in changes.iter_rows(named=True):
            delta = float(row["delta_quantity_per_person"])
            if delta == 0.0:
                continue

            labels.append(f"iter {iteration} {row['state_pair']}")
            measures.append("relative")
            values.append(delta)
            texts.append(f"{delta:+.3f}")

            home_start = row.get("home_start_desc") or "#0 | from: n/a"
            from_steps = _format_steps_with_zone_labels(row.get("from_steps"), zone_label_map)
            to_steps = _format_steps_with_zone_labels(row.get("to_steps"), zone_label_map)
            from_steps_txt = home_start if from_steps in (None, "", "none", "n/a") else f"{home_start}<br>{from_steps}"
            to_steps_txt = home_start if to_steps in (None, "", "none", "n/a") else f"{home_start}<br>{to_steps}"

            hovertexts.append(
                (
                    f"<b>Iteration {iteration}</b><br>"
                    f"<span style='font-family:monospace'>"
                    f"state_pair : {row['state_pair']}<br>"
                    f"delta_{quantity}_pp : {delta:+.3f}<br>"
                    f"persons_moved  : {float(row.get('n_persons_moved_total') or 0.0):.2f}<br>"
                    f"{row.get('demand_group_desc') or 'demand_group_id: n/a | home_zone_id: n/a | csp: n/a'}<br>"
                    f"</span>"
                    f"<br><b>From state</b><br>"
                    f"<span style='font-family:monospace'>"
                    f"{from_steps_txt}<br>"
                    f"activity_avg_h : {float(row.get('activity_time_from_avg') or 0.0):.3f}<br>"
                    f"travel_avg_h   : {float(row.get('travel_time_from_avg') or 0.0):.3f}<br>"
                    f"utility_prev   : {_fmt_prev_utility(row.get('utility_prev_from_avg'))}<br>"
                    f"utility_used   : {float(row.get('utility_from_avg') or 0.0):.3f}"
                    f"</span><br>"
                    f"<br><b>To state</b><br>"
                    f"<span style='font-family:monospace'>"
                    f"{to_steps_txt}<br>"
                    f"activity_avg_h : {float(row.get('activity_time_to_avg') or 0.0):.3f}<br>"
                    f"travel_avg_h   : {float(row.get('travel_time_to_avg') or 0.0):.3f}<br>"
                    f"utility_prev   : {_fmt_prev_utility(row.get('utility_prev_to_avg'))}<br>"
                    f"utility_used   : {float(row.get('utility_to_avg') or 0.0):.3f}"
                    f"</span>"
                )
            )

        total = float(iter_totals.filter(pl.col("iteration") == iteration)["quantity_per_person"][0])
        labels.append(f"iter {iteration} total")
        measures.append("total")
        values.append(0.0)
        texts.append(f"{total:.3f}")
        hovertexts.append(f"Iteration: {iteration}<br>Total average {quantity} per person: {total:.3f}")

    fig = go.Figure(
        go.Waterfall(
            orientation="h",
            y=labels,
            measure=measures,
            x=values,
            text=texts,
            textposition="outside",
            hovertext=hovertexts,
            hovertemplate="%{hovertext}<extra></extra>",
            connector={"line": {"color": "gray"}},
        )
    )

    fig.update_layout(
        title=f"Average {label} per person by iteration and top-{int(top_n)} state-pair deltas",
        xaxis_title=f"{label.capitalize()} per person",
        yaxis=dict(autorange="reversed"),
        width=1500,
        height=max(650, 24 * len(labels)),
        margin=dict(l=280, r=40, t=70, b=40),
    )
    fig.show("browser")


def _format_outputs(
    iter_totals: pl.DataFrame,
    ranked_pairs: pl.DataFrame,
    spec: dict[str, str | bool],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Rename generic output columns to metric-specific names.

    Args:
        iter_totals (pl.DataFrame): Iteration totals with generic column names.
        ranked_pairs (pl.DataFrame): Ranked pairs with generic delta column.
        spec (dict[str, str | bool]): Metric specification.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Renamed totals and pairs outputs.
    """
    totals_out = iter_totals.rename(
        {
            "quantity_total": str(spec["total_col"]),
            "quantity_per_person": str(spec["per_person_col"]),
        }
    )

    pairs_out = ranked_pairs.rename({"delta_quantity_per_person": str(spec["delta_col"])})
    return totals_out, pairs_out
