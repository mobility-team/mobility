import logging
import math
from typing import Any

import polars as pl
from mobility_destination_sequence_sampler import DestinationPlanSearch

from .demand_subgroups import DEMAND_UNIT_COLS, with_demand_subgroup_id


SEQUENCE_COLUMNS = ["activity_seq_id", "time_seq_id"]
RAW_CONTEXT_COLUMNS = DEMAND_UNIT_COLS + SEQUENCE_COLUMNS


def sample_destination_plans(
    *,
    activity_sequences: pl.DataFrame,
    activity_durations: pl.DataFrame,
    demand_groups: pl.DataFrame,
    destination_saturation: pl.DataFrame,
    mode_costs: pl.DataFrame,
    transport_zones: Any,
    activities: list[Any],
    resolved_activity_parameters: dict[str, Any],
    min_activity_time_constant: float,
    logit_scale: float,
    update_plan_timings: bool,
    use_shadow_prices: bool,
    exploration_seed: int,
    top_k: int,
) -> pl.DataFrame:
    """Return complete destination plans found by the bounded Rust search."""
    activity_names = sorted(resolved_activity_parameters)
    activity_ids = {
        activity_name: activity_id
        for activity_id, activity_name in enumerate(activity_names)
    }

    od_costs = _prepare_od_costs(mode_costs, logit_scale)
    destination_inputs = _prepare_destination_inputs(
        destination_saturation=destination_saturation,
        transport_zones=transport_zones,
        resolved_activity_parameters=resolved_activity_parameters,
        activity_ids=activity_ids,
    )
    steps, initial_locations, raw_to_context, source_steps = _prepare_contexts(
        activity_sequences=activity_sequences,
        activity_durations=activity_durations,
        demand_groups=demand_groups,
        activities=activities,
        resolved_activity_parameters=resolved_activity_parameters,
        activity_ids=activity_ids,
        min_activity_time_constant=min_activity_time_constant,
    )

    search = DestinationPlanSearch(
        od_costs=od_costs,
        destination_inputs=destination_inputs,
    )
    plans, report = search.top_k(
        steps=steps,
        initial_locations=initial_locations,
        logit_scale=logit_scale,
        update_plan_timings=update_plan_timings,
        use_shadow_prices=use_shadow_prices,
        exploration_seed=exploration_seed,
        top_k=top_k,
        skip_contexts_without_plan=True,
    )
    if report["contexts_without_plan"] > 0:
        logging.warning(
            "Destination plan search did not find a complete plan for %s unique contexts.",
            report["contexts_without_plan"],
        )

    # Expand deduplicated search contexts back to each demand unit, then restore
    # the Mobility destination-sequence columns expected by mode search.
    return (
        plans.join(raw_to_context, on="context_id")
        .join(source_steps, on=RAW_CONTEXT_COLUMNS + ["layer"])
        .select(
            RAW_CONTEXT_COLUMNS
            + [
                pl.col("draw_id").alias("dest_draw_id"),
                "activity",
                "home_zone_id",
                "seq_step_index",
                "step_count",
                pl.col("origin").alias("from"),
                pl.col("destination").alias("to"),
                "departure_time",
                "arrival_time",
                "next_departure_time",
            ]
        )
        .sort(RAW_CONTEXT_COLUMNS + ["dest_draw_id", "seq_step_index"])
    )


def _prepare_od_costs(mode_costs: pl.DataFrame, logit_scale: float) -> pl.DataFrame:
    """Average cost and time across modes with the destination-choice logit scale."""
    mode_costs = mode_costs.lazy().select(
        pl.col("from").cast(pl.UInt32).alias("origin"),
        pl.col("to").cast(pl.UInt32).alias("destination"),
        pl.col("cost").cast(pl.Float64),
        pl.col("time").cast(pl.Float64),
    )
    minimum_costs = mode_costs.group_by(["origin", "destination"]).agg(
        minimum_cost=pl.col("cost").min()
    )
    return (
        mode_costs.join(minimum_costs, on=["origin", "destination"])
        .with_columns(
            mode_weight=(
                -pl.lit(logit_scale)
                * (pl.col("cost") - pl.col("minimum_cost"))
            ).exp()
        )
        .group_by(["origin", "destination"])
        .agg(
            weighted_cost=(pl.col("mode_weight") * pl.col("cost")).sum(),
            weighted_time=(pl.col("mode_weight") * pl.col("time")).sum(),
            total_weight=pl.col("mode_weight").sum(),
        )
        .select(
            "origin",
            "destination",
            cost=pl.col("weighted_cost") / pl.col("total_weight"),
            time=pl.col("weighted_time") / pl.col("total_weight"),
        )
        .collect(engine="streaming")
        .sort(["origin", "destination"])
    )


def _prepare_destination_inputs(
    *,
    destination_saturation: pl.DataFrame,
    transport_zones: Any,
    resolved_activity_parameters: dict[str, Any],
    activity_ids: dict[str, int],
) -> pl.DataFrame:
    """Prepare capacity and destination-utility values for each activity and zone."""
    zone_countries = (
        pl.from_pandas(
            transport_zones.get()
            .drop("geometry", axis=1, errors="ignore")[
                ["transport_zone_id", "country"]
            ]
        )
        .select(
            pl.col("transport_zone_id").cast(pl.UInt32).alias("destination"),
            pl.col("country").cast(pl.String).alias("destination_country"),
        )
        .unique()
    )
    country_coefficients = pl.from_dicts(
        [
            {
                "activity": activity_name,
                "destination_country": destination_country,
                "country_value_coefficient": coefficient,
            }
            for activity_name, activity_parameters in resolved_activity_parameters.items()
            for destination_country, coefficient in (
                activity_parameters.country_value_coefficients or {}
            ).items()
        ],
        schema={
            "activity": pl.String,
            "destination_country": pl.String,
            "country_value_coefficient": pl.Float64,
        },
    )
    saturation_columns = set(destination_saturation.columns)
    saturation_utility = (
        pl.col("k_saturation_utility").fill_null(1.0)
        if "k_saturation_utility" in saturation_columns
        else pl.lit(1.0)
    )
    shadow_price = (
        pl.col("destination_shadow_price").fill_null(0.0)
        if "destination_shadow_price" in saturation_columns
        else pl.lit(0.0)
    )

    return (
        destination_saturation.lazy()
        .with_columns(
            activity=pl.col("activity").cast(pl.String),
            destination=pl.col("to").cast(pl.UInt32),
        )
        .join(zone_countries.lazy(), on="destination", how="left")
        .join(
            country_coefficients.lazy(),
            on=["activity", "destination_country"],
            how="left",
        )
        .with_columns(
            activity_id=pl.col("activity").replace_strict(
                activity_ids,
                return_dtype=pl.UInt32,
            ),
            country_value_coefficient=pl.col(
                "country_value_coefficient"
            ).fill_null(1.0),
            saturation_utility=saturation_utility,
            shadow_price=shadow_price,
        )
        .select(
            "activity_id",
            "destination",
            pl.col("opportunity_capacity").cast(pl.Float64),
            "country_value_coefficient",
            pl.col("saturation_utility").cast(pl.Float64),
            pl.col("shadow_price").cast(pl.Float64),
        )
        .collect(engine="streaming")
    )


def _prepare_contexts(
    *,
    activity_sequences: pl.DataFrame,
    activity_durations: pl.DataFrame,
    demand_groups: pl.DataFrame,
    activities: list[Any],
    resolved_activity_parameters: dict[str, Any],
    activity_ids: dict[str, int],
    min_activity_time_constant: float,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Prepare and deduplicate complete activity-plan contexts."""
    demand_groups = with_demand_subgroup_id(demand_groups).select(
        RAW_CONTEXT_COLUMNS[:2]
        + [
            pl.col("home_zone_id").cast(pl.UInt32),
            pl.col("country").cast(pl.String),
            pl.col("csp").cast(pl.String),
        ]
    )
    activity_durations = activity_durations.select(
        pl.col("country").cast(pl.String),
        pl.col("csp").cast(pl.String),
        pl.col("activity").cast(pl.String),
        pl.col("mean_duration_per_pers").cast(pl.Float64),
    )
    value_of_time = {
        activity_name: float(activity_parameters.value_of_time)
        for activity_name, activity_parameters in resolved_activity_parameters.items()
    }
    activities_by_name = {activity.name: activity for activity in activities}
    arrival_rigidity = {}
    for activity_name, activity_parameters in resolved_activity_parameters.items():
        rigidity = activity_parameters.arrival_time_rigidity
        if rigidity is None:
            rigidity = 1.0 if activities_by_name[activity_name].is_anchor else 0.0
        arrival_rigidity[activity_name] = float(rigidity)

    source_steps = (
        with_demand_subgroup_id(activity_sequences)
        .filter(pl.col("activity_seq_id") != 0)
        .with_columns(activity=pl.col("activity").cast(pl.String))
        .join(demand_groups, on=DEMAND_UNIT_COLS)
        .join(activity_durations, on=["country", "csp", "activity"])
        .sort(RAW_CONTEXT_COLUMNS + ["seq_step_index"])
        .with_columns(
            layer=pl.int_range(pl.len())
            .over(RAW_CONTEXT_COLUMNS)
            .cast(pl.UInt32),
            activity_id=pl.col("activity").replace_strict(
                activity_ids,
                return_dtype=pl.UInt32,
            ),
            value_of_time=pl.col("activity").replace_strict(
                value_of_time,
                return_dtype=pl.Float64,
            ),
            duration_per_person=(
                pl.col("next_departure_time") - pl.col("arrival_time")
            ).cast(pl.Float64),
            min_activity_time=(
                pl.col("mean_duration_per_pers")
                * math.exp(-min_activity_time_constant)
            ),
        )
        .with_columns(
            anchor_id=(
                pl.when(pl.col("is_anchor") & (pl.col("activity") != "home"))
                .then(pl.col("activity_id"))
                .otherwise(pl.lit(None, dtype=pl.UInt32))
            ),
            fixed_destination=(
                pl.when(pl.col("activity") == "home")
                .then(pl.col("home_zone_id"))
                .otherwise(pl.lit(None, dtype=pl.UInt32))
            ),
            arrival_time_rigidity=(
                pl.when(
                    pl.col("layer")
                    == pl.col("layer").max().over(RAW_CONTEXT_COLUMNS)
                )
                .then(pl.lit(0.0))
                .otherwise(
                    pl.col("activity").replace_strict(
                        arrival_rigidity,
                        return_dtype=pl.Float64,
                    )
                )
            ),
            origin_activity=(
                pl.col("activity")
                .shift(1)
                .over(RAW_CONTEXT_COLUMNS)
                .fill_null("home")
            ),
        )
        .with_columns(
            departure_time_rigidity=(
                pl.when(pl.col("origin_activity") == "home")
                .then(pl.lit(0.0))
                .otherwise(
                    pl.col("origin_activity").replace_strict(
                        arrival_rigidity,
                        return_dtype=pl.Float64,
                    )
                )
            )
        )
    )

    step_value_columns = [
        "layer",
        "activity_id",
        "anchor_id",
        "fixed_destination",
        "departure_time",
        "arrival_time",
        "arrival_time_rigidity",
        "departure_time_rigidity",
        "next_departure_time",
        "duration_per_person",
        "value_of_time",
        "mean_duration_per_pers",
        "min_activity_time",
    ]
    profiles = (
        source_steps.with_columns(
            step_hash=pl.struct(step_value_columns).hash(seed=17)
        )
        .group_by(RAW_CONTEXT_COLUMNS + ["home_zone_id"])
        .agg(
            sequence_hash=pl.col("step_hash")
            .sort_by("layer")
            .cast(pl.String)
            .str.join("-")
        )
        .with_columns(
            profile_key=pl.concat_str(
                [pl.col("home_zone_id").cast(pl.String), "sequence_hash"],
                separator="|",
            )
        )
    )
    unique_profiles = (
        profiles.select("profile_key")
        .unique()
        .sort("profile_key")
        .with_row_index("context_id")
        .with_columns(pl.col("context_id").cast(pl.UInt64))
    )
    raw_to_context = profiles.join(unique_profiles, on="profile_key").select(
        RAW_CONTEXT_COLUMNS + ["context_id"]
    )
    steps = (
        source_steps.join(raw_to_context, on=RAW_CONTEXT_COLUMNS)
        .sort(["context_id", "layer"])
        .unique(["context_id", "layer"], keep="first")
        .select(
            "context_id",
            "layer",
            "activity_id",
            "anchor_id",
            "fixed_destination",
            pl.col("departure_time").cast(pl.Float64),
            pl.col("next_departure_time").cast(pl.Float64),
            "duration_per_person",
            "value_of_time",
            pl.col("mean_duration_per_pers").alias(
                "mean_duration_per_person"
            ),
            "min_activity_time",
            pl.col("arrival_time").cast(pl.Float64),
            "arrival_time_rigidity",
            "departure_time_rigidity",
        )
    )
    initial_locations = (
        source_steps.join(raw_to_context, on=RAW_CONTEXT_COLUMNS)
        .select(
            "context_id",
            pl.col("home_zone_id").alias("initial_zone"),
        )
        .unique()
        .sort("context_id")
    )
    source_steps = source_steps.select(
        RAW_CONTEXT_COLUMNS
        + [
            "layer",
            "seq_step_index",
            "activity",
            "home_zone_id",
            "step_count",
            "departure_time",
            "arrival_time",
            "next_departure_time",
        ]
    )
    return steps, initial_locations, raw_to_context, source_steps
