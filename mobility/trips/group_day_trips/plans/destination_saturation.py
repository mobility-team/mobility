"""Calculate destination occupation and congestion effects."""

import logging
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step


@trace_simulation_step("destination saturation")
def get_destination_saturation(
    current_plan_steps,
    opportunities,
    resolved_activity_parameters: dict[str, Any],
    *,
    enable_opportunity_congestion: bool = True,
):
    """Recompute destination saturation per (activity, destination).

    This keeps the original ``opportunity_capacity`` and updates
    ``k_saturation_utility`` from the currently occupied activity duration
    at each destination. It is not a literal remaining-capacity stock.
    """

    logging.debug("Computing destination saturation at destinations...")

    saturation_fun_parameters = (
        pl.from_dicts(
            [
                {
                    "activity": activity_name,
                    "beta": activity_parameters.saturation_fun_beta,
                    "ref_level": activity_parameters.saturation_fun_ref_level,
                    "param_destination_soft_capacity_factor": (
                        activity_parameters.destination_soft_capacity_factor
                    ),
                    "destination_shadow_price_sensitivity": (
                        activity_parameters.destination_shadow_price_sensitivity_coefficient
                        * activity_parameters.value_of_time
                    ),
                    "destination_shadow_price_min": (
                        activity_parameters.destination_shadow_price_min_coefficient
                        * activity_parameters.value_of_time
                    ),
                    "destination_sampling_overload_gamma": (
                        activity_parameters.destination_sampling_overload_gamma
                    ),
                    "destination_sampling_min_attraction_factor": (
                        activity_parameters.destination_sampling_min_attraction_factor
                    ),
                }
                for activity_name, activity_parameters in resolved_activity_parameters.items()
            ]
        ).with_columns(activity=pl.col("activity").cast(pl.Enum(opportunities["activity"].dtype.categories)))
    )

    destination_saturation = (
        current_plan_steps.filter((pl.col("activity_seq_id") != 0) & (pl.col("activity") != "home"))
        .with_columns(activity=pl.col("activity").cast(pl.Enum(opportunities["activity"].dtype.categories)))
        # Duration is stored in Float32. Sum in a stable order and in
        # Float64 so grouping order cannot change the saturation penalty
        # used by the next simulation step.
        .sort(["to", "activity", "duration"])
        .group_by(["to", "activity"])
        .agg(opportunity_occupation=pl.col("duration").cast(pl.Float64).sum())
        .join(opportunities, on=["to", "activity"], how="full", coalesce=True)
        .join(saturation_fun_parameters, on="activity")
        .with_columns(
            destination_soft_capacity_factor=pl.col("param_destination_soft_capacity_factor")
        )
        .with_columns(opportunity_occupation=pl.col("opportunity_occupation").fill_null(0.0))
        .with_columns(
            capacity_ratio=pl.col("opportunity_occupation") / pl.col("opportunity_capacity").clip(1e-12),
            soft_capacity=(
                pl.col("destination_soft_capacity_factor")
                * pl.col("opportunity_capacity")
            ),
        )
        .with_columns(
            k_saturation_utility=(
                1.0 - pl.col("capacity_ratio").pow(pl.col("beta")) / (pl.col("ref_level").pow(pl.col("beta")))
            ).clip(0.0)
        )
        .with_columns(
            overload=pl.col("opportunity_occupation") / pl.col("soft_capacity").clip(1e-12),
        )
        .with_columns(
            destination_shadow_price=(
                pl.when(pl.col("overload") <= 1.0)
                .then(0.0)
                .otherwise(
                    -pl.col("destination_shadow_price_sensitivity")
                    * pl.col("overload").log()
                )
                .clip(pl.col("destination_shadow_price_min"), 0.0)
            )
        )
        .with_columns(
            destination_sampling_attraction_factor=(
                pl.when(pl.col("overload") <= 1.0)
                .then(1.0)
                .otherwise(
                    pl.col("overload").pow(
                        -pl.col("destination_sampling_overload_gamma")
                    )
                )
                .clip(pl.col("destination_sampling_min_attraction_factor"), 1.0)
            )
        )
        .select([
            "activity",
            "to",
            "opportunity_capacity",
            "opportunity_occupation",
            "capacity_ratio",
            "destination_soft_capacity_factor",
            "k_saturation_utility",
            "destination_shadow_price",
            "destination_sampling_overload_gamma",
            "destination_sampling_min_attraction_factor",
            "destination_sampling_attraction_factor",
        ])
    )

    if not enable_opportunity_congestion:
        # Preserve occupation for reporting, but remove its effects on
        # utility and on the choice of candidate destinations.
        destination_saturation = destination_saturation.with_columns(
            k_saturation_utility=pl.lit(1.0),
            destination_shadow_price=pl.lit(0.0),
            destination_sampling_attraction_factor=pl.lit(1.0),
        )
    return destination_saturation

def ensure_destination_saturation_columns(destination_saturation: pl.DataFrame) -> pl.DataFrame:
    """Add shadow-price columns when an older saturation table is provided."""
    columns = set(destination_saturation.columns)
    expressions = []
    if "k_saturation_utility" not in columns:
        expressions.append(pl.lit(1.0, dtype=pl.Float64).alias("k_saturation_utility"))
    if "destination_shadow_price" not in columns:
        expressions.append(pl.lit(0.0, dtype=pl.Float64).alias("destination_shadow_price"))
    if "destination_sampling_overload_gamma" not in columns:
        expressions.append(
            pl.lit(1.5, dtype=pl.Float64).alias("destination_sampling_overload_gamma")
        )
    if "destination_sampling_min_attraction_factor" not in columns:
        expressions.append(
            pl.lit(0.05, dtype=pl.Float64).alias(
                "destination_sampling_min_attraction_factor"
            )
        )
    if "destination_sampling_attraction_factor" not in columns:
        expressions.append(
            pl.lit(1.0, dtype=pl.Float64).alias(
                "destination_sampling_attraction_factor"
            )
        )
    if "opportunity_occupation" not in columns:
        expressions.append(pl.lit(0.0, dtype=pl.Float64).alias("opportunity_occupation"))
    if "capacity_ratio" not in columns:
        expressions.append(pl.lit(0.0, dtype=pl.Float64).alias("capacity_ratio"))
    if "destination_soft_capacity_factor" not in columns:
        expressions.append(pl.lit(1.25, dtype=pl.Float64).alias("destination_soft_capacity_factor"))
    if not expressions:
        return destination_saturation
    return destination_saturation.with_columns(expressions)
