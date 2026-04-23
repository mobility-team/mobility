import polars as pl

from mobility.trips.group_day_trips.core.parameters import (
    BehaviorChangeScope,
    Parameters,
)


def get_spatialized_chains(
        behavior_change_scope: BehaviorChangeScope,
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame | None,
        destination_sequence_sampler,
        activities,
        transport_zones,
        remaining_opportunities: pl.DataFrame,
        iteration: int,
        chains_by_activity: pl.DataFrame,
        demand_groups: pl.DataFrame,
        costs: pl.DataFrame,
        parameters: Parameters,
        seed: int,
    ) -> pl.DataFrame:
    """Get spatialized chains for the current simulation step.

    Args:
        behavior_change_scope: Active behavior-change scope for the step.
        current_plans: Aggregate plans occupied before the step update.
        current_plan_steps: Per-step rows for the currently occupied plans.
        destination_sequence_sampler: Sampler used when destination resampling
            is allowed.
        activities: Available activities for the simulation.
        transport_zones: Transport zones used to spatialize destinations.
        remaining_opportunities: Remaining destination capacities.
        iteration: Current simulation iteration number.
        chains_by_activity: Full chain templates indexed by activity sequence.
        demand_groups: Demand-group metadata used during spatialization.
        costs: Current OD costs used by destination sampling.
        parameters: PopulationTrips parameters.
        seed: RNG seed used for destination sampling.

    Returns:
        Spatialized chains to use for the current step.
    """
    if behavior_change_scope == BehaviorChangeScope.FULL_REPLANNING:
        chains_to_sample = chains_by_activity
    elif behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
        chains_to_sample = get_active_activity_chains(
            chains_by_activity=chains_by_activity,
            current_plans=current_plans,
        )
    elif behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
        return get_active_destination_sequences(
            current_plans=current_plans,
            current_plan_steps=current_plan_steps,
            iteration=iteration,
        )
    else:
        raise ValueError(f"Unsupported behavior change scope: {behavior_change_scope}")

    if chains_to_sample.height == 0:
        if get_active_non_stay_home_plans(current_plans).height > 0:
            raise ValueError(
                "No chains available for active non-stay-home states at "
                f"iteration={iteration} with behavior_change_scope={behavior_change_scope.value}."
            )
        return empty_spatialized_chains()

    return destination_sequence_sampler.run(
        activities,
        transport_zones,
        remaining_opportunities,
        chains_to_sample,
        demand_groups,
        costs,
        parameters,
        seed,
    )


def get_mode_sequences(
        spatialized_chains: pl.DataFrame,
        top_k_mode_sequence_search,
        iteration: int,
        costs_aggregator,
        tmp_folders: dict[str, object],
        parameters: Parameters,
    ) -> pl.DataFrame:
    """Get mode sequences for the current simulation step.

    Args:
        spatialized_chains: Spatialized chains selected for the current step.
        top_k_mode_sequence_search: Searcher that computes top-k mode
            sequences for each spatialized chain.
        iteration: Current simulation iteration number.
        costs_aggregator: Provides OD costs by transport mode.
        tmp_folders: Temporary folders for intermediate iteration artifacts.
        parameters: GroupDayTrips parameters.

    Returns:
        Mode sequences to use for the current step.
    """
    if spatialized_chains.height == 0:
        return empty_mode_sequences()

    return top_k_mode_sequence_search.run(
        iteration,
        costs_aggregator,
        tmp_folders,
        parameters,
    )


def get_active_activity_chains(
        chains_by_activity: pl.DataFrame,
        current_plans: pl.DataFrame,
    ) -> pl.DataFrame:
    """Keep chain templates for activity sequences currently selected.

    Args:
        chains_by_activity: Full chain-template table.
        current_plans: Aggregate plans occupied before the step update.

    Returns:
        Chain templates restricted to active non-stay-home activity sequences.
    """
    active_activity_sequences = get_active_non_stay_home_plans(current_plans).select(
        ["demand_group_id", "activity_seq_id"]
    ).unique()

    if active_activity_sequences.height == 0:
        return chains_by_activity.head(0)

    active_activity_sequences = active_activity_sequences.with_columns(
        demand_group_id=pl.col("demand_group_id").cast(chains_by_activity.schema["demand_group_id"]),
        activity_seq_id=pl.col("activity_seq_id").cast(chains_by_activity.schema["activity_seq_id"]),
    )

    return chains_by_activity.join(
        active_activity_sequences,
        on=["demand_group_id", "activity_seq_id"],
        how="inner",
    )


def get_active_destination_sequences(
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame | None,
        iteration: int,
    ) -> pl.DataFrame:
    """Reuse active destination sequences and tag them for a new iteration.

    Args:
        current_plans: Aggregate plans occupied before the step update.
        current_plan_steps: Per-step rows for the currently occupied plans.
        iteration: Current simulation iteration number.

    Returns:
        Destination sequences matching the active non-stay-home destination
        sequences, restamped with the current iteration.
    """
    active_dest_sequences = get_active_non_stay_home_plans(current_plans).select(
        ["demand_group_id", "activity_seq_id", "dest_seq_id"]
    ).unique()

    if active_dest_sequences.height == 0:
        return empty_spatialized_chains()

    if current_plan_steps is None:
        raise ValueError(
            "No current plan steps available for active non-stay-home "
            f"states at iteration={iteration}."
        )

    active_dest_sequences = active_dest_sequences.with_columns(
        demand_group_id=pl.col("demand_group_id").cast(pl.UInt32),
        activity_seq_id=pl.col("activity_seq_id").cast(pl.UInt32),
        dest_seq_id=pl.col("dest_seq_id").cast(pl.UInt32),
    )

    reused = (
        current_plan_steps.lazy()
        .join(
            active_dest_sequences.lazy(),
            on=["demand_group_id", "activity_seq_id", "dest_seq_id"],
            how="inner",
        )
        .with_columns(iteration=pl.lit(iteration).cast(pl.UInt32()))
        .select(
            [
                "demand_group_id",
                "activity_seq_id",
                "dest_seq_id",
                "seq_step_index",
                "from",
                "to",
                "iteration",
            ]
        )
        .unique(
            subset=["demand_group_id", "activity_seq_id", "dest_seq_id", "seq_step_index"],
            keep="first",
        )
        .collect(engine="streaming")
    )

    if reused.height == 0:
        raise ValueError(
            "Active non-stay-home states could not be matched to reusable "
            f"destination chains at iteration={iteration}."
        )

    return reused


def get_active_non_stay_home_plans(current_plans: pl.DataFrame) -> pl.DataFrame:
    """Get active non-stay-home plans from the current aggregate plan table.

    Args:
        current_plans: Aggregate plans occupied before the step update.

    Returns:
        Distinct active non-stay-home plan keys.
    """
    return (
        current_plans
        .filter(pl.col("activity_seq_id") != 0)
        .select(["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"])
        .unique()
    )


def empty_spatialized_chains() -> pl.DataFrame:
    """Create an empty spatialized-chains table with the expected schema.

    Returns:
        Empty spatialized chains.
    """
    return pl.DataFrame(
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "from": pl.Int32,
            "to": pl.Int32,
            "iteration": pl.UInt32,
        }
    )


def empty_mode_sequences() -> pl.DataFrame:
    """Create an empty mode-sequences table with the expected schema.

    Returns:
        Empty mode sequences.
    """
    return pl.DataFrame(
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "mode": pl.Utf8,
            "iteration": pl.UInt32,
        }
    )
