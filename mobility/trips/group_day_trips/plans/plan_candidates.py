import polars as pl

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope


def get_active_non_stay_home_plans(current_plans: pl.DataFrame) -> pl.DataFrame:
    """Return distinct active non-stay-home plan keys."""
    return (
        current_plans
        .filter(pl.col("activity_seq_id") != 0)
        .select(["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"])
        .unique()
    )


def get_active_activity_chains(
    chains_by_activity: pl.DataFrame,
    current_plans: pl.DataFrame,
) -> pl.DataFrame:
    """Keep chain templates for activity sequences currently selected."""
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


def empty_destination_sequences() -> pl.DataFrame:
    """Create an empty destination-sequences table with the expected schema."""
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


def get_active_destination_sequences(
    current_plans: pl.DataFrame,
    current_plan_steps: pl.DataFrame | None,
    iteration: int,
) -> pl.DataFrame:
    """Reuse active destination sequences and tag them for a new iteration."""
    active_dest_sequences = get_active_non_stay_home_plans(current_plans).select(
        ["demand_group_id", "activity_seq_id", "dest_seq_id"]
    ).unique()

    if active_dest_sequences.height == 0:
        return empty_destination_sequences()

    if current_plan_steps is None:
        raise ValueError(
            "No current plan steps available for active non-stay-home "
            f"plans at iteration={iteration}."
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
            "Active non-stay-home plans could not be matched to reusable "
            f"destination sequences at iteration={iteration}."
        )

    return reused


def get_destination_sequences_for_scope(
    *,
    behavior_change_scope: BehaviorChangeScope,
    current_plans: pl.DataFrame,
    current_plan_steps: pl.DataFrame | None,
    iteration: int,
    chains_by_activity: pl.DataFrame,
    sample_destination_sequences,
) -> pl.DataFrame:
    """Return the destination sequences allowed by the active scope."""
    if behavior_change_scope == BehaviorChangeScope.FULL_REPLANNING:
        return sample_destination_sequences(chains_by_activity)

    if behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
        filtered_chains = get_active_activity_chains(
            chains_by_activity=chains_by_activity,
            current_plans=current_plans,
        )
        if filtered_chains.height == 0:
            return empty_destination_sequences()
        return sample_destination_sequences(filtered_chains)

    if behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
        return get_active_destination_sequences(
            current_plans=current_plans,
            current_plan_steps=current_plan_steps,
            iteration=iteration,
        )

    raise ValueError(f"Unsupported behavior change scope: {behavior_change_scope}")


def get_transition_scope_inputs(
    *,
    current_plans: pl.DataFrame,
    possible_plan_utility: pl.LazyFrame,
    behavior_change_scope: BehaviorChangeScope,
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.Expr]:
    """Return scope-filtered transition inputs and the pairing constraint."""
    current_plans_for_transitions = current_plans.lazy()
    possible_plan_utility_for_transitions = possible_plan_utility

    if behavior_change_scope != BehaviorChangeScope.FULL_REPLANNING:
        current_plans_for_transitions = current_plans_for_transitions.filter(pl.col("mode_seq_id") != 0)
        possible_plan_utility_for_transitions = possible_plan_utility_for_transitions.filter(
            pl.col("mode_seq_id") != 0
        )

    scope_pair_constraint = pl.lit(True)
    if behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
        scope_pair_constraint = pl.col("activity_seq_id") == pl.col("activity_seq_id_trans")
    elif behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
        scope_pair_constraint = (
            (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
            & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
        )

    return current_plans_for_transitions, possible_plan_utility_for_transitions, scope_pair_constraint
