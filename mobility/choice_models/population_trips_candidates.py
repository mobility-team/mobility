import pathlib

import polars as pl

from mobility.choice_models.population_trips_parameters import (
    BehaviorChangeScope,
    PopulationTripsParameters,
)


def get_spatialized_chains(
        behavior_change_scope: BehaviorChangeScope,
        current_states: pl.DataFrame,
        destination_sequence_sampler,
        motives,
        transport_zones,
        remaining_sinks: pl.DataFrame,
        iteration: int,
        chains_by_motive: pl.DataFrame,
        demand_groups: pl.DataFrame,
        costs: pl.DataFrame,
        tmp_folders: dict[str, pathlib.Path],
        parameters: PopulationTripsParameters,
        seed: int,
    ) -> pl.DataFrame:
    """Get spatialized chains for the current simulation step.

    Args:
        behavior_change_scope: Active behavior-change scope for the step.
        current_states: Aggregate states occupied before the step update.
        destination_sequence_sampler: Sampler used when destination resampling
            is allowed.
        motives: Available motives for the simulation.
        transport_zones: Transport zones used to spatialize destinations.
        remaining_sinks: Remaining destination capacities.
        iteration: Current simulation iteration number.
        chains_by_motive: Full chain templates indexed by motive sequence.
        demand_groups: Demand-group metadata used during spatialization.
        costs: Current OD costs used by destination sampling.
        tmp_folders: Temporary folders for intermediate iteration artifacts.
        parameters: PopulationTrips parameters.
        seed: RNG seed used for destination sampling.

    Returns:
        Spatialized chains to use for the current step.
    """
    if behavior_change_scope == BehaviorChangeScope.FULL_REPLANNING:
        chains_to_sample = chains_by_motive
    elif behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
        chains_to_sample = get_active_motive_chains(
            chains_by_motive=chains_by_motive,
            current_states=current_states,
        )
    elif behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
        return get_active_destination_sequences(
            current_states=current_states,
            iteration=iteration,
            tmp_folders=tmp_folders,
        )
    else:
        raise ValueError(f"Unsupported behavior change scope: {behavior_change_scope}")

    if chains_to_sample.height == 0:
        if get_active_non_stay_home_states(current_states).height > 0:
            raise ValueError(
                "No chains available for active non-stay-home states at "
                f"iteration={iteration} with behavior_change_scope={behavior_change_scope.value}."
            )
        return empty_spatialized_chains()

    return destination_sequence_sampler.run(
        motives,
        transport_zones,
        remaining_sinks,
        iteration,
        chains_to_sample,
        demand_groups,
        costs,
        tmp_folders,
        parameters,
        seed,
    )


def get_mode_sequences(
        spatialized_chains: pl.DataFrame,
        top_k_mode_sequence_search,
        iteration: int,
        costs_aggregator,
        tmp_folders: dict[str, pathlib.Path],
        parameters: PopulationTripsParameters,
    ) -> pl.DataFrame:
    """Get mode sequences for the current simulation step.

    Args:
        spatialized_chains: Spatialized chains selected for the current step.
        top_k_mode_sequence_search: Searcher that computes top-k mode
            sequences for each spatialized chain.
        iteration: Current simulation iteration number.
        costs_aggregator: Provides OD costs by transport mode.
        tmp_folders: Temporary folders for intermediate iteration artifacts.
        parameters: PopulationTrips parameters.

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


def get_active_motive_chains(
        chains_by_motive: pl.DataFrame,
        current_states: pl.DataFrame,
    ) -> pl.DataFrame:
    """Keep chain templates for motive sequences currently selected.

    Args:
        chains_by_motive: Full chain-template table.
        current_states: Aggregate states occupied before the step update.

    Returns:
        Chain templates restricted to active non-stay-home motive sequences.
    """
    active_motive_sequences = get_active_non_stay_home_states(current_states).select(
        ["demand_group_id", "motive_seq_id"]
    ).unique()

    if active_motive_sequences.height == 0:
        return chains_by_motive.head(0)

    active_motive_sequences = active_motive_sequences.with_columns(
        demand_group_id=pl.col("demand_group_id").cast(chains_by_motive.schema["demand_group_id"]),
        motive_seq_id=pl.col("motive_seq_id").cast(chains_by_motive.schema["motive_seq_id"]),
    )

    return chains_by_motive.join(
        active_motive_sequences,
        on=["demand_group_id", "motive_seq_id"],
        how="inner",
    )


def get_active_destination_sequences(
        current_states: pl.DataFrame,
        iteration: int,
        tmp_folders: dict[str, pathlib.Path],
    ) -> pl.DataFrame:
    """Reuse active destination sequences and tag them for a new iteration.

    Args:
        current_states: Aggregate states occupied before the step update.
        iteration: Current simulation iteration number.
        tmp_folders: Temporary folders containing prior spatialized chains.

    Returns:
        Spatialized chains matching the active non-stay-home destination
        sequences, restamped with the current iteration.
    """
    active_dest_sequences = get_active_non_stay_home_states(current_states).select(
        ["demand_group_id", "motive_seq_id", "dest_seq_id"]
    ).unique()

    if active_dest_sequences.height == 0:
        return empty_spatialized_chains()

    available_chains = get_latest_spatialized_chains(tmp_folders)
    if available_chains is None:
        raise ValueError(
            "No prior spatialized chains available for active non-stay-home "
            f"states at iteration={iteration}."
        )

    active_dest_sequences = active_dest_sequences.with_columns(
        demand_group_id=pl.col("demand_group_id").cast(pl.UInt32),
        motive_seq_id=pl.col("motive_seq_id").cast(pl.UInt32),
        dest_seq_id=pl.col("dest_seq_id").cast(pl.UInt32),
    )

    reused = (
        available_chains
        .join(
            active_dest_sequences.lazy(),
            on=["demand_group_id", "motive_seq_id", "dest_seq_id"],
            how="inner",
        )
        .with_columns(iteration=pl.lit(iteration).cast(pl.UInt32()))
        .collect(engine="streaming")
    )

    if reused.height == 0:
        raise ValueError(
            "Active non-stay-home states could not be matched to reusable "
            f"destination chains at iteration={iteration}."
        )

    return reused


def get_active_non_stay_home_states(current_states: pl.DataFrame) -> pl.DataFrame:
    """Get active non-stay-home states from the current aggregate state table.

    Args:
        current_states: Aggregate states occupied before the step update.

    Returns:
        Distinct active non-stay-home state keys.
    """
    return (
        current_states
        .filter(pl.col("motive_seq_id") != 0)
        .select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
        .unique()
    )


def get_latest_spatialized_chains(tmp_folders: dict[str, pathlib.Path]) -> pl.LazyFrame | None:
    """Load the latest available spatialized chains across prior iterations.

    Args:
        tmp_folders: Temporary folders containing spatialized-chain parquet
            files.

    Returns:
        A lazy frame containing the most recent row for each state-step key, or
        ``None`` if no spatialized chains have been saved yet.
    """
    if not any(tmp_folders["spatialized-chains"].glob("spatialized_chains_*.parquet")):
        return None

    pattern = tmp_folders["spatialized-chains"] / "spatialized_chains_*.parquet"
    return (
        pl.scan_parquet(str(pattern))
        .sort("iteration", descending=True)
        .unique(
            subset=["demand_group_id", "motive_seq_id", "dest_seq_id", "seq_step_index"],
            keep="first",
        )
    )


def empty_spatialized_chains() -> pl.DataFrame:
    """Create an empty spatialized-chains table with the expected schema.

    Returns:
        Empty spatialized chains.
    """
    return pl.DataFrame(
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
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
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "mode": pl.Utf8,
            "iteration": pl.UInt32,
        }
    )
