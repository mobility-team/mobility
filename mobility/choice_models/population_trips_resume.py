import logging
from dataclasses import dataclass

import polars as pl
import pandas as pd

from mobility.choice_models.population_trips_checkpoint import PopulationTripsCheckpointAsset
from mobility.transport_costs.od_flows_asset import VehicleODFlowsAsset


@dataclass(frozen=True)
class ResumePlan:
    """Plan for resuming a PopulationTrips run."""

    run_key: str
    is_weekday: bool
    resume_from_iter: int | None  # last completed iteration to resume from (k), or None
    start_iteration: int          # first iteration to compute (k+1, or 1)


def compute_resume_plan(*, run_key: str, is_weekday: bool, n_iterations: int) -> ResumePlan:
    """Computes the resume plan for a PopulationTrips run.

    This inspects the checkpoint folder and returns:
    - the last completed iteration (k), if a checkpoint exists
    - the next iteration to compute (k+1), or 1 when no checkpoint exists

    Note: If k == n_iterations, then start_iteration == n_iterations + 1 and
    callers should treat the iteration loop as complete (no-op).

    Args:
        run_key: Hash-like identifier for the run. Must match
            PopulationTrips.inputs_hash.
        is_weekday: Whether this is the weekday simulation (True) or weekend (False).
        n_iterations: Total number of iterations configured for this run.

    Returns:
        ResumePlan describing whether to resume and which iteration to start from.
    """
    latest = PopulationTripsCheckpointAsset.find_latest_checkpoint_iter(
        run_key=run_key,
        is_weekday=is_weekday,
    )
    if latest is None:
        return ResumePlan(run_key=run_key, is_weekday=is_weekday, resume_from_iter=None, start_iteration=1)

    k = min(int(latest), int(n_iterations))
    return ResumePlan(run_key=run_key, is_weekday=is_weekday, resume_from_iter=k, start_iteration=k + 1)


def try_load_checkpoint(*, run_key: str, is_weekday: bool, iteration: int):
    """Loads a checkpoint payload (best-effort).

    Args:
        run_key: Run identifier.
        is_weekday: Weekday/weekend selector.
        iteration: Iteration number k (last completed).

    Returns:
        The checkpoint payload dict as returned by PopulationTripsCheckpointAsset.get(),
        or None if loading fails for any reason.
    """
    try:
        return PopulationTripsCheckpointAsset(
            run_key=run_key,
            is_weekday=is_weekday,
            iteration=iteration,
        ).get()
    except Exception:
        logging.exception("Failed to load checkpoint (run_key=%s, is_weekday=%s, iteration=%s).", run_key, str(is_weekday), str(iteration))
        return None


def restore_state_or_fresh_start(
    *,
    ckpt,
    stay_home_state: pl.DataFrame,
    sinks: pl.DataFrame,
    rng,
):
    """Restores iteration state from a checkpoint or returns a clean start state.

    This is the core of resume correctness: to continue deterministically, both
    the model state and the RNG state must be restored.

    Args:
        ckpt: Checkpoint payload dict (or None) returned by try_load_checkpoint().
        stay_home_state: Baseline "stay home" state used to build a clean start.
        sinks: Initial sinks; used to build a clean start remaining_sinks.
        rng: random.Random instance to restore with rng.setstate(...).

    Returns:
        Tuple of:
        - current_states: pl.DataFrame
        - remaining_sinks: pl.DataFrame
        - restored: bool indicating whether checkpoint restoration succeeded.
    """

    fresh_current_states = (
        stay_home_state
        .select(["demand_group_id", "iteration", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility", "n_persons"])
        .clone()
    )
    fresh_remaining_sinks = sinks.clone()

    if ckpt is None:
        return fresh_current_states, fresh_remaining_sinks, False

    try:
        rng.setstate(ckpt["rng_state"])
    except Exception:
        logging.exception("Failed to restore RNG state from checkpoint; restarting from scratch.")
        return fresh_current_states, fresh_remaining_sinks, False

    return ckpt["current_states"], ckpt["remaining_sinks"], True


def prune_tmp_artifacts(*, tmp_folders, keep_up_to_iter: int) -> None:
    """Deletes temp artifacts beyond the last completed iteration.

    If a run crashed mid-iteration, temp parquet files for that iteration may
    exist. This ensures we don't accidentally reuse partial artifacts on resume.

    Args:
        tmp_folders: Dict of temp folders produced by PopulationTrips.prepare_tmp_folders().
        keep_up_to_iter: Last completed iteration k; any artifacts for >k are removed.
    """
    try:
        for p in tmp_folders["spatialized-chains"].glob("spatialized_chains_*.parquet"):
            it = int(p.stem.split("_")[-1])
            if it > keep_up_to_iter:
                p.unlink(missing_ok=True)
        for p in tmp_folders["modes"].glob("mode_sequences_*.parquet"):
            it = int(p.stem.split("_")[-1])
            if it > keep_up_to_iter:
                p.unlink(missing_ok=True)
    except Exception:
        logging.exception("Failed to prune temp artifacts on resume. Continuing anyway.")


def rehydrate_congestion_snapshot(
    *,
    costs_aggregator,
    run_key: str,
    last_completed_iter: int,
    n_iter_per_cost_update: int,
):
    """Rehydrates congestion snapshot state for deterministic resume.

    The model stores a pointer to the "current congestion snapshot" in-memory.
    After a crash/restart, that pointer is lost, even though the snapshot files
    are cached on disk. This function reloads the last applicable flow asset and
    re-applies it so that subsequent cost lookups use the same congested costs
    as an uninterrupted run.

    Args:
        costs_aggregator: TravelCostsAggregator instance from PopulationTrips inputs.
        run_key: Run identifier (PopulationTrips.inputs_hash).
        last_completed_iter: Last completed iteration k.
        n_iter_per_cost_update: Update cadence. 0 means no congestion feedback.

    Returns:
        A costs dataframe from costs_aggregator.get(...), using congested costs
        when rehydration succeeds, or falling back to free-flow on failure.
    """
    if n_iter_per_cost_update <= 0 or last_completed_iter < 1:
        return costs_aggregator.get(congestion=False)

    last_update_iter = 1 + ((last_completed_iter - 1) // n_iter_per_cost_update) * n_iter_per_cost_update
    if last_update_iter < 1:
        return costs_aggregator.get(congestion=False)

    try:
        # Load the existing flow asset for the last congestion update iteration.
        flow_asset = VehicleODFlowsAsset(
            vehicle_od_flows=pd.DataFrame({"from": [], "to": [], "vehicle_volume": []}),
            run_key=run_key,
            iteration=last_update_iter,
            mode_name="car",
        )
        flow_asset.get()

        # Apply snapshot to the road mode so get(congestion=True) is aligned.
        for mode in costs_aggregator.modes:
            if getattr(mode, "congestion", False) and getattr(mode, "name", None) == "car":
                # Restore the in-memory pointer to the correct congestion snapshot.
                mode.travel_costs.apply_flow_snapshot(flow_asset)
                break

        return costs_aggregator.get(congestion=True)
    except Exception:
        logging.exception("Failed to rehydrate congestion snapshot on resume; falling back to free-flow costs until next update.")
        return costs_aggregator.get(congestion=False)
