"""Optional measurements for the cache-only Grand Geneve effort probe.

The measurements add work and do not alter the simulation's choices. Candidate
identities and modeled plan keys differ after timing updates, so their counts
and volumes are deliberately reported separately.
"""
import json

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePool


def install_effort_diagnostics(folder, revision_probability):
    folder.mkdir(exist_ok=True)
    previous_pool = {}
    seen_allocations = {}
    original_pool = SharedCandidatePool.build
    original_estimate = PlanUpdater.estimate_reachable_allocation
    original_apply = PlanUpdater.apply_transitions

    def labels():
        active = _ACTIVE_TRACE.get()
        return active[2] if active else {}

    def append(name, record):
        file = (folder / name).open("a", encoding="utf-8")
        try:
            file.write(json.dumps(record) + "\n")
        finally:
            file.close()

    def build(pool, *args, **kwargs):
        result = original_pool(pool, *args, **kwargs)
        info = labels()
        step, batch = info.get("step"), info.get("search_batch")
        if step is not None and batch is not None:
            current = result.candidate_ids
            previous = previous_pool.get(step)
            new = current.height if previous is None else current.join(previous, on=current.columns, how="anti").height
            previous_pool[step] = current if previous is None else pl.concat([previous, current]).unique()
            append("candidate-batches.jsonl", {"step": step, "batch": batch,
                   "pool_candidates": current.height, "first_seen_in_step": new})
        return result

    def estimate(updater, *args, **kwargs):
        allocation = original_estimate(updater, *args, **kwargs)
        info = labels()
        step, batch, scenario = info.get("step"), info.get("search_batch"), info.get("scenario")
        if step is not None and batch is not None:
            key = (step, scenario)
            current = allocation.filter((pl.col("mode_seq_id") != 0) & (pl.col("n_persons") > 0))
            current = current.select(PLAN_KEY_COLS + ["n_persons"])
            seen = seen_allocations.get(key)
            if seen is None:
                seen = current.select(PLAN_KEY_COLS).with_columns(first_batch=pl.lit(batch))
            else:
                new = current.select(PLAN_KEY_COLS).join(seen.select(PLAN_KEY_COLS), on=PLAN_KEY_COLS, how="anti")
                seen = pl.concat([seen, new.with_columns(first_batch=pl.lit(batch))])
            seen_allocations[key] = seen
            contribution = (
                current.join(seen, on=PLAN_KEY_COLS, how="inner")
                .group_by("first_batch").agg(
                    allocated_options=pl.len(),
                    allocated_persons=pl.col("n_persons").sort().implode().list.sum(),
                ).sort("first_batch").to_dicts()
            )
            append("allocated-options.jsonl", {"step": step, "batch": batch,
                   "scenario": scenario, "by_first_allocated_batch": contribution})
        return allocation

    def apply(updater, *args, **kwargs):
        if kwargs.get("behavior_change_scope") is None:
            return original_apply(updater, *args, **kwargs)
        requested_events = kwargs.get("include_events", True)
        kwargs["include_events"] = True
        plans, events = original_apply(updater, *args, **kwargs)
        changed = ~pl.col("is_self_transition")
        floor = pl.any_horizontal([
            pl.col("chosen_" + column) != pl.col(column + "_trans")
            for column in ("activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id")
        ])
        summary = events.select(
            pl.col("n_persons_moved").sum().alias("people"),
            pl.when(changed).then(pl.col("n_persons_moved")).otherwise(0.0).sum().alias("changed_people"),
            pl.when(floor).then(pl.col("n_persons_moved")).otherwise(0.0).sum().alias("floor_reassigned_people"),
            pl.when(pl.col("adjustment_factor") == 1.0).then(pl.col("n_persons_moved"))
              .otherwise(0.0).sum().alias("forced_reassigned_people"),
        ).collect(engine="streaming").row(0, named=True)
        # At revision probability 1, forced and ordinary revisions have the
        # same adjustment factor and cannot be distinguished in these events.
        if revision_probability == 1.0:
            summary["forced_reassigned_people"] = None
        info = labels()
        summary.update(step=info.get("step"), scenario=info.get("scenario"))
        append("transitions.jsonl", summary)
        return plans, events if requested_events else None

    SharedCandidatePool.build = build
    PlanUpdater.estimate_reachable_allocation = estimate
    PlanUpdater.apply_transitions = apply
