"""Compare context grouping with membership-first grouping on captured rows.

Run ``pixi run python tests/manual/benchmark_candidate_compaction.py FOLDER``
after capturing an input with ``capture_candidate_compaction.py``. Splitting
the captured rows is measured separately: a useful production design must
retain these tables upstream instead of paying to split them in every batch.
"""

import argparse
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.compact_candidate_steps import (
    MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS, STEP_KEY_COLUMNS,
)
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
folder = parser.parse_args().folder
source = pl.scan_parquet(folder / "candidate-steps.parquet")
contexts = ScoringContexts(pl.DataFrame(), pl.read_parquet(folder / "unit-map.parquet"))

started = perf_counter()
reference = contexts.collapse_step_rows(source)
print("CURRENT_SECONDS", round(perf_counter() - started, 3), "ROWS", reference.height, flush=True)

started = perf_counter()
steps = source.select(STEP_COLUMNS).unique().collect(engine="streaming")
members = source.select(MEMBERSHIP_COLUMNS).unique().collect(engine="streaming")
print("SPLIT_SECONDS", round(perf_counter() - started, 3), "STEPS", steps.height, "MEMBERS", members.height, flush=True)
step_conflicts = steps.group_by(STEP_KEY_COLUMNS).len().filter(pl.col("len") > 1)
member_conflicts = members.group_by(PLAN_KEY_COLS).len().filter(pl.col("len") > 1)
if step_conflicts.height or member_conflicts.height:
    raise ValueError(
        f"The captured rows cannot be represented without loss: "
        f"{step_conflicts.height} conflicting steps and {member_conflicts.height} conflicting memberships."
    )

started = perf_counter()
compact_members = contexts._replace_unit_keys(members).group_by(PLAN_KEY_COLS).agg(
    pl.col("country").first(), pl.col("csp").first(), pl.col("iteration").first(),
    pl.col("first_seen_iteration").min(), pl.col("last_seen_iteration").max(),
    pl.col("last_active_iteration").max(),
).collect(engine="streaming")
grouped = perf_counter()
actual = compact_members.join(steps, on=PLAN_COLUMNS, how="inner").select(source.collect_schema().names())
print("GROUP_MEMBERS_SECONDS", round(grouped - started, 3), "MEMBERS", compact_members.height, flush=True)
print("ATTACH_STEPS_SECONDS", round(perf_counter() - grouped, 3), "ROWS", actual.height, flush=True)
keys = PLAN_KEY_COLS + ["seq_step_index"]
assert_frame_equal(reference.sort(keys), actual.sort(keys))
print("EXACT_MATCH", flush=True)
