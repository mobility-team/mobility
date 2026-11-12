"""Compare step-scoring lookup strategies on captured candidate rows.

The candidate keys are real, while transport costs and activity parameters are
constructed for this focused benchmark. Run each method in a separate process:

    pixi run python tests/manual/benchmark_scoring_lookup.py CAPTURE baseline
    pixi run python tests/manual/benchmark_scoring_lookup.py CAPTURE compact
    pixi run python tests/manual/benchmark_scoring_lookup.py CAPTURE reuse

This measures only the pre-timing scoring lookup, not complete plan utilities,
inner allocation or a full simulation. The capture has two batches for one
scenario; output fingerprints make it possible to compare the methods.
"""

import argparse
import ast
import subprocess
import threading
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace

import pandas as pd
import polars as pl
import psutil

import mobility.trips.group_day_trips.plans.plan_updater as updater_module
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringLookup, ScoringMemoryBudget


parser = argparse.ArgumentParser()
parser.add_argument("capture", type=Path)
parser.add_argument("method", choices=["baseline", "compact", "reuse"])
args = parser.parse_args()
paths = [args.capture / f"step-4-batch-{batch}-carbon_tax.parquet" for batch in (1, 2)]
inputs = [pl.scan_parquet(path) for path in paths]
combined = pl.concat(inputs)

# The capture contains the real candidate keys. Construct small lookup tables
# from them so the same work can be repeated without loading a model state.
cost_keys = ["utility_profile_id", "from", "to", "mode"]
costs = combined.select(cost_keys).unique().with_columns(
    cost=(pl.col("from").cast(pl.Float64) + pl.col("to")) / 100.0,
    distance=pl.lit(10.0), time=pl.lit(1.0),
).collect(engine="streaming")
activity_dur = combined.select("country", "csp", "activity").unique().with_columns(
    mean_duration_per_pers=pl.lit(8.0),
).collect(engine="streaming")
saturation = combined.select("to", "activity").unique().with_columns(
    k_saturation_utility=pl.lit(1.0), destination_shadow_price=pl.lit(0.0),
).collect(engine="streaming")
zones = combined.select(pl.col("to").alias("transport_zone_id")).unique().with_columns(
    country=pl.lit("fr"),
).collect(engine="streaming").to_pandas()
activities = activity_dur["activity"].dtype.categories.to_list()
parameters = {
    activity: SimpleNamespace(value_of_time=1.0, country_value_coefficients={})
    for activity in activities
}
modes = [
    SimpleNamespace(inputs={"parameters": SimpleNamespace(name=name)})
    for name in inputs[0].collect_schema()["mode"].categories.to_list()
    if name != "stay_home"
]
transport = SimpleNamespace(
    modes=modes,
    get_profile_costs_by_od_and_mode=lambda profiles, columns: costs,
)


def candidate_rows(frame):
    return frame.with_columns(
        demand_group_id=pl.lit(0, dtype=pl.UInt32),
        demand_subgroup_id=pl.lit(0, dtype=pl.UInt32),
        iteration=pl.lit(4, dtype=pl.UInt16),
        first_seen_iteration=pl.lit(None, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(None, dtype=pl.UInt16),
        last_active_iteration=pl.lit(None, dtype=pl.UInt16),
    ).select(
        CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
        + CandidatePlanStepsAsset.RETENTION_COLUMNS + ["utility_profile_id"]
    )


updater = PlanUpdater()
if args.method == "baseline":
    # Load the committed pre-change function, keeping this comparison outside
    # production and avoiding a permanent second scoring implementation.
    source = subprocess.run(
        ["git", "show", "HEAD:mobility/trips/group_day_trips/plans/plan_updater.py"],
        check=True, capture_output=True, text=True,
    ).stdout
    module = ast.parse(source)
    cls = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "PlanUpdater")
    function = next(node for node in cls.body if isinstance(node, ast.FunctionDef)
                    and node.name == "compute_plan_steps_candidates_utility")
    function.decorator_list = []
    namespace = vars(updater_module).copy()
    exec(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])), "baseline", "exec"), namespace)
    score = lambda **kwargs: namespace["compute_plan_steps_candidates_utility"](updater, **kwargs)
else:
    score = updater.compute_plan_steps_candidates_utility

lookup = ScoringLookup(ScoringMemoryBudget(max_bytes=512 * 1024 * 1024)) if args.method == "reuse" else None
process = psutil.Process()
stop = threading.Event()
peak = [process.memory_info().rss]


def monitor():
    while not stop.wait(0.1):
        peak[0] = max(peak[0], process.memory_info().rss)


threading.Thread(target=monitor, daemon=True).start()
try:
    for batch, frame in enumerate(inputs, start=1):
        kwargs = dict(
            candidates=candidate_rows(frame), transport_costs=transport,
            utility_profiles={}, destination_saturation=saturation,
            activity_dur=activity_dur, transport_zones=SimpleNamespace(get=lambda: zones),
            resolved_activity_parameters=parameters, min_activity_time_constant=2.0,
            allow_missing_costs_for_current_plans=True, use_destination_shadow_prices=False,
        )
        if lookup is not None:
            kwargs["scoring_lookup"] = lookup
        started = perf_counter()
        result = score(**kwargs)
        # These order-independent checksums cover all returned columns. They
        # are a diagnostic, not a substitute for the exact small-fixture tests.
        checksum = result.select(
            pl.len().alias("rows"),
            pl.struct(pl.all()).hash(seed=0).sum().alias("hash_0"),
            pl.struct(pl.all()).hash(seed=1).sum().alias("hash_1"),
        ).collect(engine="streaming")
        print("BATCH", batch, "SECONDS", round(perf_counter() - started, 3),
              "RESULT", checksum.to_dicts()[0],
              "LOOKUP_BYTES", lookup.budget.used_bytes if lookup else 0, flush=True)
finally:
    stop.set()
    print("PEAK_PROCESS_RSS_GIB", round(peak[0] / 1024**3, 3), flush=True)
