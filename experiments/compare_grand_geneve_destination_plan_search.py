"""Compare the legacy and destination-plan-search samplers.

This script reuses the model definition in ``run_scenarios.py`` and runs it
twice. The only changed parameter is ``use_destination_plan_search``. The
configured mode-sequence search backend is therefore identical in both runs.

Run from the Mobility repository:

    python experiments/compare_grand_geneve_destination_plan_search.py

The benchmark covers the complete weekday/default-scenario model run. Results
and timings are written under the Mobility project data folder.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import pathlib
import subprocess
import sys
import time
from typing import Any

import mobility
import polars as pl


SCRIPT_FOLDER = pathlib.Path(__file__).resolve().parent
VARIANTS = {
    "legacy": False,
    "destination_plan_search": True,
}


def parse_args() -> argparse.Namespace:
    default_grand_geneve_folder = pathlib.Path(
        os.environ.get(
            "MOBILITY_GRAND_GENEVE_PROJECT_FOLDER",
            SCRIPT_FOLDER.parent.parent / "mobility-grand-geneve",
        )
    )
    parser = argparse.ArgumentParser(
        description=(
            "Compare Grand Genève results and runtime with "
            "use_destination_plan_search disabled and enabled."
        )
    )
    parser.add_argument(
        "--grand-geneve-folder",
        type=pathlib.Path,
        default=default_grand_geneve_folder,
        help=(
            "Folder containing the Grand Genève run_scenarios.py file. "
            "Defaults to MOBILITY_GRAND_GENEVE_PROJECT_FOLDER or the sibling "
            "mobility-grand-geneve repository."
        ),
    )
    parser.add_argument(
        "--order",
        choices=["legacy-first", "search-first"],
        default="legacy-first",
        help="Execution order. Shared upstream inputs may be reused by the second run.",
    )
    parser.add_argument(
        "--output-folder",
        type=pathlib.Path,
        default=None,
        help=(
            "Defaults to <MOBILITY_PROJECT_DATA_FOLDER>/benchmarks/"
            "destination-plan-search."
        ),
    )
    parser.add_argument(
        "--rebuild-variant-chain",
        action="store_true",
        help=(
            "Rebuild destination sequences, mode sequences, plan updates, and "
            "final outputs while retaining their already-cached upstream inputs."
        ),
    )
    return parser.parse_args()


def load_grand_geneve_setup(grand_geneve_folder: pathlib.Path) -> Any:
    """Execute ``run_scenarios.py`` only through its model setup."""
    scenario_script = grand_geneve_folder / "run_scenarios.py"
    if not scenario_script.exists():
        raise FileNotFoundError(
            f"Grand Genève scenario script not found: {scenario_script}"
        )
    source = scenario_script.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(scenario_script))
    setup_nodes = []

    for node in tree.body:
        setup_nodes.append(node)
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name)
            and target.id == "pop_group_day_trips"
            for target in node.targets
        ):
            break
    else:
        raise RuntimeError(
            "Could not find `pop_group_day_trips` in run_scenarios.py."
        )

    namespace = {
        "__file__": str(scenario_script),
        "__name__": "_grand_geneve_benchmark_setup",
    }
    sys.path.insert(0, str(grand_geneve_folder))
    try:
        module = ast.Module(body=setup_nodes, type_ignores=[])
        exec(
            compile(
                ast.fix_missing_locations(module),
                str(scenario_script),
                "exec",
            ),
            namespace,
        )
    finally:
        sys.path.remove(str(grand_geneve_folder))

    return namespace["pop_group_day_trips"]


def with_destination_plan_search(setup: Any, enabled: bool) -> Any:
    """Clone a setup while changing only the destination sampler flag."""
    destination_parameters = setup.parameters.destination_sequences.model_copy(
        update={"use_destination_plan_search": enabled}
    )
    parameters = setup.parameters.model_copy(
        update={"destination_sequences": destination_parameters}
    )
    return mobility.PopulationGroupDayTrips(
        population=setup.population,
        modes=setup.modes,
        activities=setup.activities,
        surveys=setup.surveys,
        scenarios=setup.scenarios,
        parameters=parameters,
    )


def summarize_plan_steps(plan_steps: pl.LazyFrame) -> dict[str, pl.DataFrame]:
    """Build small, weighted summaries from final simulated trips."""
    weighted = plan_steps.with_columns(
        weighted_distance=pl.col("n_persons") * pl.col("distance"),
        weighted_time=pl.col("n_persons") * pl.col("time"),
        weighted_ghg=(
            pl.col("n_persons") * pl.col("ghg_emissions_per_trip")
        ),
    )
    metrics = [
        pl.len().alias("plan_step_rows"),
        pl.col("n_persons").sum().alias("weighted_trip_count"),
        pl.col("weighted_distance").sum().alias("weighted_distance"),
        pl.col("weighted_time").sum().alias("weighted_time"),
        pl.col("weighted_ghg").sum().alias("weighted_ghg"),
    ]

    return {
        "overall": weighted.select(metrics).collect(),
        "by_mode": (
            weighted.group_by("mode")
            .agg(metrics)
            .sort("mode")
            .collect()
        ),
        "by_activity": (
            weighted.group_by("activity")
            .agg(metrics)
            .sort("activity")
            .collect()
        ),
    }


def run_variant(
    name: str,
    enabled: bool,
    variant: Any,
    output_folder: pathlib.Path,
) -> dict[str, Any]:
    run = variant.run(day_type="weekday", scenario="default")
    cached_before = all(path.exists() for path in run.cache_path.values())

    print(f"\nRunning {name} (use_destination_plan_search={enabled})")
    if cached_before:
        print("  Final outputs are already cached; runtime is a cache-hit time.")

    started = time.perf_counter()
    outputs = run.get()
    seconds = time.perf_counter() - started

    summaries = summarize_plan_steps(outputs["plan_steps"])
    variant_folder = output_folder / name
    variant_folder.mkdir(parents=True, exist_ok=True)
    for summary_name, table in summaries.items():
        table.write_parquet(variant_folder / f"{summary_name}.parquet")

    result = {
        "use_destination_plan_search": enabled,
        "seconds": seconds,
        "cached_before": cached_before,
        "run_inputs_hash": run.inputs_hash,
        "overall": summaries["overall"].row(0, named=True),
    }
    print(f"  Completed in {seconds:.3f} s")
    return result


def remove_variant_chain(variant: Any) -> None:
    """Remove outputs downstream of destination sampling for a fair rerun."""
    run = variant.run(day_type="weekday", scenario="default")
    run.remove()
    for state in run.iteration_state_assets:
        state.remove()
        state.mode_sequences.remove()
        state.destination_sequences.remove()


def write_comparison(output_folder: pathlib.Path) -> None:
    """Write absolute and relative differences between variant summaries."""
    for summary_name, keys in (
        ("overall", []),
        ("by_mode", ["mode"]),
        ("by_activity", ["activity"]),
    ):
        legacy = pl.read_parquet(
            output_folder / "legacy" / f"{summary_name}.parquet"
        )
        search = pl.read_parquet(
            output_folder
            / "destination_plan_search"
            / f"{summary_name}.parquet"
        )
        metric_columns = [
            column for column in legacy.columns if column not in keys
        ]

        if keys:
            comparison = legacy.join(
                search,
                on=keys,
                how="full",
                suffix="_search",
                coalesce=True,
            )
        else:
            comparison = pl.concat(
                [
                    legacy.rename(
                        {column: f"{column}_legacy" for column in metric_columns}
                    ),
                    search.rename(
                        {column: f"{column}_search" for column in metric_columns}
                    ),
                ],
                how="horizontal",
            )

        expressions = []
        for column in metric_columns:
            legacy_column = (
                f"{column}_legacy" if not keys else column
            )
            search_column = f"{column}_search"
            expressions.extend(
                [
                    (
                        pl.col(search_column) - pl.col(legacy_column)
                    ).alias(f"{column}_difference"),
                    (
                        (pl.col(search_column) - pl.col(legacy_column))
                        / pl.col(legacy_column)
                    ).alias(f"{column}_relative_difference"),
                ]
            )
        comparison.with_columns(expressions).write_parquet(
            output_folder / f"{summary_name}_comparison.parquet"
        )


def git_revision(folder: pathlib.Path) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=folder,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def main() -> None:
    args = parse_args()
    grand_geneve_folder = args.grand_geneve_folder.resolve()
    os.chdir(grand_geneve_folder)
    setup = load_grand_geneve_setup(grand_geneve_folder)

    output_folder = args.output_folder
    if output_folder is None:
        output_folder = (
            pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
            / "benchmarks"
            / "destination-plan-search"
        )
    output_folder.mkdir(parents=True, exist_ok=True)

    order = (
        ["legacy", "destination_plan_search"]
        if args.order == "legacy-first"
        else ["destination_plan_search", "legacy"]
    )
    variants = {
        name: with_destination_plan_search(setup, enabled)
        for name, enabled in VARIANTS.items()
    }
    if args.rebuild_variant_chain:
        for variant in variants.values():
            remove_variant_chain(variant)

    results = {
        name: run_variant(
            name,
            VARIANTS[name],
            variants[name],
            output_folder,
        )
        for name in order
    }
    write_comparison(output_folder)

    legacy_seconds = results["legacy"]["seconds"]
    search_seconds = results["destination_plan_search"]["seconds"]
    timings_are_comparable = not any(
        result["cached_before"] for result in results.values()
    )
    report = {
        "execution_order": order,
        "mobility_revision": git_revision(SCRIPT_FOLDER.parent),
        "grand_geneve_revision": git_revision(grand_geneve_folder),
        "variants": results,
        "timings_are_comparable": timings_are_comparable,
        "speedup_vs_legacy": (
            legacy_seconds / search_seconds
            if timings_are_comparable and search_seconds > 0.0
            else None
        ),
    }
    (output_folder / "summary.json").write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\n" + json.dumps(report, indent=2))
    print(f"\nDetailed comparisons: {output_folder}")


if __name__ == "__main__":
    main()
