from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import polars as pl

from .performance_trace import trace_simulation_step

from ..plans.plan_ids import PLAN_KEY_COLS
from ..plans.stable_key_index import add_ordered_key_hashes


OD_MODE_COLUMNS = ["from", "to", "mode"]
PLAN_CONTENT_COLUMNS = [
    "seq_step_index",
    "activity",
    "duration_per_pers",
    "departure_time",
    "arrival_time",
    "next_departure_time",
    "from",
    "to",
    "mode",
]
CANDIDATE_ID_COLS = ["candidate_id_1", "candidate_id_2"]


@trace_simulation_step("hash candidate content")
def candidate_content_ids(candidate_plan_steps: pl.DataFrame, group_columns: list[str]) -> pl.DataFrame:
    """Identify ordered plan contents once for the requested grouping."""
    missing = set(group_columns + PLAN_CONTENT_COLUMNS).difference(candidate_plan_steps.columns)
    if missing:
        raise ValueError(
            "Candidate plan steps are missing columns needed for candidate identity: "
            f"{sorted(missing)}."
        )

    # Sequence ids can differ between run contexts. Hash typed, ordered step
    # content instead of concatenating text for every step and then hashing it
    # in Python. Times use the same whole-second precision as plan identities.
    time_columns = {
        "duration_per_pers": "_duration_seconds",
        "departure_time": "_departure_seconds",
        "arrival_time": "_arrival_seconds",
        "next_departure_time": "_next_departure_seconds",
    }
    normalized_steps = candidate_plan_steps.with_columns(
        [
            (pl.col(column).cast(pl.Float64) * 3600.0)
            .round(0)
            .cast(pl.Int64)
            .alias(normalized_column)
            for column, normalized_column in time_columns.items()
        ]
    )
    step_content = pl.struct(
        pl.col("activity").cast(pl.String),
        *[pl.col(column) for column in time_columns.values()],
        pl.col("from"),
        pl.col("to"),
        pl.col("mode").cast(pl.String),
    )
    return add_ordered_key_hashes(
        normalized_steps,
        group_cols=group_columns,
        order_col="seq_step_index",
        step_content=step_content,
        key_prefix="candidate_id",
    ).select(group_columns + ["candidate_id_1", "candidate_id_2"])


def add_candidate_ids(candidate_plan_steps: pl.DataFrame) -> pl.DataFrame:
    """Add a stable identity for complete plan content, independent of local ids."""
    plan_content = candidate_content_ids(candidate_plan_steps, PLAN_KEY_COLS)

    return (
        candidate_plan_steps
        .drop("candidate_id_1", "candidate_id_2", strict=False)
        .join(plan_content, on=PLAN_KEY_COLS, how="left")
    )


def represented_od_mode_flows(current_plan_steps: pl.DataFrame, *, include_activity: bool = False) -> pl.DataFrame:
    """Aggregate represented trips into an OD-by-mode flow vector."""
    columns = OD_MODE_COLUMNS + (["activity"] if include_activity else [])
    required = set(columns + ["n_persons", "mode_seq_id"])
    missing = required.difference(current_plan_steps.columns)
    if missing:
        raise ValueError(
            "Current plan steps are missing columns needed for OD-mode flows: "
            f"{sorted(missing)}."
        )

    return (
        current_plan_steps
        .filter(pl.col("mode_seq_id") != 0)
        .group_by(columns)
        .agg(volume=pl.col("n_persons").sort().implode().list.sum())
        .sort(columns)
    )


@trace_simulation_step("aggregate inner allocation flows")
def allocation_od_mode_flows(
    allocation: pl.DataFrame, possible_plan_steps: pl.DataFrame | pl.LazyFrame, *, include_activity: bool = False,
    shared_plan_ids: pl.DataFrame | None = None, shared_steps: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Reduce allocated plans to trip flows without building wide step rows."""
    columns = OD_MODE_COLUMNS + (["activity"] if include_activity else [])
    if shared_plan_ids is not None and shared_steps is not None:
        # Attach the shared physical steps only to allocated plans. Keep each
        # allocation separate until the final sorted sum, preserving its order.
        return (
            allocation.lazy().filter(pl.col("mode_seq_id") != 0).select("plan_id", "n_persons")
            .join(shared_plan_ids.lazy(), on="plan_id", how="left")
            .join(shared_steps.lazy().select(["_timing_plan"] + columns), on="_timing_plan", how="left")
            .group_by(columns)
            .agg(volume=pl.col("n_persons").sort().implode().list.sum())
            .sort(columns).collect(engine="streaming")
        )
    return (
        allocation.lazy()
        .filter(pl.col("mode_seq_id") != 0)
        .select("plan_id", "n_persons")
        .join(
            possible_plan_steps.lazy().select(["plan_id"] + columns),
            on="plan_id",
            how="left",
        )
        .group_by(columns)
        .agg(volume=pl.col("n_persons").sort().implode().list.sum())
        .sort(columns)
        .collect(engine="streaming")
    )


def flow_change_index(previous: pl.DataFrame, current: pl.DataFrame) -> float:
    """Return symmetric absolute flow change, from 0 (same) to 1 (no overlap)."""
    keys = OD_MODE_COLUMNS + ["activity"]
    changes = previous.select(keys + [pl.col("volume").alias("previous")]).join(
        current.select(keys + [pl.col("volume").alias("current")]),
        on=keys, how="full", coalesce=True,
    ).with_columns(pl.col("previous", "current").fill_null(0.0))
    totals = changes.select(
        (pl.col("current") - pl.col("previous")).abs().sort().implode().list.sum().alias("difference"),
        (pl.col("current") + pl.col("previous")).sort().implode().list.sum().alias("total"),
    ).row(0, named=True)
    return float(totals["difference"] or 0.0) / float(totals["total"]) if totals["total"] else 0.0


def flow_change_breakdown(
    previous: pl.DataFrame | None, current: pl.DataFrame, category: str,
) -> list[dict]:
    """Report trips and flow change separately for each mode or activity."""
    keys = OD_MODE_COLUMNS + ["activity"]
    current = current.with_columns(pl.col(category).cast(pl.String))
    if previous is None:
        return (
            current.group_by(category).agg(pl.col("volume").sum().alias("trips"))
            .sort(category).with_columns(pl.lit(None, dtype=pl.Float64).alias("flow_change_index"))
            .rename({category: "category"}).to_dicts()
        )
    previous = previous.with_columns(pl.col(category).cast(pl.String))
    changes = previous.select(keys + [pl.col("volume").alias("previous")]).join(
        current.select(keys + [pl.col("volume").alias("current")]), on=keys, how="full", coalesce=True,
    ).with_columns(pl.col("previous", "current").fill_null(0.0))
    return (
        changes.group_by(category).agg(
            pl.col("current").sort().implode().list.sum().alias("trips"),
            (pl.col("current") - pl.col("previous")).abs().sort().implode().list.sum().alias("difference"),
            (pl.col("current") + pl.col("previous")).sort().implode().list.sum().alias("total"),
        ).with_columns(
            pl.when(pl.col("total") > 0).then(pl.col("difference") / pl.col("total"))
            .otherwise(0.0).alias("flow_change_index")
        ).select(pl.col(category).alias("category"), "trips", "flow_change_index").sort("category").to_dicts()
    )


def normalized_flow_change(
    previous: pl.DataFrame,
    current: pl.DataFrame,
    represented_population: float,
) -> float:
    """Return the normalized root-sum-square change between two flow vectors."""
    _validate_population(represented_population)
    previous_flows = previous.select(
        OD_MODE_COLUMNS + [pl.col("volume").alias("previous_volume")]
    )
    current_flows = current.select(
        OD_MODE_COLUMNS + [pl.col("volume").alias("current_volume")]
    )
    changes = (
        previous_flows
        .join(current_flows, on=OD_MODE_COLUMNS, how="full", coalesce=True)
        .with_columns(
            previous_volume=pl.col("previous_volume").fill_null(0.0),
            current_volume=pl.col("current_volume").fill_null(0.0),
        )
        .select((pl.col("current_volume") - pl.col("previous_volume")).pow(2).alias("squared_change"))
    )
    squared_change = changes["squared_change"].sort().sum()
    return (float(squared_change or 0.0) ** 0.5) / represented_population


def normalized_base_gap_change(
    previous_scenario: pl.DataFrame,
    previous_base: pl.DataFrame,
    current_scenario: pl.DataFrame,
    current_base: pl.DataFrame,
    represented_population: float,
) -> float:
    """Return how much a scenario-to-base OD-mode gap moved between evaluations."""
    _validate_population(represented_population)
    signed_flows = pl.concat(
        [
            _signed_flows(previous_scenario, 1.0),
            _signed_flows(previous_base, -1.0),
            _signed_flows(current_scenario, -1.0),
            _signed_flows(current_base, 1.0),
        ],
        how="vertical_relaxed",
    )
    changes = (
        signed_flows
        .group_by(OD_MODE_COLUMNS)
        .agg(change=pl.col("signed_volume").sort().implode().list.sum())
        .select(pl.col("change").pow(2).alias("squared_change"))
    )
    squared_change = changes["squared_change"].sort().sum()
    return (float(squared_change or 0.0) ** 0.5) / represented_population


@dataclass(frozen=True)
class InnerLoopConvergenceResult:
    """Stability values and consecutive-pass count for one pool evaluation."""

    flow_changes: dict[int, float | None]
    base_gap_changes: dict[int, float | None]
    consecutive_passes: int
    converged: bool


@dataclass
class InnerLoopConvergence:
    """Track scenario flow stability and scenario-to-base gaps across batches."""

    flow_tolerance: float
    base_gap_tolerance: float
    required_consecutive_passes: int = 2
    _previous_flows: dict[int, pl.DataFrame] = field(default_factory=dict)
    _consecutive_passes: int = 0

    def evaluate(
        self,
        flows_by_scenario: Mapping[int, pl.DataFrame],
        population_by_scenario: Mapping[int, float],
        *,
        base_scenario: int,
    ) -> InnerLoopConvergenceResult:
        """Record one evaluation and say whether every scenario has stabilized."""
        if base_scenario not in flows_by_scenario:
            raise ValueError(f"Base scenario '{base_scenario}' is missing from the flow vectors.")
        if set(flows_by_scenario) != set(population_by_scenario):
            raise ValueError("Flow vectors and represented populations must have the same scenarios.")
        if self.required_consecutive_passes < 1:
            raise ValueError("required_consecutive_passes must be at least 1.")

        flow_changes: dict[int, float | None] = {}
        base_gap_changes: dict[int, float | None] = {}
        for scenario, current_flows in flows_by_scenario.items():
            previous_flows = self._previous_flows.get(scenario)
            if previous_flows is None:
                flow_changes[scenario] = None
                base_gap_changes[scenario] = None if scenario != base_scenario else 0.0
                continue

            flow_changes[scenario] = normalized_flow_change(
                previous_flows,
                current_flows,
                population_by_scenario[scenario],
            )
            if scenario == base_scenario:
                base_gap_changes[scenario] = 0.0
                continue

            previous_base = self._previous_flows.get(base_scenario)
            if previous_base is None:
                base_gap_changes[scenario] = None
            else:
                base_gap_changes[scenario] = normalized_base_gap_change(
                    previous_flows,
                    previous_base,
                    current_flows,
                    flows_by_scenario[base_scenario],
                    population_by_scenario[scenario],
                )

        all_flows_pass = all(
            value is not None and value <= self.flow_tolerance
            for value in flow_changes.values()
        )
        all_gaps_pass = all(
            value is not None and value <= self.base_gap_tolerance
            for scenario, value in base_gap_changes.items()
            if scenario != base_scenario
        )
        if all_flows_pass and all_gaps_pass:
            self._consecutive_passes += 1
        else:
            self._consecutive_passes = 0

        self._previous_flows = {
            scenario: flows.clone()
            for scenario, flows in flows_by_scenario.items()
        }
        return InnerLoopConvergenceResult(
            flow_changes=flow_changes,
            base_gap_changes=base_gap_changes,
            consecutive_passes=self._consecutive_passes,
            converged=self._consecutive_passes >= self.required_consecutive_passes,
        )


def _signed_flows(flows: pl.DataFrame, sign: float) -> pl.DataFrame:
    return flows.select(
        *OD_MODE_COLUMNS,
        (pl.col("volume") * sign).alias("signed_volume"),
    )


def _validate_population(represented_population: float) -> None:
    if represented_population <= 0:
        raise ValueError("represented_population must be greater than zero.")
