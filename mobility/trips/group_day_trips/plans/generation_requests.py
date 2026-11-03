"""Plan deterministic, budgeted candidate-generation requests."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import math

import polars as pl

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_KEY_COLS


@dataclass(frozen=True)
class GenerationTargets:
    """Utility-profile requests and activity-profile coverage cells for a run."""

    requests: pl.DataFrame
    activity_cells: pl.DataFrame


def build_generation_targets(
    *,
    demand_groups: pl.DataFrame,
    current_plans: pl.DataFrame,
    activity_schedule_pool: dict[str, pl.DataFrame],
    transport_costs,
    population_segments,
    parameters,
    iteration: int,
) -> GenerationTargets:
    """Build per-zone utility requests and estimate their full search effort."""
    scope = parameters.behavior_change.scope_at(iteration)
    profile_assignments, _ = transport_costs.get_utility_profiles(
        demand_groups,
        population_segments,
    )
    profiled_units = (
        demand_groups
        .join(profile_assignments, on=DEMAND_UNIT_COLS, how="inner")
        .with_columns(
            activity_profile_id=pl.col("allowed_activities").list.join("|")
        )
    )
    zone_profiles = activity_schedule_pool["zone_profiles"]
    active_plans = current_plans.filter(pl.col("mode_seq_id") != 0)
    if scope == BehaviorChangeScope.FULL_REPLANNING:
        activity_cells = zone_profiles.select(
            "home_zone_id", "activity_profile_id"
        ).unique()
        activity_draws_by_zone_profile = None
        if parameters.activity_sequences.k_activity_sequences is None:
            schedules_per_profile = (
                activity_schedule_pool["profile_schedules"]
                .group_by("country", "activity_profile_id")
                .agg(pl.len().alias("schedule_count"))
            )
            activity_draws_by_zone = (
                zone_profiles
                .join(
                    schedules_per_profile,
                    on=["country", "activity_profile_id"],
                    how="inner",
                )
                .group_by("home_zone_id")
                .agg(pl.col("schedule_count").sum().alias("activity_draw_count"))
            )
        else:
            activity_draws_by_zone = (
                zone_profiles
                .group_by("home_zone_id")
                .agg(pl.len().alias("activity_profile_count"))
                .with_columns(
                    activity_draw_count=(
                        pl.col("activity_profile_count")
                        * parameters.activity_sequences.k_activity_sequences
                    )
                )
                .select("home_zone_id", "activity_draw_count")
            )
        destination_draw_factor = parameters.destination_sequences.k_destination_sequences
    else:
        profiled_units = profiled_units.join(
            active_plans.select(DEMAND_UNIT_COLS).unique(),
            on=DEMAND_UNIT_COLS,
            how="semi",
        )
        activity_cells = (
            profiled_units
            .filter(pl.col("allowed_activities").list.len() > 0)
            .select("home_zone_id", "activity_profile_id")
            .unique()
        )
        activity_draws_by_zone_profile = (
            active_plans
            .select(
                DEMAND_UNIT_COLS
                + ["activity_seq_id", "time_seq_id", "dest_seq_id"]
            )
            .unique()
            .join(
                profiled_units.select(
                    DEMAND_UNIT_COLS + ["home_zone_id", "utility_profile_id"]
                ).unique(),
                on=DEMAND_UNIT_COLS,
                how="inner",
            )
            .group_by("home_zone_id", "utility_profile_id")
            .agg(
                pl.struct("activity_seq_id", "time_seq_id")
                .n_unique()
                .alias("active_activity_count"),
                pl.struct("activity_seq_id", "time_seq_id", "dest_seq_id")
                .n_unique()
                .alias("active_destination_count"),
            )
        )
        activity_draws_by_zone_profile = activity_draws_by_zone_profile.with_columns(
            pl.when(scope == BehaviorChangeScope.DESTINATION_REPLANNING)
            .then(pl.col("active_activity_count"))
            .otherwise(pl.lit(0))
            .alias("activity_draw_count")
        )
        activity_cells = activity_cells.sort("home_zone_id", "activity_profile_id")
        destination_draw_factor = (
            parameters.destination_sequences.k_destination_sequences
            if scope == BehaviorChangeScope.DESTINATION_REPLANNING
            else 0
        )

    weighted_profiles = (
        profiled_units
        .filter(pl.col("allowed_activities").list.len() > 0)
        .group_by("home_zone_id", "utility_profile_id")
        .agg(population_weight=pl.col("n_persons").sum())
    )
    if activity_draws_by_zone_profile is None:
        requests = weighted_profiles.join(
            activity_draws_by_zone,
            on="home_zone_id",
            how="left",
        ).with_columns(active_destination_count=pl.lit(0, dtype=pl.UInt64))
    else:
        requests = weighted_profiles.join(
            activity_draws_by_zone_profile,
            on=["home_zone_id", "utility_profile_id"],
            how="left",
        )
    requests = requests.with_columns(
        activity_draw_count=pl.col("activity_draw_count").fill_null(0).cast(pl.UInt64),
        active_destination_count=pl.col("active_destination_count").fill_null(0).cast(pl.UInt64),
    )
    requests = requests.with_columns(
        generation_effort=(
            (
                pl.col("activity_draw_count") * destination_draw_factor
                + pl.col("active_destination_count")
            )
            * parameters.mode_sequences.k_mode_sequences
        ).cast(pl.UInt64)
    ).select(
        "home_zone_id",
        "utility_profile_id",
        "population_weight",
        "generation_effort",
    ).sort("home_zone_id", "utility_profile_id")

    return GenerationTargets(requests=requests, activity_cells=activity_cells)


class AdaptiveGenerationScheduler:
    """Choose deterministic generation requests within a per-loop effort cap.

    Effort is reserved before generation starts. A repeated or infeasible draw
    therefore consumes the same budget as a new feasible candidate.
    """

    def __init__(
        self,
        *,
        targets_by_scenario: dict[str, GenerationTargets],
        budget: int,
        max_inner_iterations: int,
    ) -> None:
        if budget < 1:
            raise ValueError("generation_effort_budget must be at least one.")
        if max_inner_iterations < 1:
            raise ValueError("max_inner_iterations must be at least one.")
        self.targets_by_scenario = targets_by_scenario
        self.budget = int(budget)
        self.max_inner_iterations = int(max_inner_iterations)
        self.effort_used = 0
        self.attempt_counts: dict[tuple[str, int, int], int] = defaultdict(int)
        self.recent_effect_by_scenario: dict[str, dict[int, float]] = {}
        self.scenario_gap_by_scenario: dict[str, float] = {}
        self.covered_cells_by_scenario: dict[str, set[tuple[int, str]]] = {
            scenario: set() for scenario in targets_by_scenario
        }
        self.covered_profiles_by_scenario: dict[str, set[tuple[int, str, int]]] = {
            scenario: set() for scenario in targets_by_scenario
        }
        self.activity_profiles_by_zone = {
            scenario: self._activity_profiles_by_zone(targets.activity_cells)
            for scenario, targets in targets_by_scenario.items()
        }
        self.utility_profile_cells_by_scenario = {
            scenario: sum(
                len(self.activity_profiles_by_zone[scenario].get(int(zone["home_zone_id"]), set()))
                * int(zone["profile_count"])
                for zone in targets.requests.group_by("home_zone_id").agg(
                    pl.col("utility_profile_id").n_unique().alias("profile_count")
                ).iter_rows(named=True)
            )
            for scenario, targets in targets_by_scenario.items()
        }
        self.population_by_zone = {
            scenario: {
                int(row["home_zone_id"]): float(row["population_weight"] or 0.0)
                for row in targets.requests.group_by("home_zone_id").agg(
                    pl.col("population_weight").sum()
                ).iter_rows(named=True)
            }
            for scenario, targets in targets_by_scenario.items()
        }
        self._preferred_requests = {
            scenario: self._preferred_per_zone(targets.requests)
            for scenario, targets in targets_by_scenario.items()
        }
        minimum_coverage_effort = sum(
            int(row["generation_effort"])
            for requests in self._preferred_requests.values()
            for row in requests.iter_rows(named=True)
        )
        if minimum_coverage_effort > self.budget:
            raise ValueError(
                "generation_effort_budget is too small to make one coverage request "
                "for each home zone across the configured scenarios. Required "
                f"{minimum_coverage_effort:,}; configured {self.budget:,}."
            )

    @staticmethod
    def _preferred_per_zone(requests: pl.DataFrame) -> pl.DataFrame:
        """Choose the most common utility profile in each home zone."""
        if requests.is_empty():
            return requests
        return (
            requests
            .sort(
                ["home_zone_id", "population_weight", "utility_profile_id"],
                descending=[False, True, False],
            )
            .unique(subset=["home_zone_id"], keep="first", maintain_order=True)
        )

    def select_requests(
        self,
        *,
        batch_index: int,
        covered_cells_by_scenario: dict[str, set[tuple[int, str]]],
        covered_profiles_by_scenario: dict[str, set[tuple[int, str, int]]],
        recent_effect_by_scenario: dict[str, dict[int, float]],
        scenario_gap_by_scenario: dict[str, float],
    ) -> dict[str, pl.DataFrame]:
        """Select a small request batch and reserve its full estimated effort."""
        self.covered_cells_by_scenario = covered_cells_by_scenario
        self.covered_profiles_by_scenario = covered_profiles_by_scenario
        self.recent_effect_by_scenario = recent_effect_by_scenario
        self.scenario_gap_by_scenario = scenario_gap_by_scenario

        remaining = self.budget - self.effort_used
        if remaining <= 0:
            empty_schema = {
                "home_zone_id": pl.Int32,
                "utility_profile_id": pl.UInt32,
                "population_weight": pl.Float64,
                "generation_effort": pl.UInt64,
                "request_reason": pl.String,
                "priority": pl.Float64,
            }
            return {
                scenario: pl.DataFrame(schema=empty_schema)
                for scenario in self.targets_by_scenario
            }
        target_batch_effort = max(1, self.budget // self.max_inner_iterations)
        selected: dict[str, list[dict[str, object]]] = {
            scenario: [] for scenario in self.targets_by_scenario
        }
        selected_keys: set[tuple[str, int, int]] = set()
        selected_zones: set[tuple[str, int]] = set()
        mandatory_effort = 0

        if batch_index == 0:
            # Start with one utility profile per zone so every represented
            # activity profile there receives a generation attempt.
            for scenario, preferred in self._preferred_requests.items():
                for row in preferred.iter_rows(named=True):
                    request = dict(row)
                    request["request_reason"] = "activity_coverage"
                    request["priority"] = 1_000_000.0
                    selected[scenario].append(request)
                    selected_keys.add(
                        (scenario, int(row["home_zone_id"]), int(row["utility_profile_id"]))
                    )
                    selected_zones.add((scenario, int(row["home_zone_id"])))
                    mandatory_effort += int(row["generation_effort"])

        if mandatory_effort > remaining:
            raise ValueError(
                "generation_effort_budget cannot cover the initial home-zone request "
                "batch. Remaining effort is "
                f"{remaining:,}; required effort is {mandatory_effort:,}."
            )

        batch_limit = max(target_batch_effort, mandatory_effort)
        batch_effort = mandatory_effort
        ranked: list[tuple[float, str, dict[str, object]]] = []
        for scenario, targets in self.targets_by_scenario.items():
            for row in targets.requests.iter_rows(named=True):
                home_zone_id = int(row["home_zone_id"])
                profile_id = int(row["utility_profile_id"])
                key = (scenario, home_zone_id, profile_id)
                if (
                    key in selected_keys
                    or (scenario, home_zone_id) in selected_zones
                    or int(row["generation_effort"]) <= 0
                ):
                    continue
                activity_profiles = self.activity_profiles_by_zone[scenario].get(
                    home_zone_id,
                    set(),
                )
                covered_activity_profiles = {
                    activity_profile
                    for zone, activity_profile
                    in self.covered_cells_by_scenario.get(scenario, set())
                    if zone == home_zone_id
                }
                activity_gaps = len(activity_profiles - covered_activity_profiles)
                covered_profile_cells = self.covered_profiles_by_scenario.get(
                    scenario,
                    set(),
                )
                profile_gaps = sum(
                    (home_zone_id, activity_profile, profile_id) not in covered_profile_cells
                    for activity_profile in activity_profiles
                )
                attempts = self.attempt_counts[key]
                population = float(row["population_weight"] or 0.0)
                zone_population = self.population_by_zone[scenario].get(home_zone_id, 0.0)
                population_share = population / zone_population if zone_population else 0.0
                recent_effect = self.recent_effect_by_scenario.get(
                    scenario, {}
                ).get(home_zone_id, 0.0)
                scenario_gap = self.scenario_gap_by_scenario.get(scenario, 0.0)
                priority = (
                    activity_gaps * 1_000_000.0
                    + profile_gaps * 100_000.0
                    + recent_effect * 100.0
                    + scenario_gap * 10.0
                    + population_share
                    + 1.0 / math.sqrt(attempts + 1)
                )
                if activity_gaps:
                    reason = "activity_coverage"
                elif profile_gaps:
                    reason = "utility_profile_gap"
                elif recent_effect > 0.0:
                    reason = "recent_allocation_effect"
                elif scenario_gap > 0.0:
                    reason = "scenario_flow_difference"
                else:
                    reason = "exploration"
                ranked.append((priority, scenario, {**row, "request_reason": reason}))

        ranked.sort(
            key=lambda item: (
                -item[0],
                item[1],
                int(item[2]["home_zone_id"]),
                int(item[2]["utility_profile_id"]),
            )
        )
        if not selected_keys and ranked:
            # Let a later batch spend its remaining budget on one useful
            # request even when that request exceeds the usual batch share.
            affordable_efforts = [
                int(request["generation_effort"])
                for _, _, request in ranked
                if int(request["generation_effort"]) <= remaining
            ]
            if affordable_efforts:
                batch_limit = max(batch_limit, min(affordable_efforts))
        for priority, scenario, request in ranked:
            effort = int(request["generation_effort"])
            home_zone_id = int(request["home_zone_id"])
            if (scenario, home_zone_id) in selected_zones:
                continue
            if batch_effort + effort > batch_limit or self.effort_used + batch_effort + effort > self.budget:
                continue
            request["priority"] = priority
            selected[scenario].append(request)
            selected_keys.add(
                (
                    scenario,
                    home_zone_id,
                    int(request["utility_profile_id"]),
                )
            )
            selected_zones.add((scenario, home_zone_id))
            batch_effort += effort

        request_frames: dict[str, pl.DataFrame] = {}
        for scenario, records in selected.items():
            if records:
                frame = pl.DataFrame(records).with_columns(
                    home_zone_id=pl.col("home_zone_id").cast(pl.Int32),
                    utility_profile_id=pl.col("utility_profile_id").cast(pl.UInt32),
                    generation_effort=pl.col("generation_effort").cast(pl.UInt64),
                    request_reason=pl.col("request_reason").cast(pl.String),
                    priority=pl.col("priority").cast(pl.Float64),
                ).sort("home_zone_id", "utility_profile_id")
            else:
                frame = pl.DataFrame(
                    schema={
                        "home_zone_id": pl.Int32,
                        "utility_profile_id": pl.UInt32,
                        "population_weight": pl.Float64,
                        "generation_effort": pl.UInt64,
                        "request_reason": pl.String,
                        "priority": pl.Float64,
                    }
                )
            request_frames[scenario] = frame
            for row in frame.iter_rows(named=True):
                key = (
                    scenario,
                    int(row["home_zone_id"]),
                    int(row["utility_profile_id"]),
                )
                self.attempt_counts[key] += 1
                self.effort_used += int(row["generation_effort"])

        return request_frames

    @staticmethod
    def _activity_profiles_by_zone(activity_cells: pl.DataFrame) -> dict[int, set[str]]:
        """Build compact required-activity sets for quick priority scoring."""
        result: dict[int, set[str]] = defaultdict(set)
        for row in activity_cells.iter_rows(named=True):
            result[int(row["home_zone_id"])].add(str(row["activity_profile_id"]))
        return dict(result)

def allocation_effect_by_zone(
    previous: pl.DataFrame | None,
    current: pl.DataFrame,
    context_groups: pl.DataFrame,
) -> dict[int, float]:
    """Measure recent MNL share movement by origin zone and activity profile."""
    if previous is None or previous.is_empty():
        return {}
    context_columns = DEMAND_UNIT_COLS + [
        "home_zone_id",
        "activity_profile_id",
        "utility_profile_id",
    ]
    plan_columns = [column for column in PLAN_KEY_COLS if column not in DEMAND_UNIT_COLS]
    target_columns = ["home_zone_id", "activity_profile_id", "utility_profile_id"]
    join_columns = target_columns + plan_columns

    def group_shares(plans: pl.DataFrame) -> pl.DataFrame:
        return (
            plans.lazy()
            .join(context_groups.select(context_columns).lazy(), on=DEMAND_UNIT_COLS)
            .group_by(join_columns)
            .agg(pl.col("n_persons").sum())
            .collect(engine="streaming")
        )

    before = group_shares(previous).rename({"n_persons": "previous"})
    after = group_shares(current).rename({"n_persons": "current"})
    changes = (
        before.join(after, on=join_columns, how="full", coalesce=True)
        .with_columns(
            previous=pl.col("previous").fill_null(0.0),
            current=pl.col("current").fill_null(0.0),
        )
        .group_by(target_columns)
        .agg(change=(pl.col("current") - pl.col("previous")).abs().sum())
    )
    population = context_groups.group_by(target_columns).agg(
        population=pl.col("n_persons").sum()
    )
    changes = changes.join(population, on=target_columns, how="left").with_columns(
        effect=(pl.col("change") / (2.0 * pl.col("population").clip(lower_bound=1.0)))
    )
    return {
        int(row["home_zone_id"]): float(row["effect"] or 0.0)
        for row in changes.group_by("home_zone_id").agg(pl.col("effect").sum()).iter_rows(named=True)
    }
