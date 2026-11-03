"""Check adaptive generation coverage and effort accounting."""

from types import SimpleNamespace

import polars as pl
import pytest

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.plans.activity_sequences import ActivitySequences
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.generation_requests import (
    AdaptiveGenerationScheduler,
    GenerationTargets,
    build_generation_targets,
)
from mobility.trips.group_day_trips.plans.mode_sequence_search.mode_sequences import ModeSequences


def _generation_parameters(scope, *, activity_draws=None):
    return SimpleNamespace(
        behavior_change=SimpleNamespace(scope_at=lambda iteration: scope),
        activity_sequences=SimpleNamespace(k_activity_sequences=activity_draws),
        destination_sequences=SimpleNamespace(k_destination_sequences=3),
        mode_sequences=SimpleNamespace(k_mode_sequences=4),
    )


def _demand_groups():
    return pl.DataFrame(
        {
            "demand_group_id": [1, 2, 3, 4],
            "demand_subgroup_id": [0, 0, 0, 0],
            "home_zone_id": [10, 10, 10, 20],
            "country": ["FR", "FR", "FR", "FR"],
            "allowed_activities": [["home"], ["school"], ["home"], ["school"]],
            "n_persons": [90.0, 10.0, 5.0, 4.0],
        }
    )


class _TransportCosts:
    def get_utility_profiles(self, demand_groups, population_segments):
        return (
            pl.DataFrame(
                {
                    "demand_group_id": [1, 2, 3, 4],
                    "demand_subgroup_id": [0, 0, 0, 0],
                    "utility_profile_id": [1, 2, 2, 1],
                }
            ),
            None,
        )


def _schedule_pool():
    return {
        "zone_profiles": pl.DataFrame(
            {
                "home_zone_id": [10, 10, 20],
                "country": ["FR", "FR", "FR"],
                "activity_profile_id": ["home", "school", "school"],
            }
        ),
        "profile_schedules": pl.DataFrame(
            {
                "country": ["FR", "FR", "FR", "FR", "FR"],
                "activity_profile_id": ["home", "home", "school", "school", "school"],
            }
        ),
    }


def _current_plans():
    return pl.DataFrame(
        {
            "demand_group_id": [1, 2, 4],
            "demand_subgroup_id": [0, 0, 0],
            "activity_seq_id": [11, 12, 13],
            "time_seq_id": [1, 1, 1],
            "dest_seq_id": [21, 22, 23],
            "mode_seq_id": [1, 1, 1],
        }
    )


def test_full_replanning_targets_every_zone_activity_and_estimates_draw_effort():
    targets = build_generation_targets(
        demand_groups=_demand_groups(),
        current_plans=_current_plans(),
        activity_schedule_pool=_schedule_pool(),
        transport_costs=_TransportCosts(),
        population_segments=[],
        parameters=_generation_parameters(
            BehaviorChangeScope.FULL_REPLANNING,
            activity_draws=None,
        ),
        iteration=1,
    )

    requests = {
        (row["home_zone_id"], row["utility_profile_id"]): row["generation_effort"]
        for row in targets.requests.iter_rows(named=True)
    }
    activity_cells = set(targets.activity_cells.iter_rows())
    assert requests == {(10, 1): 60, (10, 2): 60, (20, 1): 36}
    assert activity_cells == {(10, "home"), (10, "school"), (20, "school")}


@pytest.mark.parametrize(
    ("scope", "expected_effort"),
    [
        (BehaviorChangeScope.MODE_REPLANNING, 4),
        (BehaviorChangeScope.DESTINATION_REPLANNING, 16),
    ],
)
def test_restricted_replanning_targets_only_active_activity_profiles(scope, expected_effort):
    targets = build_generation_targets(
        demand_groups=_demand_groups(),
        current_plans=_current_plans(),
        activity_schedule_pool=_schedule_pool(),
        transport_costs=_TransportCosts(),
        population_segments=[],
        parameters=_generation_parameters(scope, activity_draws=3),
        iteration=2,
    )

    requests = {
        (row["home_zone_id"], row["utility_profile_id"]): row["generation_effort"]
        for row in targets.requests.iter_rows(named=True)
    }
    assert requests == {(10, 1): expected_effort, (10, 2): expected_effort, (20, 1): expected_effort}
    assert set(targets.activity_cells.iter_rows()) == {
        (10, "home"),
        (10, "school"),
        (20, "school"),
    }


def test_scheduler_covers_home_zones_then_targets_missing_profile_cells():
    targets = GenerationTargets(
        requests=pl.DataFrame(
            {
                "home_zone_id": [10, 10, 20],
                "utility_profile_id": [1, 2, 1],
                "population_weight": [90.0, 15.0, 50.0],
                "generation_effort": [40, 40, 40],
            }
        ),
        activity_cells=pl.DataFrame(
            {
                "home_zone_id": [10, 10, 20],
                "activity_profile_id": ["home", "school", "home"],
            }
        ),
    )
    scheduler = AdaptiveGenerationScheduler(
        targets_by_scenario={"base": targets},
        budget=200,
        max_inner_iterations=2,
    )
    no_coverage = {"base": set()}
    no_profile_coverage = {"base": set()}
    no_effect = {"base": {}}
    no_scenario_gap = {"base": 0.0}

    first_batch = scheduler.select_requests(
        batch_index=0,
        covered_cells_by_scenario=no_coverage,
        covered_profiles_by_scenario=no_profile_coverage,
        recent_effect_by_scenario=no_effect,
        scenario_gap_by_scenario=no_scenario_gap,
    )["base"]
    assert set(first_batch.select("home_zone_id", "utility_profile_id").iter_rows()) == {
        (10, 1),
        (20, 1),
    }
    assert scheduler.effort_used == 80
    assert scheduler.utility_profile_cells_by_scenario["base"] == 5

    second_batch = scheduler.select_requests(
        batch_index=1,
        covered_cells_by_scenario={"base": {(10, "home"), (20, "home")}},
        covered_profiles_by_scenario={
            "base": {(10, "home", 1), (20, "home", 1)}
        },
        recent_effect_by_scenario=no_effect,
        scenario_gap_by_scenario=no_scenario_gap,
    )["base"]
    assert set(second_batch.select("home_zone_id", "utility_profile_id").iter_rows()) == {
        (10, 2),
        (20, 1),
    }
    assert "activity_coverage" in second_batch["request_reason"].to_list()
    assert scheduler.effort_used == 160


def test_scheduler_rejects_a_budget_that_cannot_cover_each_home_zone():
    targets = GenerationTargets(
        requests=pl.DataFrame(
            {
                "home_zone_id": [10, 20],
                "utility_profile_id": [1, 1],
                "population_weight": [10.0, 20.0],
                "generation_effort": [40, 40],
            }
        ),
        activity_cells=pl.DataFrame(
            {"home_zone_id": [10, 20], "activity_profile_id": ["home", "home"]}
        ),
    )

    with pytest.raises(ValueError, match="too small to make one coverage request"):
        AdaptiveGenerationScheduler(
            targets_by_scenario={"base": targets},
            budget=79,
            max_inner_iterations=5,
        )


def test_sequence_assets_hash_request_keys_without_hashing_polars_frames():
    first_requests = pl.DataFrame(
        {
            "home_zone_id": [10, 20],
            "utility_profile_id": [1, 2],
            "generation_effort": [40, 20],
        }
    )
    same_requests_in_reverse = first_requests.reverse()
    different_profile_requests = first_requests.with_columns(
        pl.when(pl.col("home_zone_id") == 10)
        .then(pl.lit(3))
        .otherwise(pl.col("utility_profile_id"))
        .alias("utility_profile_id")
    )

    activity_first = ActivitySequences(
        is_weekday=True,
        iteration=1,
        generation_requests=first_requests,
    )
    activity_same = ActivitySequences(
        is_weekday=True,
        iteration=1,
        generation_requests=same_requests_in_reverse,
    )
    destination_first = DestinationSequences(
        is_weekday=True,
        iteration=1,
        generation_requests=first_requests,
    )
    destination_same = DestinationSequences(
        is_weekday=True,
        iteration=1,
        generation_requests=same_requests_in_reverse,
    )
    destination_different = DestinationSequences(
        is_weekday=True,
        iteration=1,
        generation_requests=different_profile_requests,
    )
    mode_first = ModeSequences(
        is_weekday=True,
        iteration=1,
        destination_sequences=InMemoryAsset({}),
        generation_requests=first_requests,
        transport_costs=None,
        parameters=None,
    )

    assert activity_first.inputs_hash == activity_same.inputs_hash
    assert destination_first.inputs_hash == destination_same.inputs_hash
    assert destination_first.inputs_hash != destination_different.inputs_hash
    assert mode_first.generation_requests is first_requests
