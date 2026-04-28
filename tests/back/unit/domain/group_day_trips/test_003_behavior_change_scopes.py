import math
import pathlib
import shutil

import polars as pl

from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, Parameters
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.plan_ids import add_plan_id
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


def _make_possible_plan_steps(rows: dict[str, list]) -> pl.DataFrame:
    rows = {
        "country": rows.get("country", ["fr"] * len(rows["demand_group_id"])),
        "time_seq_id": rows.get("time_seq_id", [0] * len(rows["demand_group_id"])),
        "first_seen_iteration": rows.get("first_seen_iteration", [None] * len(rows["demand_group_id"])),
        "last_active_iteration": rows.get("last_active_iteration", [None] * len(rows["demand_group_id"])),
        **rows,
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "country": pl.Utf8,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "activity": pl.Utf8,
            "from": pl.Int32,
            "to": pl.Int32,
            "mode": pl.Utf8,
            "duration_per_pers": pl.Float64,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
            "iteration": pl.UInt32,
            "csp": pl.Utf8,
            "first_seen_iteration": pl.UInt32,
            "last_active_iteration": pl.UInt32,
            "cost": pl.Float64,
            "distance": pl.Float64,
            "time": pl.Float64,
            "mean_duration_per_pers": pl.Float64,
            "value_of_time": pl.Float64,
            "k_saturation_utility": pl.Float64,
            "min_activity_time": pl.Float64,
            "utility": pl.Float64,
        },
    )


def _make_current_plans(rows: dict[str, list]) -> pl.DataFrame:
    rows = {
        "time_seq_id": rows.get("time_seq_id", [0] * len(rows["demand_group_id"])),
        **rows,
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
            "n_persons": pl.Float64,
        },
    )


def _make_possible_plan_utility(rows: dict[str, list]) -> pl.LazyFrame:
    rows = {
        "time_seq_id": rows.get("time_seq_id", [0] * len(rows["demand_group_id"])),
        **rows,
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
        },
    ).lazy()


def _make_local_tmp_path(name: str) -> pathlib.Path:
    path = pathlib.Path(".pytest-local-tmp") / "group_day_trips" / name
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path

def _with_plan_id(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    name: str,
) -> pl.DataFrame | pl.LazyFrame:
    return add_plan_id(frame, index_folder=_make_local_tmp_path(name))
def test_parameters_returns_default_behavior_change_scope():
    parameters = Parameters()

    assert parameters.get_behavior_change_scope(1) == BehaviorChangeScope.FULL_REPLANNING


def test_parameters_resolves_active_behavior_change_scope():
    parameters = Parameters(
        behavior_change_phases=[
            BehaviorChangePhase(start_iteration=3, scope=BehaviorChangeScope.MODE_REPLANNING),
            BehaviorChangePhase(start_iteration=5, scope=BehaviorChangeScope.DESTINATION_REPLANNING),
        ]
    )

    assert parameters.get_behavior_change_scope(1) == BehaviorChangeScope.FULL_REPLANNING
    assert parameters.get_behavior_change_scope(3) == BehaviorChangeScope.MODE_REPLANNING
    assert parameters.get_behavior_change_scope(6) == BehaviorChangeScope.DESTINATION_REPLANNING


def test_sample_active_destination_sequences_keeps_only_active_activity_sequences():
    class _StubAsset:
        def __init__(self, df):
            self._df = df

        def get_cached_asset(self):
            return self._df

    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=3,
        base_folder=_make_local_tmp_path("active_destination_sequences"),
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [1],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        activity_sequences=_StubAsset(
            pl.DataFrame(
                {
                    "demand_group_id": [1],
                    "activity_seq_id": [10],
                    "time_seq_id": [0],
                    "seq_step_index": [0],
                    "activity": ["work"],
                },
                schema={
                    "demand_group_id": pl.UInt32,
                    "activity_seq_id": pl.UInt32,
                    "time_seq_id": pl.UInt32,
                    "seq_step_index": pl.UInt32,
                    "activity": pl.Utf8,
                },
            )
        ),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=Parameters(),
        seed=123,
    )

    seen = {}

    def fake_run(activities, transport_zones, destination_saturation, chains, demand_groups, costs, parameters, seed):
        seen["chains"] = chains
        return pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "dest_seq_id": [100],
                "seq_step_index": [0],
                "from": [1],
                "to": [2],
                "departure_time": [8.0],
                "arrival_time": [9.0],
                "next_departure_time": [17.0],
                "iteration": [3],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "departure_time": pl.Float64,
                "arrival_time": pl.Float64,
                "next_departure_time": pl.Float64,
                "iteration": pl.UInt32,
            },
        )

    destination_sequences.run = fake_run
    destination_sequences._sample_active_destination_sequences()

    assert seen["chains"].select("activity_seq_id").to_series().to_list() == [10]


def test_reuse_current_destination_sequences_reuses_current_plan_steps():
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=4,
        base_folder=_make_local_tmp_path("reuse_current_destination_sequences"),
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [1],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "activity_seq_id": [10, 10],
                "time_seq_id": [1, 1],
                "dest_seq_id": [100, 100],
                "mode_seq_id": [1000, 1000],
                "seq_step_index": [0, 1],
                "from": [21, 22],
                "to": [22, 23],
                "departure_time": [8.0, 9.0],
                "arrival_time": [8.5, 9.5],
                "next_departure_time": [9.0, 17.0],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "departure_time": pl.Float64,
                "arrival_time": pl.Float64,
                "next_departure_time": pl.Float64,
            },
        ),
    )

    result = destination_sequences._reuse_current_destination_sequences()

    assert result["iteration"].unique().to_list() == [4]
    assert result["seq_step_index"].sort().to_list() == [0, 1]


def test_get_transition_probabilities_blocks_stay_home_in_mode_replanning():
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [0, 10],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
            "utility": [0.0, 1.0],
            "n_persons": [5.0, 5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 11],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1001, 1002, 1003],
            "utility": [10.0, 2.0, 3.0, 4.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 11],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1001, 1002, 1003],
            "seq_step_index": [0, 0, 0, 0],
            "activity": ["home", "work", "work", "other"],
            "from": [1, 1, 1, 1],
            "to": [1, 2, 2, 3],
            "mode": ["stay_home", "car", "walk", "bike"],
            "duration_per_pers": [24.0, 8.0, 8.0, 6.0],
            "departure_time": [0.0, 8.0, 8.0, 9.0],
            "arrival_time": [0.0, 9.0, 9.0, 10.0],
            "next_departure_time": [24.0, 17.0, 17.0, 15.0],
            "iteration": [0, 1, 1, 1],
            "csp": ["x", "x", "x", "x"],
            "cost": [0.0, 1.0, 1.0, 1.0],
            "distance": [0.0, 10.0, 10.0, 12.0],
            "time": [0.0, 1.0, 1.0, 1.0],
            "mean_duration_per_pers": [24.0, 8.0, 8.0, 6.0],
            "value_of_time": [0.0, 1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0, 1.0],
            "min_activity_time": [0.0, 1.0, 1.0, 1.0],
            "utility": [10.0, 2.0, 3.0, 4.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_mode_replanning_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_mode_replanning_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
        transport_zones=None,
    )

    assert result.filter(pl.col("activity_seq_id") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id") != 100).height == 0
    assert result.filter(pl.col("activity_seq_id_trans") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id_trans") != 100).height == 0
    assert result.filter(pl.col("activity_seq_id_trans") == 0).height == 0


def test_add_plan_id_keeps_stay_home_and_non_stay_home_states_distinct():
    index_folder = _make_local_tmp_path("add_plan_id")
    plans = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [0, 10],
            "time_seq_id": [0, 0],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = add_plan_id(plans, index_folder=index_folder)

    assert result["plan_id"].n_unique() == 2
    assert (
        result
        .filter(pl.col("activity_seq_id") == 0)
        .select("plan_id")
        .item()
        != result
        .filter(pl.col("activity_seq_id") == 10)
        .select("plan_id")
        .item()
    )

    repeated = add_plan_id(plans, index_folder=index_folder)
    assert result["plan_id"].to_list() == repeated["plan_id"].to_list()


def test_candidate_memory_ignores_persisted_stay_home_rows():
    previous_candidate_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [0, 10],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
            "seq_step_index": [0, 0],
            "activity": ["home", "work"],
            "from": [1, 1],
            "to": [1, 2],
            "mode": ["stay_home", "car"],
            "duration_per_pers": [24.0, 8.0],
            "departure_time": [0.0, 8.0],
            "arrival_time": [0.0, 9.0],
            "next_departure_time": [24.0, 17.0],
            "iteration": [0, 1],
            "csp": ["x", "x"],
            "cost": [0.0, 1.0],
            "distance": [0.0, 10.0],
            "time": [0.0, 1.0],
            "mean_duration_per_pers": [24.0, 8.0],
            "value_of_time": [0.0, 1.0],
            "k_saturation_utility": [1.0, 1.0],
            "min_activity_time": [0.0, 1.0],
            "utility": [0.0, 1.0],
        }
    )

    class _StubAsset:
        def __init__(self, df):
            self._df = df

        def get_cached_asset(self):
            return self._df

    destination_sequences = _StubAsset(
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [0],
                "dest_seq_id": [100],
                "seq_step_index": [0],
                "from": [1],
                "to": [2],
                "departure_time": [8.0],
                "arrival_time": [9.0],
                "next_departure_time": [17.0],
                "iteration": [1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "departure_time": pl.Float64,
                "arrival_time": pl.Float64,
                "next_departure_time": pl.Float64,
                "iteration": pl.UInt32,
            },
        )
    )
    mode_sequences = _StubAsset(
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [0],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
                "seq_step_index": [0],
                "mode": ["car"],
                "iteration": [1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "mode": pl.Utf8,
                "iteration": pl.UInt32,
            },
        )
    )
    chains = pl.DataFrame(
        {
            "activity_seq_id": [10],
            "time_seq_id": [0],
            "seq_step_index": [0],
            "activity": ["work"],
            "duration_per_pers": [8.0],
            "departure_time": [8.0],
            "arrival_time": [9.0],
            "next_departure_time": [17.0],
        },
        schema={
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "activity": pl.Utf8,
            "duration_per_pers": pl.Float64,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        },
    )
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1],
            "country": ["fr"],
            "csp": ["x"],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "country": pl.Utf8,
            "csp": pl.Utf8,
        },
    )

    result = CandidatePlanStepsAsset.build_candidate_memory(
        destination_sequences=destination_sequences,
        mode_sequences=mode_sequences,
        survey_plan_steps=chains,
        demand_groups=demand_groups,
        current_plans=pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            }
        ),
        previous_candidate_plan_steps=previous_candidate_plan_steps,
        current_iteration=1,
        n_warmup_iterations=1,
        max_inactive_age=2,
    ).collect()

    assert result.filter(pl.col("mode_seq_id") == 0).height == 0


def test_candidate_memory_prunes_old_inactive_plans_after_warmup():
    previous_candidate_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 11],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "seq_step_index": [0, 0],
            "activity": ["work", "shop"],
            "from": [1, 1],
            "to": [2, 3],
            "mode": ["car", "bike"],
            "duration_per_pers": [8.0, 1.0],
            "departure_time": [8.0, 18.0],
            "arrival_time": [9.0, 18.5],
            "next_departure_time": [17.0, 19.0],
            "iteration": [1, 1],
            "csp": ["x", "x"],
            "first_seen_iteration": [1, 1],
            "last_active_iteration": [3, None],
            "cost": [1.0, 1.0],
            "distance": [10.0, 3.0],
            "time": [1.0, 0.5],
            "mean_duration_per_pers": [8.0, 1.0],
            "value_of_time": [1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0],
            "min_activity_time": [1.0, 1.0],
            "utility": [1.0, 1.0],
        }
    )

    class _StubAsset:
        def __init__(self, df):
            self._df = df

        def get_cached_asset(self):
            return self._df

    destination_sequences = _StubAsset(
        pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "departure_time": pl.Float64,
                "arrival_time": pl.Float64,
                "next_departure_time": pl.Float64,
                "iteration": pl.UInt32,
            }
        )
    )
    mode_sequences = _StubAsset(
        pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "mode": pl.Utf8,
                "iteration": pl.UInt32,
            }
        )
    )
    chains = pl.DataFrame(
        schema={
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "activity": pl.Utf8,
            "duration_per_pers": pl.Float64,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        }
    )
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1],
            "country": ["fr"],
            "csp": ["x"],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "country": pl.Utf8,
            "csp": pl.Utf8,
        },
    )

    result = CandidatePlanStepsAsset.build_candidate_memory(
        destination_sequences=destination_sequences,
        mode_sequences=mode_sequences,
        survey_plan_steps=chains,
        demand_groups=demand_groups,
        current_plans=pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            }
        ),
        previous_candidate_plan_steps=previous_candidate_plan_steps,
        current_iteration=5,
        n_warmup_iterations=1,
        max_inactive_age=2,
    ).collect()

    assert result.select("activity_seq_id").sort("activity_seq_id").to_series().to_list() == [10]

def test_candidate_memory_is_already_compact_before_persistence():
    class _StubAsset:
        def __init__(self, df):
            self._df = df

        def get_cached_asset(self):
            return self._df

    destination_sequences = _StubAsset(
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [0],
                "dest_seq_id": [100],
                "seq_step_index": [1],
                "from": [11],
                "to": [22],
                "departure_time": [8.0],
                "arrival_time": [8.5],
                "next_departure_time": [17.0],
                "iteration": [1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt8,
                "from": pl.UInt16,
                "to": pl.UInt16,
                "departure_time": pl.Float32,
                "arrival_time": pl.Float32,
                "next_departure_time": pl.Float32,
                "iteration": pl.UInt16,
            },
        )
    )
    mode_sequences = _StubAsset(
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [0],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
                "seq_step_index": [1],
                "mode": ["car"],
                "iteration": [1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt8,
                "mode": pl.Enum(["car", "walk", "stay_home"]),
                "iteration": pl.UInt16,
            },
        )
    )
    survey_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [10],
            "time_seq_id": [0],
            "seq_step_index": [1],
            "activity": ["work"],
            "duration_per_pers": [8.0],
            "departure_time": [8.0],
            "arrival_time": [8.5],
            "next_departure_time": [17.0],
        },
        schema={
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "activity": pl.Utf8,
            "duration_per_pers": pl.Float32,
            "departure_time": pl.Float32,
            "arrival_time": pl.Float32,
            "next_departure_time": pl.Float32,
        },
    )
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1],
            "country": ["fr"],
            "csp": ["x"],
        }
    )

    result = CandidatePlanStepsAsset.build_candidate_memory(
        destination_sequences=destination_sequences,
        mode_sequences=mode_sequences,
        survey_plan_steps=survey_plan_steps,
        demand_groups=demand_groups,
        current_plans=pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            }
        ),
        previous_candidate_plan_steps=None,
        current_iteration=1,
        n_warmup_iterations=1,
        max_inactive_age=2,
    ).collect()

    assert result.schema["seq_step_index"] == pl.UInt8
    assert result.schema["from"] == pl.UInt16
    assert result.schema["to"] == pl.UInt16
    assert result.schema["duration_per_pers"] == pl.Float32
    assert result.schema["departure_time"] == pl.Float32
    assert result.schema["arrival_time"] == pl.Float32
    assert result.schema["next_departure_time"] == pl.Float32
    assert result.schema["iteration"] == pl.UInt16
    assert result.schema["first_seen_iteration"] == pl.UInt16
    assert result.schema["last_active_iteration"] == pl.UInt16
    assert isinstance(result.schema["mode"], pl.Enum)
def test_get_transition_probabilities_limits_destination_replanning_to_same_timing_profile():
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [1.0, 2.0, 3.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "other"],
            "from": [1, 1, 1],
            "to": [2, 3, 2],
            "mode": ["car", "car", "walk"],
            "duration_per_pers": [8.0, 8.0, 6.0],
            "departure_time": [8.0, 8.0, 9.0],
            "arrival_time": [9.0, 9.5, 10.0],
            "next_departure_time": [17.0, 17.0, 15.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 8.0],
            "time": [1.0, 1.5, 1.0],
            "mean_duration_per_pers": [8.0, 8.0, 6.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [1.0, 2.0, 3.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_destination_replanning_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_destination_replanning_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
        transport_zones=None,
    )

    assert result.select("time_seq_id_trans").unique().to_series().to_list() == [0]


def test_get_transition_probabilities_filters_candidates_by_distance_threshold():
    updater = PlanUpdater()
    updater.attach_transition_distances = lambda allowed_transitions, **kwargs: allowed_transitions.with_columns(
        distance=pl.when(pl.col("dest_seq_id_trans") == pl.col("dest_seq_id"))
        .then(0.0)
        .when(pl.col("dest_seq_id_trans") == 101)
        .then(0.05)
        .otherwise(1.0)
    )
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {"demand_group_id": [1, 1, 1], "activity_seq_id": [10, 10, 10], "dest_seq_id": [100, 101, 102], "mode_seq_id": [1000, 1001, 1002], "utility": [1.0, 2.0, 3.0]},
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "shop"],
            "from": [1, 1, 1],
            "to": [2, 3, 4],
            "mode": ["car", "bike", "walk"],
            "duration_per_pers": [8.0, 8.0, 1.0],
            "departure_time": [8.0, 8.0, 18.0],
            "arrival_time": [9.0, 9.0, 18.5],
            "next_departure_time": [17.0, 17.0, 19.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 14.0],
            "time": [1.0, 1.0, 0.5],
            "mean_duration_per_pers": [8.0, 8.0, 1.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [1.0, 2.0, 3.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_distance_threshold_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_distance_threshold_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        enable_transition_distance_model=True,
        transition_distance_threshold=0.1,
    )

    assert result["mode_seq_id_trans"].sort().to_list() == [1000, 1001]
    assert abs(float(result["p_transition"].sum()) - 1.0) < 1e-9


def test_get_transition_probabilities_uses_revision_probability_for_redistribution():
    updater = PlanUpdater()
    updater.attach_transition_distances = lambda allowed_transitions, **kwargs: allowed_transitions.with_columns(
        distance=pl.when(pl.col("dest_seq_id_trans") == pl.col("dest_seq_id"))
        .then(0.0)
        .otherwise(1.0)
    )
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "utility": [1.0, 2.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "seq_step_index": [0, 0],
            "activity": ["work", "work"],
            "from": [1, 1],
            "to": [2, 3],
            "mode": ["car", "bike"],
            "duration_per_pers": [8.0, 8.0],
            "departure_time": [8.0, 8.0],
            "arrival_time": [9.0, 9.0],
            "next_departure_time": [17.0, 17.0],
            "iteration": [1, 1],
            "csp": ["x", "x"],
            "cost": [1.0, 1.0],
            "distance": [10.0, 12.0],
            "time": [1.0, 1.0],
            "mean_duration_per_pers": [8.0, 8.0],
            "value_of_time": [1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0],
            "min_activity_time": [1.0, 1.0],
            "utility": [1.0, 2.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_revision_probability_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_revision_probability_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        enable_transition_distance_model=True,
        transition_distance_threshold=10.0,
        transition_revision_probability=0.25,
    )

    q_self = result.filter(pl.col("mode_seq_id_trans") == 1000)["q_transition"][0]
    q_switch = result.filter(pl.col("mode_seq_id_trans") == 1001)["q_transition"][0]
    p_self = result.filter(pl.col("mode_seq_id_trans") == 1000)["p_transition"][0]
    p_switch = result.filter(pl.col("mode_seq_id_trans") == 1001)["p_transition"][0]

    assert abs(float(q_self + q_switch) - 1.0) < 1e-9
    assert abs(float(p_switch) - float(0.25 * q_switch)) < 1e-9
    assert abs(float(p_self) - float(0.75 + 0.25 * q_self)) < 1e-9
    assert abs(float(p_self + p_switch) - 1.0) < 1e-9


def test_get_transition_probabilities_transition_logit_scale_softens_choice_probabilities():
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "utility": [1.0, 2.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "seq_step_index": [0, 0],
            "activity": ["work", "work"],
            "from": [1, 1],
            "to": [2, 3],
            "mode": ["car", "bike"],
            "duration_per_pers": [8.0, 8.0],
            "departure_time": [8.0, 8.0],
            "arrival_time": [9.0, 9.0],
            "next_departure_time": [17.0, 17.0],
            "iteration": [1, 1],
            "csp": ["x", "x"],
            "cost": [1.0, 1.0],
            "distance": [10.0, 12.0],
            "time": [1.0, 1.0],
            "mean_duration_per_pers": [8.0, 8.0],
            "value_of_time": [1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0],
            "min_activity_time": [1.0, 1.0],
            "utility": [1.0, 2.0],
        }
    )

    result_default = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_logit_scale_default_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_logit_scale_default_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
    )
    result_scaled = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_logit_scale_scaled_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_logit_scale_scaled_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        transition_logit_scale=0.25,
    )

    q_switch_default = result_default.filter(pl.col("mode_seq_id_trans") == 1001)["q_transition"][0]
    q_switch_scaled = result_scaled.filter(pl.col("mode_seq_id_trans") == 1001)["q_transition"][0]

    assert float(q_switch_scaled) < float(q_switch_default)
    assert abs(float(q_switch_scaled) - float(0.5621765008857981)) < 1e-9


def test_get_transition_probabilities_scales_pruning_window_with_transition_logit_scale():
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [10.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "utility": [10.0, 4.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "seq_step_index": [0, 0],
            "activity": ["work", "work"],
            "from": [1, 1],
            "to": [2, 3],
            "mode": ["car", "bike"],
            "duration_per_pers": [8.0, 8.0],
            "departure_time": [8.0, 8.0],
            "arrival_time": [9.0, 9.0],
            "next_departure_time": [17.0, 17.0],
            "iteration": [1, 1],
            "csp": ["x", "x"],
            "cost": [1.0, 1.0],
            "distance": [10.0, 12.0],
            "time": [1.0, 1.0],
            "mean_duration_per_pers": [8.0, 8.0],
            "value_of_time": [1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0],
            "min_activity_time": [1.0, 1.0],
            "utility": [10.0, 4.0],
        }
    )

    result_default = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_pruning_default_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_pruning_default_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
    )
    result_scaled = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_pruning_scaled_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_pruning_scaled_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        transition_logit_scale=0.25,
    )

    assert result_default["mode_seq_id_trans"].to_list() == [1000]
    assert result_scaled["mode_seq_id_trans"].sort().to_list() == [1000, 1001]


def test_get_transition_probabilities_transition_distance_friction_penalizes_far_states():
    updater = PlanUpdater()
    updater.attach_transition_distances = lambda allowed_transitions, **kwargs: allowed_transitions.with_columns(
        distance=pl.when(pl.col("dest_seq_id_trans") == pl.col("dest_seq_id"))
        .then(0.0)
        .when(pl.col("dest_seq_id_trans") == 101)
        .then(0.1)
        .otherwise(5.0)
    )
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [1.0, 2.0, 2.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "shop"],
            "from": [1, 1, 1],
            "to": [2, 3, 4],
            "mode": ["car", "bike", "walk"],
            "duration_per_pers": [8.0, 8.0, 1.0],
            "departure_time": [8.0, 8.0, 18.0],
            "arrival_time": [9.0, 9.0, 18.5],
            "next_departure_time": [17.0, 17.0, 19.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 14.0],
            "time": [1.0, 1.0, 0.5],
            "mean_duration_per_pers": [8.0, 8.0, 1.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [1.0, 2.0, 2.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            name="transition_probabilities_distance_friction_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            name="transition_probabilities_distance_friction_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        enable_transition_distance_model=True,
        transition_distance_friction=10.0,
        transition_distance_threshold=1e9,
    )

    q_near = result.filter(pl.col("mode_seq_id_trans") == 1001)["q_transition"][0]
    q_far = result.filter(pl.col("mode_seq_id_trans") == 1002)["q_transition"][0]
    tau_far = result.filter(pl.col("mode_seq_id_trans") == 1002)["tau_transition"][0]

    assert float(q_near) > float(q_far)
    assert float(tau_far) > 0.0
