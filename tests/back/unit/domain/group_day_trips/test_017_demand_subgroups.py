import polars as pl
import pytest

from mobility.runtime.assets.cache_schema import validate_cached_table
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.iterations.iteration_assets import (
    DEMAND_GROUPS_SCHEMA,
    _read_run_state,
    _state_cache_paths,
    _write_run_state,
)
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.demand_subgroups import (
    demand_unit_hash,
    split_large_demand_groups,
)


def test_split_large_demand_groups_keeps_one_default_subgroup_without_limit():
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1, 2],
            "n_persons": [120.0, 20.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )

    result = split_large_demand_groups(
        demand_groups,
        max_persons_per_demand_subgroup=None,
    )

    assert result.select(["demand_group_id", "demand_subgroup_id", "n_persons"]).to_dicts() == [
        {"demand_group_id": 1, "demand_subgroup_id": 0, "n_persons": 120.0},
        {"demand_group_id": 2, "demand_subgroup_id": 0, "n_persons": 20.0},
    ]


def test_split_large_demand_groups_splits_only_large_groups_and_keeps_total_weight():
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1, 2],
            "n_persons": [120.0, 20.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )

    result = split_large_demand_groups(
        demand_groups,
        max_persons_per_demand_subgroup=50,
    )

    assert result.select(["demand_group_id", "demand_subgroup_id", "n_persons"]).to_dicts() == [
        {"demand_group_id": 1, "demand_subgroup_id": 0, "n_persons": 40.0},
        {"demand_group_id": 1, "demand_subgroup_id": 1, "n_persons": 40.0},
        {"demand_group_id": 1, "demand_subgroup_id": 2, "n_persons": 40.0},
        {"demand_group_id": 2, "demand_subgroup_id": 0, "n_persons": 20.0},
    ]
    assert result["n_persons"].sum() == demand_groups["n_persons"].sum()


def test_demand_unit_hash_keeps_old_hash_for_default_subgroup():
    rows = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 1],
            "activity_seq_id": [10, 10],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
        },
    )

    result = rows.with_columns(
        old_hash=pl.struct(["demand_group_id", "activity_seq_id"]).hash(seed=123),
        unit_hash=demand_unit_hash(["activity_seq_id"], seed=123),
    )

    assert result["unit_hash"][0] == result["old_hash"][0]
    assert result["unit_hash"][1] != result["old_hash"][1]


def test_split_large_demand_groups_owns_subgroup_creation():
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "n_persons": [20.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )

    with pytest.raises(ValueError, match="owns subgroup creation"):
        split_large_demand_groups(
            demand_groups,
            max_persons_per_demand_subgroup=None,
        )


def test_validate_cached_table_reports_schema_diff(tmp_path):
    table = pl.DataFrame(
        {
            "demand_group_id": [1],
            "n_persons": [20.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )

    with pytest.raises(RuntimeError) as error:
        validate_cached_table(
            table,
            table_name="demand_groups",
            required_schema=DEMAND_GROUPS_SCHEMA,
            cache_path=tmp_path / "demand_groups.parquet",
        )

    message = str(error.value)
    assert "Cached table `demand_groups` does not match the current schema." in message
    assert "Missing columns:" in message
    assert "- demand_subgroup_id" in message
    assert "Please clear the matching cached files and rerun the model." in message


def test_validate_cached_table_reports_wrong_dtype(tmp_path):
    table = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "n_persons": [20.0],
        },
        schema={
            "demand_group_id": pl.Int64,
            "demand_subgroup_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )

    with pytest.raises(RuntimeError) as error:
        validate_cached_table(
            table,
            table_name="demand_groups",
            required_schema=DEMAND_GROUPS_SCHEMA,
            cache_path=tmp_path / "demand_groups.parquet",
        )

    message = str(error.value)
    assert "Wrong column types:" in message
    assert "- demand_group_id: expected UInt32, found Int64" in message


def test_read_run_state_rejects_old_cached_schema(tmp_path):
    cache_path = _state_cache_paths(tmp_path, 0)
    state = _minimal_run_state()
    _write_run_state(cache_path, state, rng_state=("rng",))
    state.demand_groups.drop("demand_subgroup_id").write_parquet(cache_path["demand_groups"])

    with pytest.raises(RuntimeError, match="demand_subgroup_id"):
        _read_run_state(cache_path, start_iteration=1)


def test_candidate_plan_steps_asset_rejects_old_cached_schema(tmp_path):
    asset = CandidatePlanStepsAsset(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=tmp_path,
    )
    old_candidates = _candidate_plan_steps().drop("demand_subgroup_id")
    old_candidates.write_parquet(asset.cache_path)

    with pytest.raises(RuntimeError, match="candidate_plan_steps"):
        asset.get_cached_asset()


def _minimal_run_state() -> RunState:
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "n_persons": [20.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )
    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [0],
            "time_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
            "plan_id": [0],
            "n_persons": [20.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "plan_id": pl.UInt32,
            "n_persons": pl.Float64,
        },
    )
    current_plan_steps = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [0],
            "time_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
            "seq_step_index": [0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
        },
    )
    plan_id_index = current_plans.select(
        [
            "demand_group_id",
            "demand_subgroup_id",
            "activity_seq_id",
            "time_seq_id",
            "dest_seq_id",
            "mode_seq_id",
            "plan_id",
        ]
    )
    return RunState(
        survey_plans=pl.DataFrame(),
        survey_plan_steps=pl.DataFrame(),
        demand_groups=demand_groups,
        activity_dur=pl.DataFrame(),
        home_night_dur=pl.DataFrame(),
        stay_home_plan=current_plan_steps,
        opportunities=pl.DataFrame(),
        current_plans=current_plans,
        candidate_plan_steps=_candidate_plan_steps(),
        plan_id_index=plan_id_index,
        destination_saturation=pl.DataFrame(),
        costs=pl.DataFrame(),
        start_iteration=1,
        current_plan_steps=current_plan_steps,
    )


def _candidate_plan_steps() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [0],
            "time_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
            "seq_step_index": [0],
            "first_seen_iteration": [1],
            "last_seen_iteration": [1],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "first_seen_iteration": pl.UInt16,
            "last_seen_iteration": pl.UInt16,
        },
    ).select(CandidatePlanStepsAsset.REQUIRED_SCHEMA.keys())
