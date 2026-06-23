import polars as pl

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
