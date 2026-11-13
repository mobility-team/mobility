import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.stable_key_index import StableKeyIndex


def test_stable_key_index_reserves_zero_and_extends_previous_ids():
    """Real sequence ids start at one so zero can mean stay-home."""
    first_rows = pl.DataFrame({"mode_sequence_key": ["car-walk", "walk-walk"]})

    mode_index = StableKeyIndex(
        key_cols=["mode_sequence_key"],
        index_col="mode_seq_id",
        first_new_id=1,
    )
    indexed_first_rows, first_index = mode_index.extend(
        first_rows,
        previous_index=None,
    )

    assert indexed_first_rows.sort("mode_sequence_key")["mode_seq_id"].to_list() == [1, 2]

    second_rows = pl.DataFrame({"mode_sequence_key": ["walk-walk", "bike-bike"]})
    indexed_second_rows, second_index = mode_index.extend(
        second_rows,
        previous_index=first_index,
    )

    assert (
        indexed_second_rows
        .sort("mode_sequence_key")
        .select(["mode_sequence_key", "mode_seq_id"])
        .to_dicts()
        == [
            {"mode_sequence_key": "bike-bike", "mode_seq_id": 3},
            {"mode_sequence_key": "walk-walk", "mode_seq_id": 2},
        ]
    )
    assert second_index["mode_seq_id"].to_list() == [1, 2, 3]


def test_new_ids_follow_sorted_new_keys_even_when_existing_keys_are_mixed_in():
    indexer = StableKeyIndex(["activity", "mode"], "plan_id")
    previous = pl.DataFrame({"activity": [1, 3], "mode": [2, 1], "plan_id": [4, 7]}).with_columns(
        pl.col("plan_id").cast(pl.UInt32)
    )
    rows = pl.DataFrame({"activity": [3, 1, 2, 1, 2, 1], "mode": [1, 3, 2, 2, 1, 3]})
    _, result = indexer.extend(rows.lazy(), previous)
    assert result.to_dicts() == [
        {"activity": 1, "mode": 2, "plan_id": 4},
        {"activity": 3, "mode": 1, "plan_id": 7},
        {"activity": 1, "mode": 3, "plan_id": 8},
        {"activity": 2, "mode": 1, "plan_id": 9},
        {"activity": 2, "mode": 2, "plan_id": 10},
    ]


def test_lazy_index_collects_keys_without_evaluating_wide_values():
    calls = []

    def expensive(value):
        calls.append(value)
        return value * 10

    rows = pl.DataFrame({"key": [2, 1, 2], "value": [3, 4, 5]}).lazy().with_columns(
        pl.col("value").map_elements(expensive, return_dtype=pl.Int64).alias("value")
    )
    indexer = StableKeyIndex(key_cols=["key"], index_col="plan_id", first_new_id=0)
    indexed, index = indexer.extend(rows, previous_index=None)
    assert isinstance(indexed, pl.LazyFrame)
    assert calls == []
    assert index.to_dicts() == [{"key": 1, "plan_id": 0}, {"key": 2, "plan_id": 1}]
    actual = indexed.collect().sort("value")
    expected, eager_index = indexer.extend(
        pl.DataFrame({"key": [2, 1, 2], "value": [30, 40, 50]}), previous_index=None,
    )
    assert_frame_equal(actual, expected.sort("value"))
    assert_frame_equal(index, eager_index)
    assert len(calls) == 3


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("lazy", [False, True])
def test_additional_keys_keep_the_same_ids_as_two_extensions(existing, lazy):
    indexer = StableKeyIndex(["group", "plan"], "plan_id", first_new_id=0)
    previous = pl.DataFrame({"group": [3, 1], "plan": [2, 8], "plan_id": [7, 2]}).with_columns(
        pl.col("plan_id").cast(pl.UInt32)
    ) if existing else None
    mobile = pl.DataFrame({"group": [2, 1, 3, 1], "plan": [4, 2, 2, 2], "value": [1, 2, 3, 4]})
    home = pl.DataFrame({"group": [2, 1, 2, 1], "plan": [0, 0, 4, 0]})
    rows = mobile.lazy() if lazy else mobile
    old_rows, first_index = indexer.extend(rows, previous)
    _, expected_index = indexer.extend(home, first_index)
    new_rows, actual_index = indexer.extend(rows, previous, additional_keys=home)
    assert_frame_equal(actual_index, expected_index)
    if lazy:
        old_rows, new_rows = old_rows.collect(), new_rows.collect()
    assert_frame_equal(old_rows.sort("value"), new_rows.sort("value"))


def test_lazy_index_still_rejects_null_keys():
    indexer = StableKeyIndex(key_cols=["key"], index_col="plan_id")
    with pytest.raises(ValueError, match="cannot contain nulls"):
        indexer.extend(pl.DataFrame({"key": [1, None]}).lazy(), previous_index=None)


def test_index_checks_previous_ids_and_collisions_after_cast():
    """Existing IDs must be unique even when their original type is not UInt32."""
    indexer = StableKeyIndex(key_cols=["key"], index_col="plan_id")
    rows = pl.DataFrame({"key": [1]})
    duplicate_ids = pl.DataFrame({"key": [1, 2], "plan_id": [1, 1]}).with_columns(
        pl.col("plan_id").cast(pl.UInt32)
    )
    pytest.raises(ValueError, indexer.extend, rows, previous_index=duplicate_ids)
    fractional_ids = pl.DataFrame({"key": [1, 2], "plan_id": [1.1, 1.9]})
    pytest.raises(ValueError, indexer.extend, rows, previous_index=fractional_ids)
