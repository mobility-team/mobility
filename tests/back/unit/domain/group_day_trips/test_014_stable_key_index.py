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


def test_lazy_index_still_rejects_null_keys():
    indexer = StableKeyIndex(key_cols=["key"], index_col="plan_id")
    with pytest.raises(ValueError, match="cannot contain nulls"):
        indexer.extend(pl.DataFrame({"key": [1, None]}).lazy(), previous_index=None)
