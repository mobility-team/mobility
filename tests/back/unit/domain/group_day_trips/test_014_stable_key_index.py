import polars as pl

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
