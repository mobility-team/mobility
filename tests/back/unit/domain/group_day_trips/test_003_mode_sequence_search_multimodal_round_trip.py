import polars as pl

from mobility.transport.modes.choice.compute_subtour_mode_probs_parallel_utilities import run_top_k_search


def test_run_top_k_search_supports_multimodal_return_pairing_on_full_round_trip():
    result = run_top_k_search(
        1,
        [10, 11, 10],
        1,
        {(10, 11): [0], (11, 10): [1]},
        {(10, 11, 0): 1.0, (11, 10, 1): 1.0},
        {0: True, 1: True},
        {0: 0, 1: 0},
        {0: True, 1: True},
        {0: False, 1: True},
        {0: 1},
        k=5,
    )

    assert isinstance(result, pl.DataFrame)
    assert result.sort(["mode_seq_index", "seq_step_index"]).to_dict(as_series=False) == {
        "mode_seq_index": [0, 0],
        "location": [11, 10],
        "seq_step_index": [1, 2],
        "mode_index": [0, 1],
        "dest_seq_id": [1, 1],
    }
