import pandas as pd
from pathlib import Path
from mobility.trips import Trips


def test_get_cached_asset_reads_parquet(project_dir, fake_population_asset, patch_mobility_survey, parquet_stubs, fake_inputs_hash):
    cached_trips_dataframe = pd.DataFrame(
        {"trip_id": ["t1", "t2"], "mode_id": [1, 2], "distance": [10.0, 2.0]}
    )
    parquet_stubs["install_read"](cached_trips_dataframe)

    trips_instance = Trips(population=fake_population_asset)

    result_dataframe = trips_instance.get_cached_asset()
    assert isinstance(result_dataframe, pd.DataFrame)
    assert list(result_dataframe.columns) == ["trip_id", "mode_id", "distance"]
    assert parquet_stubs["calls"]["read"], "read_parquet was not called"
    read_path = parquet_stubs["calls"]["read"][0]

    expected_cache_path = project_dir / f"{fake_inputs_hash}-trips.parquet"
    assert Path(read_path) == expected_cache_path
