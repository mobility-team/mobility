from pathlib import Path
import pandas as pd
from mobility.trips import Trips


def test_create_and_get_asset_delegates_and_writes(
    project_dir,
    fake_population_asset,
    patch_mobility_survey,
    parquet_stubs,
    fake_inputs_hash,
    deterministic_shortuuid,
):
    population_individuals_dataframe = pd.DataFrame(
        {
            "individual_id": [1, 2],
            "transport_zone_id": [101, 102],
            "socio_pro_category": ["1", "1"],
            "ref_pers_socio_pro_category": ["1", "1"],
            "n_pers_household": ["2", "2"],
            "n_cars": ["1", "1"],
            "country": ["FR", "FR"],
        }
    )
    parquet_stubs["install_read"](population_individuals_dataframe)
    parquet_stubs["install_write"]()

    trips_instance = Trips(population=fake_population_asset)
    result_trips_dataframe = trips_instance.create_and_get_asset()

    assert parquet_stubs["calls"]["write"], "to_parquet was not called"
    written_path = parquet_stubs["calls"]["write"][0]
    expected_cache_path = project_dir / f"{fake_inputs_hash}-trips.parquet"
    assert Path(written_path) == expected_cache_path
    assert f"{fake_inputs_hash}-" in written_path.name

    expected_columns = {
        "trip_id", "mode_id", "distance", "n_other_passengers", "date",
        "previous_motive", "motive", "trip_type", "individual_id", "gwp"
    }
    assert expected_columns.issubset(set(result_trips_dataframe.columns))
    assert (result_trips_dataframe["gwp"] >= 0).all()
