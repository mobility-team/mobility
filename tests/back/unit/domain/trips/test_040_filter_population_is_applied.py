import pandas as pd
from pathlib import Path

from mobility.trips import Trips
from mobility.transport_modes.default_gwp import DefaultGWP


def test_filter_population_is_applied(
    fake_population_asset,
    patch_mobility_survey,
    parquet_stubs,
    deterministic_shortuuid,
    fake_inputs_hash,
):
    """
    Ensure the optional filter_population callable is used inside create_and_get_asset.
    We feed a population of 2 individuals, filter it down to 1, and verify:
      - Trips.create_and_get_asset() runs without I/O outside tmp_path
      - The cached parquet path has the hashed prefix
      - Only the filtered individual_id remains in the output
    """
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

    # Use the dict-style helpers exposed by the parquet_stubs fixture
    parquet_stubs["install_read"](return_dataframe=population_individuals_dataframe)
    parquet_stubs["install_write"]()

    def filter_population_keep_first(input_population_dataframe: pd.DataFrame) -> pd.DataFrame:
        return input_population_dataframe[input_population_dataframe["individual_id"] == 1].copy()

    trips_instance = Trips(
        population=fake_population_asset,
        filter_population=filter_population_keep_first,
        gwp=DefaultGWP(),
    )

    trips_dataframe = trips_instance.create_and_get_asset()

    # Assert the parquet write path is the hashed cache path
    assert len(parquet_stubs["calls"]["write"]) == 1
    written_path: Path = parquet_stubs["calls"]["write"][0]
    assert written_path.name.startswith(f"{fake_inputs_hash}-")
    assert written_path.name.endswith("trips.parquet")

    # Verify filtering took effect
    assert "individual_id" in trips_dataframe.columns
    remaining_individual_ids = set(trips_dataframe["individual_id"].unique().tolist())
    assert remaining_individual_ids == {1}
