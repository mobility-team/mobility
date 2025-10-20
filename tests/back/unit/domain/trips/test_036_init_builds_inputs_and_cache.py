from pathlib import Path
from mobility.trips import Trips
from mobility.transport_modes.default_gwp import DefaultGWP


def test_init_builds_inputs_and_cache(project_dir, fake_population_asset, patch_mobility_survey, fake_inputs_hash):
    trips_instance = Trips(population=fake_population_asset, gwp=DefaultGWP())

    # Inputs are stored
    assert "population" in trips_instance.inputs
    assert "mobility_survey" in trips_instance.inputs
    assert "gwp" in trips_instance.inputs

    # Cache path is normalized and hash-prefixed
    expected_cache_path = project_dir / f"{fake_inputs_hash}-trips.parquet"
    assert trips_instance.cache_path == expected_cache_path
    assert trips_instance.hash_path == expected_cache_path
