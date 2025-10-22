from pathlib import Path

import mobility.population as population_module


def test_create_and_get_asset_french_path_writes_parquet_and_uses_hash(
    fake_transport_zones,
    parquet_stubs,
    deterministic_sampling,
    deterministic_shortuuid,
    fake_inputs_hash,
):
    """
    Exercise the French code path; expect two parquet writes with the deadbeef… hash prefix.
    """
    population = population_module.Population(
        transport_zones=fake_transport_zones,
        sample_size=4,
        switzerland_census=None,
    )

    returned_cache_paths = population.create_and_get_asset()

    assert returned_cache_paths is population.cache_path

    parquet_write_paths = parquet_stubs.writes
    assert len(parquet_write_paths) == 2
    parquet_write_names = {path.name for path in parquet_write_paths}

    for path in parquet_write_paths:
        assert path.name.startswith(f"{fake_inputs_hash}-"), f"Expected hash prefix in {path}"

    assert any(name.endswith("individuals.parquet") for name in parquet_write_names)
    assert any(name.endswith("population_groups.parquet") for name in parquet_write_names)

    # Exact paths should match the instance cache paths
    assert set(map(Path, population.cache_path.values())) == set(parquet_write_paths)
