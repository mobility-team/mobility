from pathlib import Path

import mobility.population as population_module


def test_get_cached_asset_returns_expected_cache_paths(fake_transport_zones):
    population = population_module.Population(
        transport_zones=fake_transport_zones,
        sample_size=5,
        switzerland_census=None,
    )
    cache_paths = population.get_cached_asset()
    assert set(cache_paths.keys()) == {"individuals", "population_groups"}
    for cache_path in cache_paths.values():
        assert isinstance(cache_path, Path)
