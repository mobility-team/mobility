# tests/unit/mobility/test_001_init_builds_inputs_and_cache.py
from pathlib import Path

import mobility.population as population_module


def test_init_sets_inputs_and_hashed_cache_paths(project_dir, fake_inputs_hash, fake_transport_zones):
    population = population_module.Population(
        transport_zones=fake_transport_zones,
        sample_size=10,
        switzerland_census=None,
    )

    assert population.inputs["transport_zones"] is fake_transport_zones
    assert population.inputs["sample_size"] == 10
    assert population.inputs["switzerland_census"] is None

    individuals_cache_path = population.cache_path["individuals"]
    population_groups_cache_path = population.cache_path["population_groups"]

    assert isinstance(individuals_cache_path, Path)
    assert isinstance(population_groups_cache_path, Path)

    assert individuals_cache_path.parent == project_dir
    assert population_groups_cache_path.parent == project_dir

    assert individuals_cache_path.name.startswith(f"{fake_inputs_hash}-")
    assert individuals_cache_path.name.endswith("individuals.parquet")

    assert population_groups_cache_path.name.startswith(f"{fake_inputs_hash}-")
    assert population_groups_cache_path.name.endswith("population_groups.parquet")
