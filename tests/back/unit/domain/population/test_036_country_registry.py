from types import SimpleNamespace
from pathlib import Path

import pandas as pd

import mobility.population.population as population_module


def test_population_can_use_a_third_country_population_class(project_dir, deterministic_sampling, deterministic_shortuuid, monkeypatch):
    written_tables = {}

    def fake_to_parquet(self, path, *args, **kwargs):
        written_tables[Path(path).name] = self.copy()

    class FakeCityLegalPopulation:
        def __init__(self, countries=None):
            self.countries = countries

        def get(self):
            return pd.DataFrame(
                {
                    "local_admin_unit_id": ["de-001"],
                    "legal_population": [100],
                }
            )

    monkeypatch.setattr(population_module, "CityLegalPopulation", FakeCityLegalPopulation)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=True)
    class FakeGermanPopulationGroups:
        def build(self, transport_zones, legal_pop_by_city, lau_to_tz_coeff):
            return pd.DataFrame(
                {
                    "transport_zone_id": ["tz-1", "tz-1"],
                    "local_admin_unit_id": ["de-001", "de-001"],
                    "age": [30, 31],
                    "socio_pro_category": ["1", "2"],
                    "ref_pers_socio_pro_category": ["1", "2"],
                    "n_pers_household": [1, 2],
                    "n_cars": [0, 1],
                    "weight": [1.0, 1.0],
                    "country": ["de", "de"],
                }
            )

    monkeypatch.setattr(
        population_module,
        "available_population_groups",
        lambda _population, _census: {"de": FakeGermanPopulationGroups()},
    )

    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": ["tz-1"],
                "local_admin_unit_id": ["de-001"],
                "country": ["de"],
                "weight": [1.0],
                "geometry": [None],
            }
        ),
        countries=["de"],
    )

    population = population_module.Population(
        transport_zones=transport_zones,
        sample_size=2,
        switzerland_census=None,
    )

    population.create_and_get_asset()

    assert any(path.endswith("individuals.parquet") for path in written_tables)
    assert any(path.endswith("population_groups.parquet") for path in written_tables)
    individuals = next(df for path, df in written_tables.items() if path.endswith("individuals.parquet"))
    assert individuals["country"].tolist() == ["de", "de"]
