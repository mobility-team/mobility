from types import SimpleNamespace

from mobility.population.countries import available_legal_population, available_population_groups


def test_population_country_registries_expose_built_in_countries(monkeypatch):
    french_calls = []
    swiss_calls = []

    class FakeFrenchPopulationGroups:
        def __init__(self, census_localized_individuals):
            french_calls.append(census_localized_individuals)

    class FakeSwissPopulationGroups:
        def __init__(self, switzerland_census):
            swiss_calls.append(switzerland_census)

    monkeypatch.setattr(
        "mobility.population.countries.FrenchPopulationGroups",
        FakeFrenchPopulationGroups,
    )
    monkeypatch.setattr(
        "mobility.population.countries.SwissPopulationGroups",
        FakeSwissPopulationGroups,
    )

    population = SimpleNamespace(inputs={"switzerland_census": "swiss-census"})
    registries = available_population_groups(population, "census-localized-individuals")

    assert set(available_legal_population()) == {"fr", "ch"}
    assert set(registries) == {"fr", "ch"}
    assert french_calls == ["census-localized-individuals"]
    assert swiss_calls == ["swiss-census"]
