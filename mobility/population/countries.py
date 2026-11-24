from mobility.population.france import FrenchCityLegalPopulation, FrenchPopulationGroups
from mobility.population.germany import GermanCityLegalPopulation, GermanPopulationGroups
from mobility.population.switzerland import SwissCityLegalPopulation, SwissPopulationGroups


def available_legal_population():
    """Return built-in legal population data by country."""
    return {
        "fr": FrenchCityLegalPopulation,
        "ch": SwissCityLegalPopulation,
        "de": GermanCityLegalPopulation,
    }


def available_population_groups(population, census_localized_individuals):
    """Return built-in population-group data by country."""
    return {
        "fr": FrenchPopulationGroups(census_localized_individuals),
        "ch": SwissPopulationGroups(population.inputs["switzerland_census"]),
        "de": GermanPopulationGroups(),
    }
