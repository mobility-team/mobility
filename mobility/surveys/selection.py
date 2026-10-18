import warnings

from mobility.countries import normalize_country_codes
from mobility.population import Population
from mobility.surveys.mobility_survey import MobilitySurvey


def select_surveys_for_population(
    population: Population,
    surveys: list[MobilitySurvey],
) -> list[MobilitySurvey]:
    """Return the surveys that cover the population countries."""
    population_countries = set(_get_population_countries(population))
    survey_countries = {
        survey.inputs["parameters"].country
        for survey in surveys
    }

    missing_countries = sorted(population_countries - survey_countries)
    # if missing_countries:
    #     raise ValueError(
    #         "PopulationGroupDayTrips requires at least one survey for each population country. "
    #         f"Missing survey coverage for: {', '.join(missing_countries)}. "
    #         f"Provided survey countries: {', '.join(sorted(survey_countries))}."
    #     )

    unused_countries = sorted(survey_countries - population_countries)
    if unused_countries:
        warnings.warn(
            "Some provided surveys will not be used because their countries are absent from the population: "
            + ", ".join(unused_countries)
            + ".",
            stacklevel=2,
        )

    return [
        survey
        for survey in surveys
        if survey.inputs["parameters"].country in population_countries
    ]


def _get_population_countries(population: Population) -> list[str]:
    """Return population countries from the transport zones table."""
    transport_zones = population.transport_zones
    countries = normalize_country_codes(getattr(transport_zones, "countries", None))
    if not countries and hasattr(transport_zones, "study_area"):
        countries = normalize_country_codes(getattr(transport_zones.study_area, "countries", None))
    if not countries:
        raise ValueError("Population transport zones should expose a country list.")
    return countries
