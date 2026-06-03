import warnings

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
    if missing_countries:
        raise ValueError(
            "PopulationGroupDayTrips requires at least one survey for each population country. "
            f"Missing survey coverage for: {', '.join(missing_countries)}. "
            f"Provided survey countries: {', '.join(sorted(survey_countries))}."
        )

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
    """Infer population countries from the study-area local admin ids."""
    transport_zones = population.transport_zones
    if hasattr(transport_zones, "get_study_area_countries"):
        return transport_zones.get_study_area_countries()

    study_area = transport_zones.study_area.get()
    return sorted({
        str(local_admin_unit_id)[:2]
        for local_admin_unit_id in study_area["local_admin_unit_id"]
    })
