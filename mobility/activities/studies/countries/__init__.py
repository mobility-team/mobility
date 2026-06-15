from mobility.activities.studies.countries.france import FrenchStudy
from mobility.activities.studies.countries.switzerland import SwissStudy


def available_study_data():
    """Return built-in study data by country."""
    return {
        "fr": FrenchStudy(),
        "ch": SwissStudy(),
    }
