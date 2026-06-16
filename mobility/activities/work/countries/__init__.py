from mobility.activities.work.countries.france import FrenchWork
from mobility.activities.work.countries.switzerland import SwissWork


def available_work_data():
    """Return built-in work data by country."""
    return {
        "fr": FrenchWork(),
        "ch": SwissWork(),
    }
