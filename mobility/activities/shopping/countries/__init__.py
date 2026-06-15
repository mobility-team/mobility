from mobility.activities.shopping.countries.france import FrenchShopping
from mobility.activities.shopping.countries.switzerland import SwissShopping


def available_shopping_data():
    """Return built-in shopping data by country."""
    return {
        "fr": FrenchShopping(),
        "ch": SwissShopping(),
    }
