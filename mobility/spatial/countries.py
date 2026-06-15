from mobility.spatial.admin_units import FrenchAdminUnits, SwissAdminUnits
from mobility.spatial.france import FrenchLocalAdminUnitsCategories
from mobility.spatial.switzerland import SwissLocalAdminUnitsCategories


def available_admin_units():
    """Return built-in admin-unit data by country."""
    return {
        "fr": (FrenchAdminUnits, "commune"),
        "ch": (SwissAdminUnits, "municipality"),
    }


def available_local_admin_unit_categories():
    """Return built-in local-admin-unit category data by country."""
    return {
        "fr": FrenchLocalAdminUnitsCategories(),
        "ch": SwissLocalAdminUnitsCategories(),
    }
