import pandas as pd
import pytest

import mobility.population as population_module


def test_get_swiss_pop_groups_raises_without_census():
    """
    If zones include Switzerland ("ch-") and switzerland_census is missing,
    the method should raise ValueError.
    """
    class TransportZonesWithSwiss:
        def __init__(self):
            self.inputs = {}
        def get(self):
            import geopandas as geopandas_module
            data_frame = pd.DataFrame({
                "transport_zone_id": ["tz-fr", "tz-ch"],
                "local_admin_unit_id": ["fr-75056", "ch-2601"],
                "weight": [0.5, 0.5],
                "geometry": [None, None],
            })
            return geopandas_module.GeoDataFrame(data_frame, geometry="geometry")

    population = population_module.Population(
        transport_zones=TransportZonesWithSwiss(),
        sample_size=5,
        switzerland_census=None,
    )

    with pytest.raises(ValueError):
        population.get_swiss_pop_groups(
            transport_zones=population.inputs["transport_zones"].get(),
            legal_pop_by_city=pd.DataFrame({"local_admin_unit_id": ["ch-2601"], "legal_population": [5000]}),
            lau_to_tz_coeff=pd.DataFrame({
                "transport_zone_id": ["tz-ch"],
                "local_admin_unit_id": ["ch-2601"],
                "lau_to_tz_coeff": [1.0],
            }),
        )


def test_get_swiss_pop_groups_happy_path():
    """
    Provide a minimal Swiss census asset and verify output schema and 'country' marker.
    """
    class SwissCensusAssetFake:
        def get(self):
            return pd.DataFrame({
                "local_admin_unit_id": ["ch-2601", "ch-2601"],
                "individual_id": ["i1", "i2"],
                "age": [28, 40],
                "socio_pro_category": ["A", "B"],
                "ref_pers_socio_pro_category": ["RA", "RB"],
                "n_pers_household": [2, 3],
                "n_cars": [1, 0],
                "weight": [10.0, 30.0],
            })

    class TransportZonesSwissOnly:
        def __init__(self):
            self.inputs = {}
        def get(self):
            import geopandas as geopandas_module
            data_frame = pd.DataFrame({
                "transport_zone_id": ["tz-ch"],
                "local_admin_unit_id": ["ch-2601"],
                "weight": [1.0],
                "geometry": [None],
            })
            return geopandas_module.GeoDataFrame(data_frame, geometry="geometry")

    import mobility.parsers as parsers_module_local

    population = population_module.Population(
        transport_zones=TransportZonesSwissOnly(),
        sample_size=3,
        switzerland_census=SwissCensusAssetFake(),
    )

    transport_zones_geo_data_frame = population.inputs["transport_zones"].get()
    legal_population_by_city = parsers_module_local.CityLegalPopulation().get()
    lau_to_transport_zone_coefficients = (
        transport_zones_geo_data_frame[["transport_zone_id", "local_admin_unit_id", "weight"]]
        .rename(columns={"weight": "lau_to_tz_coeff"})
    )

    swiss_population_groups = population.get_swiss_pop_groups(
        transport_zones_geo_data_frame,
        legal_population_by_city,
        lau_to_transport_zone_coefficients,
    )

    expected_columns = {
        "transport_zone_id", "local_admin_unit_id", "age", "socio_pro_category",
        "ref_pers_socio_pro_category", "n_pers_household", "n_cars", "weight", "country",
    }
    assert expected_columns.issubset(swiss_population_groups.columns)
    assert (swiss_population_groups["country"] == "ch").all()
