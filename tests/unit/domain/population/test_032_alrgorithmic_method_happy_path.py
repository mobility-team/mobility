import pandas as pd

import mobility.population as population_module


def test_get_sample_sizes_happy_path(fake_transport_zones):
    """
    Validate allocation basics: integer type, >= 1 per zone, and non-pathological totals.
    """
    population = population_module.Population(
        transport_zones=fake_transport_zones,
        sample_size=10,
        switzerland_census=None,
    )

    transport_zones_geo_data_frame = fake_transport_zones.get()
    lau_to_transport_zone_coefficients = (
        transport_zones_geo_data_frame[["transport_zone_id", "local_admin_unit_id", "weight"]]
        .rename(columns={"weight": "lau_to_tz_coeff"})
    )

    output_population_allocation = population.get_sample_sizes(
        lau_to_tz_coeff=lau_to_transport_zone_coefficients,
        sample_size=10,
    )

    # Schema
    assert {"transport_zone_id", "local_admin_unit_id", "n_persons", "legal_population"}.issubset(
        output_population_allocation.columns
    )

    # Types and invariants
    assert pd.api.types.is_integer_dtype(output_population_allocation["n_persons"])
    assert (output_population_allocation["n_persons"] >= 1).all()
    assert output_population_allocation["n_persons"].sum() >= len(
        output_population_allocation["transport_zone_id"].unique()
    )
