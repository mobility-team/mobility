import pandas as pandas

import mobility.population as population_module


def test_get_sample_sizes_handles_missing_legal_population_row():
    """
    Cover the branch where some transport zones have no matching legal population.
    Expect those rows to be filled to 0.0, allocated 0 before max, then clamped to 1.
    Also verify the 'n_persons' column remains integer-typed.
    """
    population = population_module.Population(
        transport_zones=None,  # not used by get_sample_sizes in this test
        sample_size=7,
        switzerland_census=None,
    )

    lau_to_transport_zone_coefficients = pandas.DataFrame(
        {
            "transport_zone_id": ["tz-present", "tz-missing"],
            "local_admin_unit_id": ["fr-75056", "fr-00000"],  # second one intentionally absent
            "lau_to_tz_coeff": [1.0, 1.0],
        }
    )

    output_population_allocation = population.get_sample_sizes(
        lau_to_tz_coeff=lau_to_transport_zone_coefficients,
        sample_size=7,
    )

    # Confirm both rows are present
    assert set(output_population_allocation["transport_zone_id"]) == {"tz-present", "tz-missing"}

    # The missing row must become zero legal population after fillna, then clamped to at least 1 person
    missing_row = output_population_allocation.loc[
        output_population_allocation["transport_zone_id"] == "tz-missing"
    ].iloc[0]
    assert missing_row["legal_population"] == 0.0
    assert int(missing_row["n_persons"]) >= 1

    # Column type should be integer
    assert pandas.api.types.is_integer_dtype(output_population_allocation["n_persons"])
