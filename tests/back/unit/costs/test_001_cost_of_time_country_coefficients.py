import numpy as np

from mobility.transport.costs.parameters.cost_of_time_parameters import CostOfTimeParameters


def test_cost_of_time_country_coefficients_use_lookup_map():
    params = CostOfTimeParameters(
        intercept=10.0,
        breaks=[0.0, 100.0],
        slopes=[0.0],
        max_value=20.0,
        country_coefficients={"de": 1.5},
    )

    values = params.compute(
        np.array([10.0, 10.0], dtype=float),
        np.array(["de", "fr"], dtype=object),
    )

    assert values.tolist() == [15.0, 10.0]
