import pytest

from mobility.runtime.parameter_values import (
    DEFAULT_SCENARIO,
    ParameterValue,
    resolve_parameter_values,
)
from mobility.transport.costs.parameters.generalized_cost_parameters import GeneralizedCostParameters
from mobility.transport.costs.parameters.path_routing_parameters import PathRoutingParameters
from mobility.transport.modes.public_transport.public_transport_graph import (
    PublicTransportRoutingParameters,
)


def test_path_routing_parameters_accept_explicit_max_beeline_distance():
    params = PathRoutingParameters(max_beeline_distance=80.0)

    assert params.max_beeline_distance == 80.0
    assert params.filter_max_speed is None
    assert params.filter_max_time is None


def test_path_routing_parameters_rejects_negative_max_beeline_distance():
    with pytest.raises(ValueError, match="max_beeline_distance should be greater than 0"):
        PathRoutingParameters(max_beeline_distance=-1.0)


def test_path_routing_parameters_validates_scenario_value_after_resolution():
    params = PathRoutingParameters(
        max_beeline_distance=ParameterValue.by_scenario(default=80.0, project=-1.0)
    )

    assert resolve_parameter_values(params).max_beeline_distance == 80.0
    with pytest.raises(ValueError, match="max_beeline_distance should be greater than 0"):
        resolve_parameter_values(params, scenario="project")


def test_path_routing_parameters_requires_explicit_or_legacy_definition():
    with pytest.raises(ValueError, match="requires `max_beeline_distance`"):
        PathRoutingParameters()


def test_path_routing_parameters_normalizes_legacy_speed_time_inputs():
    with pytest.deprecated_call():
        params = PathRoutingParameters(filter_max_speed=60.0, filter_max_time=1.0)

    assert params.max_beeline_distance == 60.0
    assert params.filter_max_speed is None
    assert params.filter_max_time is None


def test_public_transport_routing_parameters_exposes_explicit_outer_distance():
    params = PublicTransportRoutingParameters()

    assert params.max_beeline_distance == 80.0


def test_public_transport_routing_parameters_resolve_list_profiles_by_iteration():
    params = PublicTransportRoutingParameters(
        additional_gtfs_files=ParameterValue.by_iteration(
            {
                1: ["base.zip"],
                2: ["base.zip", "event.zip"],
            },
        )
    )

    step_1 = resolve_parameter_values(params, iteration=1)
    step_2 = resolve_parameter_values(params, iteration=2)

    assert step_1.additional_gtfs_files == ["base.zip"]
    assert step_2.additional_gtfs_files == ["base.zip", "event.zip"]


def test_public_transport_routing_parameters_resolve_scenario_gtfs_files():
    params = PublicTransportRoutingParameters(
        additional_gtfs_files=ParameterValue.by_scenario_and_iteration(
            baseline=None,
            saleve_jura={
                1: [],
                5: ["rer.zip"],
            },
        )
    )

    assert params.additional_gtfs_files.scenario_names() == {"baseline", "saleve_jura"}

    baseline = resolve_parameter_values(params, scenario="baseline", iteration=1)
    saleve_jura_1 = resolve_parameter_values(params, scenario="saleve_jura", iteration=1)
    saleve_jura_5 = resolve_parameter_values(params, scenario="saleve_jura", iteration=5)

    assert isinstance(baseline, PublicTransportRoutingParameters)
    assert baseline.additional_gtfs_files is None

    assert isinstance(saleve_jura_1, PublicTransportRoutingParameters)
    assert saleve_jura_1.additional_gtfs_files == []
    assert saleve_jura_5.additional_gtfs_files == ["rer.zip"]


def test_scenario_generalized_cost_parameter_resolves_to_plain_value():
    generalized_cost_parameters = GeneralizedCostParameters(
        cost_constant=ParameterValue.by_scenario(default=1.0, zone30=2.0)
    )

    assert DEFAULT_SCENARIO == "default"
    assert resolve_parameter_values(generalized_cost_parameters).cost_constant == 1.0
    assert (
        resolve_parameter_values(
            generalized_cost_parameters,
            scenario="default",
        ).cost_constant
        == 1.0
    )
    assert (
        resolve_parameter_values(
            generalized_cost_parameters,
            scenario="zone30",
        ).cost_constant
        == 2.0
    )

    with pytest.raises(ValueError, match="Scenario 'missing' is not defined"):
        resolve_parameter_values(
            generalized_cost_parameters,
            scenario="missing",
        )


def test_scenario_generalized_cost_parameter_requires_defined_default_scenario():
    generalized_cost_parameters = GeneralizedCostParameters(
        cost_constant=ParameterValue.by_scenario(baseline=1.0, zone30=2.0)
    )

    with pytest.raises(ValueError, match="Scenario 'default' is not defined"):
        resolve_parameter_values(generalized_cost_parameters)
