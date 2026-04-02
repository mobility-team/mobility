import pytest

from mobility.runtime.parameter_profiles import (
    ListParameterProfile,
    resolve_model_for_iteration,
)
from mobility.transport.costs.parameters.path_routing_parameters import PathRoutingParameters
from mobility.transport.modes.public_transport.public_transport_graph import (
    PublicTransportRoutingParameters,
)


def test_path_routing_parameters_accept_explicit_max_beeline_distance():
    params = PathRoutingParameters(max_beeline_distance=80.0)

    assert params.max_beeline_distance == 80.0
    assert params.filter_max_speed is None
    assert params.filter_max_time is None


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
        additional_gtfs_files=ListParameterProfile(
            points={
                1: ["base.zip"],
                2: ["base.zip", "event.zip"],
            }
        )
    )

    step_1 = resolve_model_for_iteration(params, 1)
    step_2 = resolve_model_for_iteration(params, 2)

    assert step_1.additional_gtfs_files == ["base.zip"]
    assert step_2.additional_gtfs_files == ["base.zip", "event.zip"]
