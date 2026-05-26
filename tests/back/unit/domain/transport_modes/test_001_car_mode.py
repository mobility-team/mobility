from types import SimpleNamespace

from mobility.transport.modes import car as car_module


def test_car_mode_forwards_congestion_assignment_parameters(monkeypatch):
    """Check that public car-mode settings reach path travel costs."""
    seen = {}

    class FakePathTravelCosts:
        def __init__(
            self,
            *,
            mode_name,
            transport_zones,
            routing_parameters,
            osm_capacity_parameters,
            congestion,
            congestion_flows_scaling_factor,
            target_max_vehicles_per_od_endpoint,
            congestion_assignment_max_iterations,
            congestion_assignment_max_gap,
            congestion_assignment_retained_volume_share,
            speed_modifiers,
        ):
            seen.update(
                {
                    "mode_name": mode_name,
                    "congestion": congestion,
                    "target_max_vehicles_per_od_endpoint": (
                        target_max_vehicles_per_od_endpoint
                    ),
                    "congestion_assignment_max_iterations": (
                        congestion_assignment_max_iterations
                    ),
                    "congestion_assignment_max_gap": congestion_assignment_max_gap,
                    "congestion_assignment_retained_volume_share": (
                        congestion_assignment_retained_volume_share
                    ),
                    "speed_modifiers": speed_modifiers,
                }
            )

    class FakePathGeneralizedCost:
        def __init__(self, travel_costs, generalized_cost_parameters, mode_name):
            self.travel_costs = travel_costs
            self.generalized_cost_parameters = generalized_cost_parameters
            self.mode_name = mode_name

    monkeypatch.setattr(car_module, "PathTravelCosts", FakePathTravelCosts)
    monkeypatch.setattr(car_module, "PathGeneralizedCost", FakePathGeneralizedCost)
    monkeypatch.setattr(
        car_module.TransportMode,
        "compute_inputs_hash",
        lambda self: "fake-car-mode-hash",
    )

    car_module.CarMode(
        transport_zones=SimpleNamespace(),
        congestion=True,
        target_max_vehicles_per_od_endpoint=500.0,
        congestion_assignment_max_iterations=7,
        congestion_assignment_max_gap=0.03,
        congestion_assignment_retained_volume_share=0.8,
        speed_modifiers=["speed-modifier"],
    )

    assert seen == {
        "mode_name": "car",
        "congestion": True,
        "target_max_vehicles_per_od_endpoint": 500.0,
        "congestion_assignment_max_iterations": 7,
        "congestion_assignment_max_gap": 0.03,
        "congestion_assignment_retained_volume_share": 0.8,
        "speed_modifiers": ["speed-modifier"],
    }
