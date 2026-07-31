from types import SimpleNamespace

import pandas as pd
import polars as pl

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.runtime.population_segments import (
    PopulationSegment,
    population_segment_defaults,
)
from mobility.transport.costs.parameters import GeneralizedCostParameters
from mobility.transport.costs.transport_costs import TransportCosts


class _GeneralizedCost(InMemoryAsset):
    calls = 0

    def get(self, metrics, **_kwargs):
        self.__class__.calls += 1
        parameters = self.inputs["parameters"]
        row = {
            "from": 1,
            "to": 2,
            "cost": parameters.cost_constant
            + 10.0 * parameters.cost_of_distance,
            "distance": 10.0,
            "time": 0.5,
        }
        return pd.DataFrame([{column: row[column] for column in ["from", "to"] + metrics}])


def test_identical_segment_coefficients_share_one_utility_profile():
    _GeneralizedCost.calls = 0
    transport_costs = _transport_costs(
        GeneralizedCostParameters(
            cost_constant=ParameterValue.by_population_segment(
                default=1.0,
                segment_values={"pupils": 3.0},
            ),
            cost_of_distance=2.0,
        )
    )
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "demand_subgroup_id": [0, 1, 0],
            "population_segments": [[], ["pupils"], ["pupils"]],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "population_segments": pl.List(pl.String),
        },
    )

    assignments, profiles = transport_costs.get_utility_profiles(
        demand_groups,
        [PopulationSegment(name="pupils", csp="8a")],
    )
    profile_costs = transport_costs.get_profile_costs_by_od_and_mode(
        profiles,
        ["cost", "time"],
    ).sort("utility_profile_id")

    assert assignments["utility_profile_id"].to_list() == [0, 1, 1]
    assert len(profiles) == 2
    assert profile_costs["cost"].to_list() == [21.0, 23.0]
    assert _GeneralizedCost.calls == 1


def test_generalized_costs_are_resolved_once_per_segment_membership():
    transport_costs = _transport_costs(
        GeneralizedCostParameters(
            cost_constant=ParameterValue.by_population_segment(
                default=1.0,
                segment_values={"pupils": 3.0},
            )
        )
    )
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1, 2, 3, 4],
            "demand_subgroup_id": [0, 0, 0, 0],
            "population_segments": [[], ["pupils"], ["pupils"], []],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "population_segments": pl.List(pl.String),
        },
    )
    original_resolver = transport_costs._resolved_generalized_cost
    resolved_memberships = []

    def recording_resolver(generalized_cost, *, memberships, population_segments):
        resolved_memberships.append(frozenset(memberships))
        return original_resolver(
            generalized_cost,
            memberships=memberships,
            population_segments=population_segments,
        )

    transport_costs._resolved_generalized_cost = recording_resolver

    assignments, _ = transport_costs.get_utility_profiles(
        demand_groups,
        [PopulationSegment(name="pupils", csp="8a")],
    )

    assert resolved_memberships == [frozenset(), frozenset({"pupils"})]
    assert assignments["utility_profile_id"].to_list() == [0, 1, 1, 0]


def _transport_costs(parameters):
    default_parameters = population_segment_defaults(parameters)
    generalized_cost = _GeneralizedCost({"parameters": parameters})
    mode = SimpleNamespace(
        inputs={
            "generalized_cost": generalized_cost,
            "parameters": SimpleNamespace(name="car"),
        }
    )
    transport_costs = TransportCosts.__new__(TransportCosts)
    transport_costs.modes = [mode]
    transport_costs.inputs = {
        "congestion": False,
        "road_flow_asset": None,
    }
    transport_costs.get_costs_by_od_and_mode = lambda metrics, **_kwargs: (
        pl.DataFrame(
            {
                "from": [1],
                "to": [2],
                "mode": ["car"],
                "cost": [
                    default_parameters.cost_constant
                    + 10.0 * default_parameters.cost_of_distance
                ],
                "distance": [10.0],
                "time": [0.5],
            }
        ).select(["from", "to", "mode"] + metrics)
    )
    return transport_costs
