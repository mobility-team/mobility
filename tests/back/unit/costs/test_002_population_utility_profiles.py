from types import SimpleNamespace
import math

import pandas as pd
import polars as pl
from polars.testing import assert_frame_equal

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.runtime.population_segments import (
    PopulationSegment,
    population_segment_defaults,
)
from mobility.transport.costs.parameters import GeneralizedCostParameters
from mobility.transport.costs.transport_costs import TransportCosts
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationTransportCostsAsset


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


def test_profile_assignment_preserves_first_seen_order_and_normalizes_memberships():
    transport_costs = _transport_costs(GeneralizedCostParameters(
        cost_constant=ParameterValue.by_population_segment(default=1.0, segment_values={"pupils": 3.0}),
    ))
    groups = pl.DataFrame({
        "demand_group_id": [6, 1, 5, 2, 4, 3], "demand_subgroup_id": [0] * 6,
        "population_segments": [["pupils", "other", "pupils"], None, ["other"],
                                ["pupils"], ["other", "pupils"], []],
    }, schema={"demand_group_id": pl.UInt32, "demand_subgroup_id": pl.UInt32,
               "population_segments": pl.List(pl.String)})
    assignments, profiles = transport_costs.get_utility_profiles(
        groups, [PopulationSegment(name="pupils", csp="8a")],
    )
    assert assignments["demand_group_id"].to_list() == [6, 1, 5, 2, 4, 3]
    assert assignments["utility_profile_id"].to_list() == [0, 1, 1, 0, 0, 1]
    assert len(profiles) == 2


def test_profile_costs_reuse_provided_default_and_keep_segment_overrides():
    transport_costs = _transport_costs(GeneralizedCostParameters(
        cost_constant=ParameterValue.by_population_segment(default=1.0, segment_values={"pupils": 3.0}),
        cost_of_distance=2.0,
    ))
    groups = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0],
        "population_segments": [[], ["pupils"]],
    }, schema={"demand_group_id": pl.UInt32, "demand_subgroup_id": pl.UInt32,
               "population_segments": pl.List(pl.String)})
    _, profiles = transport_costs.get_utility_profiles(groups, [PopulationSegment(name="pupils", csp="8a")])
    metrics = ["cost", "distance", "time"]
    defaults = transport_costs.get_costs_by_od_and_mode(metrics)
    expected = transport_costs.get_profile_costs_by_od_and_mode(profiles, metrics).sort("utility_profile_id")

    def unexpected_rebuild(*args, **kwargs):
        raise AssertionError("The supplied default costs should be reused.")

    transport_costs.get_costs_by_od_and_mode = unexpected_rebuild
    _GeneralizedCost.calls = 0
    actual = transport_costs.get_profile_costs_by_od_and_mode(
        profiles, metrics, default_costs=defaults,
    ).sort("utility_profile_id")
    assert_frame_equal(actual, expected)
    assert _GeneralizedCost.calls == 1  # The overridden profile still needs its own cost.


def test_profile_costs_can_be_limited_to_requested_pairs():
    transport_costs = _transport_costs(GeneralizedCostParameters(
        cost_constant=ParameterValue.by_population_segment(default=1.0, segment_values={"pupils": 3.0}),
        cost_of_distance=2.0,
    ))
    groups = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0],
        "population_segments": [[], ["pupils"]],
    }, schema={"demand_group_id": pl.UInt32, "demand_subgroup_id": pl.UInt32,
               "population_segments": pl.List(pl.String)})
    _, profiles = transport_costs.get_utility_profiles(groups, [PopulationSegment(name="pupils", csp="8a")])
    pairs = pl.DataFrame({"utility_profile_id": [0, 1], "from": [9, 1], "to": [9, 2]},
                         schema={"utility_profile_id": pl.UInt32, "from": pl.Int64, "to": pl.Int64})
    full = transport_costs.get_profile_costs_by_od_and_mode(profiles, ["cost", "time"])
    expected = full.join(pairs, on=["utility_profile_id", "from", "to"], how="semi")
    actual = transport_costs.get_profile_costs_by_od_and_mode(profiles, ["cost", "time"], od_pairs=pairs)
    assert_frame_equal(actual, expected)
    assert actual["cost"].to_list() == [23.0]
    empty = transport_costs.get_profile_costs_by_od_and_mode(profiles, ["cost", "time"], od_pairs=pairs.head(0))
    assert_frame_equal(empty, expected.head(0))


def test_congested_iteration_reuses_its_default_cost_table_for_profiles():
    defaults = pl.DataFrame({
        "from": [1], "to": [2], "mode": ["car"], "cost": [3.0], "distance": [4.0], "time": [5.0],
    })
    flow = object()
    observed = []

    def profile_costs(profiles, metrics, *, default_costs):
        observed.append((profiles, metrics, default_costs))
        return default_costs.with_columns(utility_profile_id=pl.lit(0, dtype=pl.UInt32))

    asset = object.__new__(IterationTransportCostsAsset)
    asset.congestion_flows = SimpleNamespace(get=lambda: flow)
    asset.transport_costs = SimpleNamespace(asset_for_road_flows=lambda value: (
        SimpleNamespace(get_profile_costs_by_od_and_mode=profile_costs) if value is flow else None
    ))
    asset.get_costs_by_od_and_mode = lambda metrics: defaults.select(["from", "to", "mode"] + metrics)
    asset._profile_costs_cache = None
    asset._profile_costs_cache_key = None
    profiles = {0: []}
    result = asset.get_profile_costs_by_od_and_mode(profiles, ["cost", "time"])
    asset.get_profile_costs_by_od_and_mode(profiles, ["distance"])
    assert len(observed) == 1
    assert_frame_equal(observed[0][2], defaults)
    assert result["cost"].to_list() == [3.0]
    asset.release_profile_costs()
    assert asset._profile_costs_cache is None
    assert asset._profile_costs_cache_key is None
    rebuilt = asset.get_profile_costs_by_od_and_mode(profiles, ["cost", "time"])
    assert len(observed) == 2
    assert_frame_equal(rebuilt, result)


def test_filtered_iteration_costs_do_not_replace_full_profile_cache():
    defaults = pl.DataFrame({
        "from": [1, 3], "to": [2, 4], "mode": ["car", "car"],
        "cost": [3.0, 7.0], "distance": [4.0, 8.0], "time": [5.0, 9.0],
    })
    flow = object()
    calls = []

    def profile_costs(profiles, metrics, *, default_costs, od_pairs=None):
        calls.append(od_pairs)
        costs = default_costs.with_columns(utility_profile_id=pl.lit(0, dtype=pl.UInt32))
        if od_pairs is not None:
            costs = costs.join(od_pairs, on=["utility_profile_id", "from", "to"], how="semi")
        return costs.select(["from", "to", "mode", "utility_profile_id"] + metrics)

    asset = object.__new__(IterationTransportCostsAsset)
    asset.congestion_flows = SimpleNamespace(get=lambda: flow)
    asset.transport_costs = SimpleNamespace(asset_for_road_flows=lambda _: SimpleNamespace(
        get_profile_costs_by_od_and_mode=profile_costs,
    ))
    asset.get_costs_by_od_and_mode = lambda metrics: defaults.select(["from", "to", "mode"] + metrics)
    asset._profile_costs_cache = None
    asset._profile_costs_cache_key = None
    profiles = {0: []}
    full = asset.get_profile_costs_by_od_and_mode(profiles, ["cost"])
    pairs = pl.DataFrame({"utility_profile_id": [0], "from": [1], "to": [2]},
                         schema={"utility_profile_id": pl.UInt32, "from": pl.Int64, "to": pl.Int64})
    filtered = asset.get_profile_costs_by_od_and_mode(profiles, ["cost"], od_pairs=pairs)
    assert filtered["cost"].to_list() == [3.0]
    assert len(calls) == 1  # The matching full table can serve the narrower request.
    assert_frame_equal(asset.get_profile_costs_by_od_and_mode(profiles, ["cost"]), full)
    asset.release_profile_costs()
    assert_frame_equal(asset.get_profile_costs_by_od_and_mode(profiles, ["cost"], od_pairs=pairs), filtered)
    assert_frame_equal(asset.get_profile_costs_by_od_and_mode(profiles, ["cost"]), full)
    assert len(calls) == 3  # A narrow result must never replace the full cache.


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


def test_expected_od_cost_handles_very_large_finite_costs():
    """A congested OD must not produce 0/0 when every mode has a large cost."""
    costs = pl.DataFrame({
        "from": [1, 2, 2], "to": [2, 3, 3],
        "mode": ["car", "car", "walk"], "cost": [1727.65, 1000.0, 1001.0],
    })
    expected = 1000.0 + math.exp(-1.0) / (1.0 + math.exp(-1.0))
    for cls in (TransportCosts, IterationTransportCostsAsset):
        asset = object.__new__(cls)
        asset.get_costs_by_od_and_mode = lambda metrics, **kwargs: costs
        result = asset.get_costs_by_od(["cost"]).sort("from")
        assert result["cost"].is_finite().all()
        assert abs(result["cost"][0] - 1727.65) < 1e-10
        assert abs(result["cost"][1] - expected) < 1e-10
