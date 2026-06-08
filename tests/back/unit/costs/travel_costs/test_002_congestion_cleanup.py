import pytest

from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.costs.travel_costs_asset import TravelCostsBase
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.transport.modes.carpool.detailed import (
    detailed_carpool_travel_costs as detailed_carpool_module,
)
from mobility.transport.modes.carpool.detailed.detailed_carpool_travel_costs import (
    DetailedCarpoolTravelCosts,
)
from mobility.transport.modes.carpool.detailed.detailed_carpool_travel_costs import (
    DetailedCarpoolTravelCostsTable,
)
from mobility.transport.modes.public_transport.public_transport_travel_costs import (
    PublicTransportTravelCosts,
)


class _Removable:
    def __init__(self, *, inputs=None):
        self.inputs = inputs or {}
        self.remove_calls = 0

    def remove(self):
        self.remove_calls += 1


class _CostTable:
    def __init__(self, value):
        self.value = value
        self.get_calls = 0

    def get(self):
        self.get_calls += 1
        return self.value


class _CongestedPathGraph:
    def __init__(self, *, handles_congestion):
        self.inputs = {"handles_congestion": handles_congestion}


class _FakeAsset:
    def __init__(self, *, inputs=None):
        self.inputs = inputs or {}
        for name, value in self.inputs.items():
            setattr(self, name, value)

    def get_cached_hash(self):
        return "fake-hash"


class _PathReturningAsset:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


class _FakeScriptRunner:
    instances = []

    def __init__(self, script_path):
        self.script_path = script_path
        self.args = None
        _FakeScriptRunner.instances.append(self)

    def run(self, *, args):
        self.args = args


class _LegTravelCosts(TravelCostsBase):
    def __init__(self, variant):
        self.variant = variant
        self.inputs = {}

    def asset_for_road_flows(self, road_flow_asset):
        return self.variant

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None

    def get(self):
        return None


def _make_public_transport_asset(first_leg, last_leg):
    asset = object.__new__(PublicTransportTravelCosts)
    asset.first_leg_travel_costs = first_leg
    asset.last_leg_travel_costs = last_leg
    asset.first_leg_mode_name = "walk"
    asset.last_leg_mode_name = "car"
    asset.inputs = {
        "transport_zones": object(),
        "parameters": object(),
        "first_modal_transfer": object(),
        "last_modal_transfer": object(),
        "intermodal_graph": _Removable(),
    }
    return asset


def test_public_transport_asset_for_road_flows_returns_self_when_legs_do_not_change():
    first_leg = _LegTravelCosts(None)
    last_leg = _LegTravelCosts(None)
    asset = _make_public_transport_asset(first_leg, last_leg)

    assert asset.asset_for_road_flows(object()) is asset


def test_public_transport_asset_for_road_flows_rebinds_changed_leg(monkeypatch):
    first_leg_variant = object()
    first_leg = _LegTravelCosts(first_leg_variant)
    last_leg = _LegTravelCosts(None)
    asset = _make_public_transport_asset(first_leg, last_leg)

    def fake_init(
        self,
        transport_zones,
        parameters,
        first_leg_travel_costs,
        last_leg_travel_costs,
        first_leg_mode_name,
        last_leg_mode_name,
        first_modal_transfer=None,
        last_modal_transfer=None,
    ):
        self.first_leg_travel_costs = first_leg_travel_costs
        self.last_leg_travel_costs = last_leg_travel_costs
        self.first_leg_mode_name = first_leg_mode_name
        self.last_leg_mode_name = last_leg_mode_name
        self.inputs = {
            "transport_zones": transport_zones,
            "parameters": parameters,
            "first_modal_transfer": first_modal_transfer,
            "last_modal_transfer": last_modal_transfer,
            "intermodal_graph": _Removable(),
        }

    monkeypatch.setattr(PublicTransportTravelCosts, "__init__", fake_init)

    variant = asset.asset_for_road_flows(object())

    assert variant is not asset
    assert variant.first_leg_travel_costs is first_leg_variant
    assert variant.last_leg_travel_costs is last_leg
    assert variant.first_leg_mode_name == "walk"
    assert variant.last_leg_mode_name == "car"


def test_public_transport_remove_congestion_artifacts_removes_owned_variant():
    asset = _make_public_transport_asset(first_leg=object(), last_leg=object())
    intermodal_graph = _Removable()
    variant = _Removable(inputs={"intermodal_graph": intermodal_graph})
    asset.asset_for_road_flows = lambda road_flow_asset: variant

    asset.remove_congestion_artifacts(object())

    assert variant.remove_calls == 1
    assert intermodal_graph.remove_calls == 1


def test_public_transport_remove_congestion_artifacts_is_noop_when_variant_is_self():
    asset = _make_public_transport_asset(first_leg=object(), last_leg=object())
    asset.asset_for_road_flows = lambda road_flow_asset: asset

    asset.remove_congestion_artifacts(object())

    assert asset.inputs["intermodal_graph"].remove_calls == 0


def _make_path_travel_costs():
    asset = object.__new__(PathTravelCosts)
    asset.inputs = {"mode_name": "car"}
    return asset


def _patch_asset_initializers(monkeypatch):
    def fake_file_asset_init(self, inputs, cache_path):
        self.inputs = inputs
        self.cache_path = cache_path

    def fake_in_memory_asset_init(self, inputs):
        self.inputs = inputs
        for name, value in inputs.items():
            setattr(self, name, value)

    monkeypatch.setattr(FileAsset, "__init__", fake_file_asset_init)
    monkeypatch.setattr(InMemoryAsset, "__init__", fake_in_memory_asset_init)


def test_path_constructor_can_reuse_congested_graph_chain(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    modified_graph = _FakeAsset()
    congested_graph = _FakeAsset(
        inputs={
            "modified_graph": modified_graph,
            "handles_congestion": True,
        }
    )
    contracted_graph = _FakeAsset(inputs={"congested_graph": congested_graph})

    asset = PathTravelCosts(
        mode_name="car",
        transport_zones=_FakeAsset(),
        routing_parameters=_FakeAsset(),
        osm_capacity_parameters=_FakeAsset(),
        contracted_graph=contracted_graph,
        default_congestion=True,
    )

    assert asset.modified_path_graph is modified_graph
    assert asset.congested_path_graph is congested_graph
    assert asset.contracted_path_graph is contracted_graph
    assert asset.default_congestion is True


def test_path_get_uses_freeflow_without_road_flow_asset():
    asset = _make_path_travel_costs()
    asset.freeflow_costs = _CostTable("free-flow")
    asset.congested_costs = _CostTable("congested")
    asset.default_congestion = False
    asset.congested_path_graph = _CongestedPathGraph(handles_congestion=True)

    assert asset.get() == "free-flow"
    assert asset.get(congestion=True) == "free-flow"
    assert asset.freeflow_costs.get_calls == 2
    assert asset.congested_costs.get_calls == 0


def test_path_get_defaults_to_congested_for_road_flow_variant():
    asset = _make_path_travel_costs()
    asset.freeflow_costs = _CostTable("free-flow")
    asset.congested_costs = _CostTable("congested")
    asset.default_congestion = True

    assert asset.get() == "congested"
    assert asset.freeflow_costs.get_calls == 0
    assert asset.congested_costs.get_calls == 1


def test_path_get_uses_road_flow_variant_when_requested():
    asset = _make_path_travel_costs()
    variant = _CostTable("road-flow-costs")
    asset._handles_congestion = lambda: True
    asset.asset_for_road_flows = lambda road_flow_asset: variant

    assert asset.get(congestion=True, road_flow_asset=object()) == "road-flow-costs"


def test_path_get_uses_freeflow_when_mode_does_not_handle_congestion():
    asset = _make_path_travel_costs()
    asset.freeflow_costs = _CostTable("free-flow")
    asset.congested_costs = _CostTable("congested")
    asset.default_congestion = False
    asset.congested_path_graph = _CongestedPathGraph(handles_congestion=False)

    assert asset.get(congestion=True) == "free-flow"
    assert asset.freeflow_costs.get_calls == 1
    assert asset.congested_costs.get_calls == 0


def test_detailed_carpool_get_uses_freeflow_without_road_flow_asset():
    asset = object.__new__(DetailedCarpoolTravelCosts)
    asset.freeflow_costs = _CostTable("free-flow")
    asset.congested_costs = _CostTable("congested")
    asset.default_congestion = False

    assert asset.get() == "free-flow"
    assert asset.get(congestion=True) == "free-flow"


def test_detailed_carpool_get_defaults_to_congested_for_road_flow_variant():
    asset = object.__new__(DetailedCarpoolTravelCosts)
    asset.freeflow_costs = _CostTable("free-flow")
    asset.congested_costs = _CostTable("congested")
    asset.default_congestion = True

    assert asset.get() == "congested"


def test_detailed_carpool_get_uses_road_flow_variant_when_requested():
    asset = object.__new__(DetailedCarpoolTravelCosts)
    variant = _CostTable("road-flow-carpool")
    asset.asset_for_road_flows = lambda road_flow_asset: variant
    asset.default_congestion = False
    asset.freeflow_costs = _CostTable("free-flow")

    assert asset.get(congestion=True, road_flow_asset=object()) == "road-flow-carpool"


def test_detailed_carpool_remove_congestion_artifacts_keeps_shared_freeflow_table():
    asset = object.__new__(DetailedCarpoolTravelCosts)
    freeflow_costs = _Removable()
    congested_costs = _Removable()
    variant = object.__new__(DetailedCarpoolTravelCosts)
    variant.freeflow_costs = freeflow_costs
    variant.congested_costs = congested_costs
    asset.asset_for_road_flows = lambda road_flow_asset: variant

    asset.remove_congestion_artifacts(object())

    assert freeflow_costs.remove_calls == 0
    assert congested_costs.remove_calls == 1


def test_path_get_congested_graph_path_uses_flow_variant():
    asset = _make_path_travel_costs()
    variant = _FakeAsset(
        inputs={
            "congested_path_graph": _PathReturningAsset("variant-graph.gpkg"),
        }
    )
    asset.asset_for_flow_asset = lambda flow_asset: variant

    assert asset.get_congested_graph_path(object()) == "variant-graph.gpkg"


def test_path_get_congested_graph_path_uses_base_graph_without_flow():
    asset = _make_path_travel_costs()
    asset.congested_path_graph = _PathReturningAsset("base-graph.gpkg")

    assert asset.get_congested_graph_path() == "base-graph.gpkg"


def test_path_remove_removes_both_cost_tables():
    asset = _make_path_travel_costs()
    asset.freeflow_costs = _Removable()
    asset.congested_costs = _Removable()

    asset.remove()

    assert asset.freeflow_costs.remove_calls == 1
    assert asset.congested_costs.remove_calls == 1


def test_detailed_carpool_table_init_builds_congested_cache_path(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    road_flow_asset = _FakeAsset()

    table = DetailedCarpoolTravelCostsTable(
        car_travel_costs=_FakeAsset(),
        parameters=_FakeAsset(),
        modal_transfer=_FakeAsset(),
        congestion=True,
        road_flow_asset=road_flow_asset,
    )

    assert table.inputs["road_flow_asset"] is road_flow_asset
    assert table.cache_path == project_dir / "travel_costs_congested_carpool.parquet"


def test_detailed_carpool_table_get_cached_asset_reads_cache(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    table = DetailedCarpoolTravelCostsTable(
        car_travel_costs=_FakeAsset(),
        parameters=_FakeAsset(),
        modal_transfer=_FakeAsset(),
        congestion=False,
    )
    monkeypatch.setattr(
        detailed_carpool_module.pd,
        "read_parquet",
        lambda cache_path: f"read {cache_path.name}",
    )

    assert table.get_cached_asset() == f"read {table.cache_path.name}"


def test_detailed_carpool_table_create_and_get_asset_delegates(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    table = DetailedCarpoolTravelCostsTable(
        car_travel_costs=_FakeAsset(),
        parameters=_FakeAsset(),
        modal_transfer=_FakeAsset(),
        congestion=False,
    )
    table._compute_travel_costs = lambda: "computed-carpool-costs"

    assert table.create_and_get_asset() == "computed-carpool-costs"


def test_detailed_carpool_table_compute_uses_congested_graph(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    _FakeScriptRunner.instances = []
    road_flow_asset = _FakeAsset()
    car_travel_costs = _FakeAsset(
        inputs={
            "transport_zones": _FakeAsset(
                inputs={
                    "cache_path": project_dir / "zones.gpkg",
                    "study_area": _FakeAsset(
                        inputs={"cache_path": {"polygons": project_dir / "area.gpkg"}}
                    ),
                }
            )
        }
    )
    car_travel_costs.get_congested_graph_path = lambda flow_asset: project_dir / "congested.gpkg"
    modal_transfer = _FakeAsset()
    modal_transfer.model_dump = lambda mode: {"access": "walk"}
    table = DetailedCarpoolTravelCostsTable(
        car_travel_costs=car_travel_costs,
        parameters=_FakeAsset(),
        modal_transfer=modal_transfer,
        congestion=True,
        road_flow_asset=road_flow_asset,
    )
    monkeypatch.setattr(detailed_carpool_module, "RScriptRunner", _FakeScriptRunner)
    monkeypatch.setattr(
        detailed_carpool_module.pd,
        "read_parquet",
        lambda cache_path: "computed-carpool-costs",
    )

    assert table._compute_travel_costs() == "computed-carpool-costs"
    assert _FakeScriptRunner.instances[0].args[2] == str(project_dir / "congested.gpkg")


def test_detailed_carpool_table_compute_uses_modified_graph(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    car_travel_costs = _FakeAsset(
        inputs={
            "transport_zones": _FakeAsset(
                inputs={
                    "cache_path": project_dir / "zones.gpkg",
                    "study_area": _FakeAsset(
                        inputs={"cache_path": {"polygons": project_dir / "area.gpkg"}}
                    ),
                }
            ),
            "modified_path_graph": _PathReturningAsset(project_dir / "modified.gpkg"),
        }
    )
    modal_transfer = _FakeAsset()
    modal_transfer.model_dump = lambda mode: {"access": "walk"}
    table = DetailedCarpoolTravelCostsTable(
        car_travel_costs=car_travel_costs,
        parameters=_FakeAsset(),
        modal_transfer=modal_transfer,
        congestion=False,
    )
    monkeypatch.setattr(detailed_carpool_module, "RScriptRunner", _FakeScriptRunner)
    monkeypatch.setattr(
        detailed_carpool_module.pd,
        "read_parquet",
        lambda cache_path: "computed-carpool-costs",
    )

    assert table._compute_travel_costs() == "computed-carpool-costs"


def test_detailed_carpool_constructor_marks_road_flow_variant(project_dir, monkeypatch):
    _patch_asset_initializers(monkeypatch)
    road_flow_asset = _FakeAsset()

    asset = DetailedCarpoolTravelCosts(
        car_travel_costs=_FakeAsset(),
        parameters=_FakeAsset(),
        modal_transfer=_FakeAsset(),
        road_flow_asset=road_flow_asset,
    )

    assert asset.road_flow_asset is road_flow_asset
    assert asset.default_congestion is True
    assert asset.congested_costs.road_flow_asset is road_flow_asset
    assert asset.freeflow_costs.road_flow_asset is None


def test_detailed_carpool_remove_removes_both_cost_tables():
    asset = object.__new__(DetailedCarpoolTravelCosts)
    asset.freeflow_costs = _Removable()
    asset.congested_costs = _Removable()

    asset.remove()

    assert asset.freeflow_costs.remove_calls == 1
    assert asset.congested_costs.remove_calls == 1


@pytest.mark.parametrize("variant_factory", [lambda asset: None, lambda asset: asset])
def test_detailed_carpool_remove_congestion_artifacts_returns_early(variant_factory):
    asset = object.__new__(DetailedCarpoolTravelCosts)
    asset.asset_for_road_flows = lambda road_flow_asset: variant_factory(asset)

    asset.remove_congestion_artifacts(object())


def test_path_asset_for_road_flows_ignores_modes_that_do_not_handle_congestion():
    asset = _make_path_travel_costs()
    asset.inputs["congested_path_graph"] = _Removable(
        inputs={"handles_congestion": False}
    )

    assert asset.asset_for_road_flows(object()) is None


@pytest.mark.parametrize("variant_factory", [lambda asset: None, lambda asset: asset])
def test_path_remove_congestion_artifacts_returns_early_for_missing_variant(variant_factory):
    asset = _make_path_travel_costs()
    asset.asset_for_road_flows = lambda road_flow_asset: variant_factory(asset)

    asset.remove_congestion_artifacts(object())


def test_path_remove_congestion_artifacts_removes_owned_graph_chain():
    asset = _make_path_travel_costs()
    congested_graph = _Removable()
    contracted_graph = _Removable(inputs={"congested_graph": congested_graph})
    variant = _Removable(inputs={"contracted_path_graph": contracted_graph})
    asset.asset_for_road_flows = lambda road_flow_asset: variant

    asset.remove_congestion_artifacts(object())

    assert variant.remove_calls == 1
    assert contracted_graph.remove_calls == 1
    assert congested_graph.remove_calls == 1

