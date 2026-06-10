from types import SimpleNamespace

from pydantic import BaseModel

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.graph import build_asset_graph
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.iterations import iteration_assets
from mobility.trips.group_day_trips.core import run as run_module
from mobility.trips.group_day_trips.core.parameters import (
    GroupDayTripsParameters,
    GroupDayTripsRunParameters,
)
from mobility.trips.group_day_trips.core.run import Run


class _MemoryAsset(InMemoryAsset):
    def __init__(self, name):
        super().__init__({"name": name})


class _FinalStateAsset(FileAsset):
    def __init__(self, cache_folder):
        super().__init__({"name": "final-state"}, cache_folder / "final_state.json")

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None


class _ModeParameters(BaseModel):
    name: str
    survey_ids: list[str]


class _ModeAsset(InMemoryAsset):
    def __init__(self, *, name, hidden_file_asset):
        super().__init__(
            {
                "parameters": _ModeParameters(
                    name=name,
                    survey_ids=[],
                ),
                "travel_costs": hidden_file_asset,
            }
        )


def test_run_inputs_keep_context_objects_out_of_direct_dependencies(tmp_path, monkeypatch):
    """Run keeps setup objects as attributes, not as direct execution inputs."""
    final_state = _FinalStateAsset(tmp_path)

    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(
        run_module,
        "Iterations",
        lambda **kwargs: SimpleNamespace(folder_paths={}),
    )
    monkeypatch.setattr(
        run_module,
        "InitialIterationStateAsset",
        lambda **kwargs: _MemoryAsset("initial-state"),
    )
    monkeypatch.setattr(
        run_module,
        "IterationTransportCostsAsset",
        lambda **kwargs: _MemoryAsset("initial-costs"),
    )
    monkeypatch.setattr(
        Run,
        "_build_iteration_state_assets",
        lambda self, **kwargs: [final_state],
    )

    population = _MemoryAsset("population")
    activity = _MemoryAsset("activity")
    mode = _MemoryAsset("mode")
    survey = _MemoryAsset("survey")
    survey_plan_assets = _MemoryAsset("survey-plan-assets")
    transport_costs = _MemoryAsset("transport-costs")
    parameters = GroupDayTripsParameters(
        run=GroupDayTripsRunParameters(n_iterations=1),
    )

    run = Run(
        population=population,
        transport_costs=transport_costs,
        activities=[activity],
        modes=[mode],
        surveys=[survey],
        survey_plan_assets=survey_plan_assets,
        parameters=parameters,
        is_weekday=True,
        scenario="default",
    )

    assert run.population is population
    assert run.activities == [activity]
    assert run.modes == [mode]
    assert run.surveys == [survey]
    assert run.survey_plan_assets is survey_plan_assets
    assert set(run.inputs) == {"version", "run_context_hash", "final_iteration_state"}

    graph = build_asset_graph(run)
    direct_parents = set(graph.predecessors(run))

    assert direct_parents == {final_state}


def test_initial_state_inputs_keep_full_modes_out_of_dependencies(tmp_path, monkeypatch):
    """Initial state hashes mode names, not full unresolved mode assets."""
    hidden_file_asset = _FinalStateAsset(tmp_path / "hidden")
    mode = _ModeAsset(name="walk/public_transport/walk", hidden_file_asset=hidden_file_asset)

    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(
        iteration_assets,
        "PopulationWeightedPlanSteps",
        lambda **kwargs: _MemoryAsset("population-weighted-plan-steps"),
    )
    monkeypatch.setattr(
        iteration_assets,
        "resolve_activity_parameters",
        lambda activities, iteration, scenario=None, sensitivity_case=None: {},
    )

    initial_state = iteration_assets.InitialIterationStateAsset(
        is_weekday=True,
        base_folder=tmp_path,
        population=_MemoryAsset("population"),
        survey_plan_assets=_MemoryAsset("survey-plan-assets"),
        activities=[
            SimpleNamespace(
                name="home",
                has_opportunities=False,
                opportunities=None,
                inputs={},
            )
        ],
        modes=[mode],
        parameters=GroupDayTripsParameters(),
        scenario="default",
        initial_transport_costs=_MemoryAsset("initial-transport-costs"),
    )

    graph = build_asset_graph(initial_state)

    assert "modes" not in initial_state.inputs
    assert initial_state.inputs["mode_values"] == ["walk/public_transport/walk", "stay_home"]
    assert hidden_file_asset not in graph
