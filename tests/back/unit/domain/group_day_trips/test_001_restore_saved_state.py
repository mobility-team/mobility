import random
from types import SimpleNamespace

import polars as pl
import pytest

from mobility.runtime.parameter_profiles import SimulationStep
from mobility.trips.group_day_trips.core.parameters import Parameters
from mobility.trips.group_day_trips.core.run import Run, RunState


class FakeIterations:
    def __init__(self, saved_state):
        self._saved_state = saved_state
        self.requested_iterations = []
        self.discarded_iteration = None

    def iteration(self, iteration):
        self.requested_iterations.append(iteration)
        if isinstance(self._saved_state, Exception):
            return SimpleNamespace(load_state=lambda: (_ for _ in ()).throw(self._saved_state))
        return SimpleNamespace(load_state=lambda: self._saved_state)

    def discard_future_iterations(self, *, iteration):
        self.discarded_iteration = iteration


class FakeCostsAggregator:
    def __init__(self, congestion_state):
        self._congestion_state = congestion_state
        self.load_calls = []
        self.get_calls = []
        self.resolve_calls = []

    def resolve_for_step(self, step):
        self.resolve_calls.append(step)
        return self

    def load_congestion_state(self, **kwargs):
        self.load_calls.append(kwargs)
        return self._congestion_state

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        return pl.DataFrame({"cost": [1.5]})


def make_run(*, congestion_state):
    run = object.__new__(Run)
    run.inputs_hash = "run-hash"
    run.is_weekday = True
    run.parameters = Parameters(n_iter_per_cost_update=3)
    run.costs_aggregator = FakeCostsAggregator(congestion_state=congestion_state)
    run.rng = random.Random(123)
    return run


def make_state():
    return RunState(
        chains_by_activity=pl.DataFrame(),
        chains=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        activity_dur=pl.DataFrame(),
        home_night_dur=pl.DataFrame(),
        stay_home_plan=pl.DataFrame(),
        opportunities=pl.DataFrame(),
        current_plans=pl.DataFrame({"plan_id": [0]}),
        remaining_opportunities=pl.DataFrame({"opportunity_id": [0]}),
        costs=pl.DataFrame({"cost": [0.0]}),
        congestion_state=None,
        start_iteration=1,
    )


def test_restore_saved_state_happy_path_restores_mutable_state():
    saved_rng = random.Random(999)
    saved_state = SimpleNamespace(
        current_plans=pl.DataFrame({"plan_id": [11, 12]}),
        remaining_opportunities=pl.DataFrame({"opportunity_id": [21, 22]}),
        rng_state=saved_rng.getstate(),
    )
    iterations = FakeIterations(saved_state=saved_state)
    run = make_run(congestion_state="congestion-state")
    state = make_state()

    run._restore_saved_state(
        iterations=iterations,
        state=state,
        resume_from_iteration=2,
    )

    assert iterations.requested_iterations == [2]
    assert iterations.discarded_iteration == 2
    assert state.current_plans.equals(saved_state.current_plans)
    assert state.remaining_opportunities.equals(saved_state.remaining_opportunities)
    assert state.congestion_state == "congestion-state"
    assert state.start_iteration == 3
    assert state.costs.equals(pl.DataFrame({"cost": [1.5]}))
    assert run.costs_aggregator.load_calls == [
        {
            "run_key": "run-hash",
            "is_weekday": True,
            "last_completed_iteration": 2,
            "cost_update_interval": 3,
        }
    ]
    assert run.costs_aggregator.get_calls == [
        {
            "congestion": True,
            "congestion_state": "congestion-state",
        }
    ]
    assert run.costs_aggregator.resolve_calls == [SimulationStep(iteration=3)]


def test_restore_saved_state_wraps_load_state_errors():
    iterations = FakeIterations(saved_state=ValueError("broken state"))
    run = make_run(congestion_state=None)
    state = make_state()

    with pytest.raises(RuntimeError, match="Failed to load saved GroupDayTrips iteration state"):
        run._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=2,
        )


def test_restore_saved_state_wraps_rng_restore_errors():
    saved_state = SimpleNamespace(
        current_plans=pl.DataFrame({"plan_id": [11]}),
        remaining_opportunities=pl.DataFrame({"opportunity_id": [21]}),
        rng_state=object(),
    )
    iterations = FakeIterations(saved_state=saved_state)
    run = make_run(congestion_state=None)
    state = make_state()

    with pytest.raises(RuntimeError, match="Failed to restore RNG state"):
        run._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=2,
        )
