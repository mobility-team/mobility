import random
from types import SimpleNamespace

import polars as pl
import pytest

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
    def __init__(self):
        self.get_calls = []
        self.resolve_calls = []

    def for_iteration(self, iteration):
        self.resolve_calls.append(iteration)
        return self

    def asset_for_iteration(self, run, iteration):
        self.resolve_calls.append(iteration)
        return self

    def get_costs_by_od(self, metrics):
        self.get_calls.append(metrics)
        return pl.DataFrame({"cost": [1.5]})


def make_run():
    run = object.__new__(Run)
    run.inputs_hash = "run-hash"
    run.is_weekday = True
    run.parameters = Parameters(n_iter_per_cost_update=3)
    run.transport_costs = FakeCostsAggregator()
    run.rng = random.Random(123)
    return run


def make_state():
    return RunState(
        survey_plans=pl.DataFrame(),
        survey_plan_steps=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        activity_dur=pl.DataFrame(),
        home_night_dur=pl.DataFrame(),
        stay_home_plan=pl.DataFrame(),
        opportunities=pl.DataFrame(),
        current_plans=pl.DataFrame({"plan_id": [0]}),
        candidate_plan_steps=pl.DataFrame({"plan_id": [0], "seq_step_index": [0]}),
        destination_saturation=pl.DataFrame({"opportunity_id": [0]}),
        costs=pl.DataFrame({"cost": [0.0]}),
        start_iteration=1,
        current_plan_steps=None,
    )


def test_restore_saved_state_happy_path_restores_mutable_state():
    saved_rng = random.Random(999)
    saved_state = SimpleNamespace(
        current_plans=pl.DataFrame({"plan_id": [11, 12]}),
        current_plan_steps=pl.DataFrame({"step_id": [31, 32]}),
        candidate_plan_steps=pl.DataFrame({"plan_id": [41, 42], "seq_step_index": [0, 1]}),
        destination_saturation=pl.DataFrame({"opportunity_id": [21, 22]}),
        rng_state=saved_rng.getstate(),
    )
    iterations = FakeIterations(saved_state=saved_state)
    run = make_run()
    state = make_state()

    run._restore_saved_state(
        iterations=iterations,
        state=state,
        resume_from_iteration=2,
    )

    assert iterations.requested_iterations == [2]
    assert iterations.discarded_iteration == 2
    assert state.current_plans.equals(saved_state.current_plans)
    assert state.current_plan_steps.equals(saved_state.current_plan_steps)
    assert state.candidate_plan_steps.equals(saved_state.candidate_plan_steps)
    assert state.destination_saturation.equals(saved_state.destination_saturation)
    assert state.start_iteration == 3
    assert state.costs.equals(pl.DataFrame({"cost": [1.5]}))
    assert run.transport_costs.get_calls == [["cost", "distance"]]
    assert run.transport_costs.resolve_calls == [3]


def test_restore_saved_state_wraps_load_state_errors():
    iterations = FakeIterations(saved_state=ValueError("broken state"))
    run = make_run()
    state = make_state()

    with pytest.raises(RuntimeError, match="Failed to load saved PopulationGroupDayTrips iteration state"):
        run._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=2,
        )


def test_restore_saved_state_wraps_incomplete_saved_state_errors():
    iterations = FakeIterations(
        saved_state=RuntimeError(
            "Saved PopulationGroupDayTrips iteration state is incomplete. Missing current_plan_steps."
        )
    )
    run = make_run()
    state = make_state()

    with pytest.raises(RuntimeError, match="Failed to load saved PopulationGroupDayTrips iteration state"):
        run._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=2,
        )


def test_restore_saved_state_wraps_rng_restore_errors():
    saved_state = SimpleNamespace(
        current_plans=pl.DataFrame({"plan_id": [11]}),
        current_plan_steps=pl.DataFrame({"step_id": [31]}),
        candidate_plan_steps=pl.DataFrame({"plan_id": [41], "seq_step_index": [0]}),
        destination_saturation=pl.DataFrame({"opportunity_id": [21]}),
        rng_state=object(),
    )
    iterations = FakeIterations(saved_state=saved_state)
    run = make_run()
    state = make_state()

    with pytest.raises(RuntimeError, match="Failed to restore RNG state"):
        run._restore_saved_state(
            iterations=iterations,
            state=state,
            resume_from_iteration=2,
        )
