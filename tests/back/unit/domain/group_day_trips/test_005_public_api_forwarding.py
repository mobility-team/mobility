from pathlib import Path
from types import SimpleNamespace

from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, GroupDayTrips
from mobility.trips.group_day_trips.core import group_day_trips as group_day_trips_module


class _FakeRun:
    def __init__(self, *, parameters, is_weekday, enabled, **kwargs):
        self.parameters = parameters
        self.is_weekday = is_weekday
        self.enabled = enabled
        base = Path("cache") / ("weekday" if is_weekday else "weekend")
        self.cache_path = {
            "plan_steps": base / "plan_steps.parquet",
            "opportunities": base / "opportunities.parquet",
            "costs": base / "costs.parquet",
            "chains": base / "chains.parquet",
            "transitions": base / "transitions.parquet",
            "demand_groups": base / "demand_groups.parquet",
        }


def test_group_day_trips_wrapper_forwards_new_parameters(monkeypatch):
    monkeypatch.setattr(GroupDayTrips, "_validate_modes", lambda self, modes: None)
    monkeypatch.setattr(GroupDayTrips, "_validate_activities", lambda self, activities: None)
    monkeypatch.setattr(GroupDayTrips, "_validate_surveys", lambda self, surveys: None)
    monkeypatch.setattr(group_day_trips_module, "Run", _FakeRun)
    monkeypatch.setattr(group_day_trips_module, "TransportCosts", lambda modes: SimpleNamespace(modes=modes))

    wrapper = GroupDayTrips(
        population=object(),
        modes=[object()],
        activities=[object()],
        surveys=[object()],
        n_iterations=4,
        k_activity_sequences=5,
        k_destination_sequences=6,
        n_warmup_iterations=2,
        max_inactive_age=3,
        transition_revision_probability=0.4,
        transition_logit_scale=0.75,
        enable_transition_distance_model=True,
        transition_distance_threshold=8.0,
        transition_distance_friction=1.5,
        plan_embedding_dimension_weights=[1.0, 2.0, 3.0],
        behavior_change_phases=[
            BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING),
        ],
        simulate_weekend=False,
    )

    parameters = wrapper.weekday_run.parameters

    assert parameters.n_iterations == 4
    assert parameters.k_activity_sequences == 5
    assert parameters.k_destination_sequences == 6
    assert parameters.n_warmup_iterations == 2
    assert parameters.max_inactive_age == 3
    assert parameters.transition_revision_probability == 0.4
    assert parameters.transition_logit_scale == 0.75
    assert parameters.enable_transition_distance_model is True
    assert parameters.transition_distance_threshold == 8.0
    assert parameters.transition_distance_friction == 1.5
    assert parameters.plan_embedding_dimension_weights == [1.0, 2.0, 3.0]
    assert parameters.behavior_change_phases == [
        BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING)
    ]
    assert wrapper.weekend_run.enabled is False
