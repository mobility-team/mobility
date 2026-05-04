import pytest
import polars as pl

from mobility.activities.activity import resolve_activity_arrival_time_rigidity
from mobility.activities.home import HomeActivity
from mobility.activities.other import OtherActivity
from mobility.activities.work.work import WorkActivity, WorkParameters
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


def _make_plan_steps(rows: dict[str, list]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "activity": pl.Utf8,
            "departure_time": pl.Float32,
            "arrival_time": pl.Float32,
            "next_departure_time": pl.Float32,
            "duration_per_pers": pl.Float32,
            "time": pl.Float64,
        },
    ).lazy()


def test_plan_schedule_updater_keeps_reference_schedule_when_disabled():
    """Check that turning the feature off leaves the survey schedule untouched.

    Even if the modeled travel time is different from the
    survey travel time, we should keep the original departure, arrival, and
    activity duration when the global timing-update switch is disabled.
    """
    possible_plan_steps = _make_plan_steps(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_seq_id": [1000],
            "mode_seq_id": [10000],
            "seq_step_index": [0],
            "activity": ["work"],
            "departure_time": [8.0],
            "arrival_time": [9.0],
            "next_departure_time": [17.0],
            "duration_per_pers": [8.0],
            "time": [2.0],
        }
    )

    result = PlanScheduleUpdater().update_plan_timings(
        possible_plan_steps,
        arrival_time_rigidity_by_activity={"work": 1.0},
        enabled=False,
    ).collect()

    assert result["departure_time"].to_list() == [8.0]
    assert result["arrival_time"].to_list() == [9.0]
    assert result["next_departure_time"].to_list() == [17.0]
    assert result["duration_per_pers"].to_list() == [8.0]


def test_plan_schedule_updater_reconciles_timings_from_modeled_travel_times():
    """Check that modeled travel times shift trips and activity time as intended.

    If the trip to an anchor activity like work becomes
    longer, we move departure earlier and keep arrival fixed. If a later trip to
    a flexible activity becomes longer, we keep departure fixed and arrive
    later. After that, the remaining activity time is recomputed from the new
    schedule, and tiny infeasible durations are clipped to a very small value
    instead of dropping the plan.
    """
    possible_plan_steps = _make_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "time_seq_id": [100, 100],
            "dest_seq_id": [1000, 1000],
            "mode_seq_id": [10000, 10000],
            "seq_step_index": [0, 1],
            "activity": ["work", "other"],
            "departure_time": [8.0, 17.0],
            "arrival_time": [9.0, 17.5],
            "next_departure_time": [17.0, 17.5],
            "duration_per_pers": [8.0, 0.0],
            "time": [2.0, 1.0],
        }
    )

    result = PlanScheduleUpdater().update_plan_timings(
        possible_plan_steps,
        arrival_time_rigidity_by_activity={"work": 1.0, "other": 0.0},
        enabled=True,
    ).collect()

    assert result["departure_time"].to_list() == [7.0, 17.0]
    assert result["arrival_time"].to_list() == [9.0, 18.0]
    assert result["next_departure_time"].to_list() == [17.0, 17.5]
    durations = result["duration_per_pers"].to_list()
    assert durations[0] == 8.0
    assert durations[1] == pytest.approx(PlanScheduleUpdater.MIN_ACTIVITY_DURATION_HOURS)


def test_plan_schedule_updater_keeps_last_activity_boundary_when_no_next_trip_exists():
    """Check that the last explicit activity keeps its own end boundary.

    In plain language: the final activity in a plan should not collapse to zero
    duration just because there is no following trip row. It should keep the
    original survey boundary stored on that last step, and only then apply the
    timing adjustment.
    """
    possible_plan_steps = _make_plan_steps(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_seq_id": [1000],
            "mode_seq_id": [10000],
            "seq_step_index": [0],
            "activity": ["other"],
            "departure_time": [17.0],
            "arrival_time": [17.5],
            "next_departure_time": [19.0],
            "duration_per_pers": [1.5],
            "time": [1.0],
        }
    )

    result = PlanScheduleUpdater().update_plan_timings(
        possible_plan_steps,
        arrival_time_rigidity_by_activity={"other": 0.0},
        enabled=True,
    ).collect()

    assert result["departure_time"].to_list() == [17.0]
    assert result["arrival_time"].to_list() == [18.0]
    assert result["next_departure_time"].to_list() == [19.0]
    assert result["duration_per_pers"].to_list() == [1.0]


def test_activity_arrival_time_rigidity_defaults_from_anchor_flag_and_allows_override():
    """Check the default and override rules for timing rigidity by activity.

    Anchor activities should, by default, behave like
    fixed-arrival activities, while non-anchor activities should default to more
    flexible timing. The test also checks that a user can override the default
    value for a specific activity.
    """
    activities = [
        HomeActivity(),
        OtherActivity(
            opportunities=pl.DataFrame({"to": [1], "n_opp": [1.0]}).to_pandas(),
        ),
        WorkActivity(parameters=WorkParameters(arrival_time_rigidity=0.25)),
    ]

    rigidity_by_activity = resolve_activity_arrival_time_rigidity(activities, iteration=1)

    assert rigidity_by_activity["home"] == 1.0
    assert rigidity_by_activity["other"] == 0.0
    assert rigidity_by_activity["work"] == 0.25
