import pytest
import polars as pl

from mobility.activities.activity import resolve_activity_time_rigidity
from mobility.activities.home import HomeActivity, HomeParameters
from mobility.activities.other import OtherActivity
from mobility.activities.work.work import WorkActivity, WorkParameters
from mobility.runtime.assets.input_hashing import hash_inputs
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


def _make_plan_steps(rows: dict[str, list]) -> pl.LazyFrame:
    rows = {
        **rows,
        "demand_subgroup_id": rows.get(
            "demand_subgroup_id",
            [0] * len(rows["demand_group_id"]),
        ),
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
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
        activity_time_rigidity_by_activity={"work": 1.0},
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
        activity_time_rigidity_by_activity={"work": 1.0, "other": 0.0},
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
        activity_time_rigidity_by_activity={"other": 0.0},
        enabled=True,
    ).collect()

    assert result["departure_time"].to_list() == [16.75]
    assert result["arrival_time"].to_list() == [17.75]
    assert result["next_departure_time"].to_list() == [19.0]
    assert result["duration_per_pers"].to_list() == [1.25]


def test_timing_update_keeps_each_plan_end_when_rows_are_interleaved():
    steps = _make_plan_steps({
        "demand_group_id": [1, 1, 1, 1, 1],
        "demand_subgroup_id": [0, 1, 0, 1, 0],
        "activity_seq_id": [10] * 5,
        "time_seq_id": [100] * 5,
        "dest_seq_id": [1000] * 5,
        "mode_seq_id": [1, 1, 1, 1, 2],
        "seq_step_index": [1, 0, 0, 1, 0],
        "activity": ["other"] * 5,
        "departure_time": [17.0, 9.0, 8.0, 18.0, 10.0],
        "arrival_time": [17.5, 9.5, 8.5, 18.5, 10.5],
        "next_departure_time": [20.0, 18.0, 17.0, 21.0, 22.0],
        "duration_per_pers": [2.5, 8.5, 8.5, 2.5, 11.5],
        "time": [0.5] * 5,
    })

    result = PlanScheduleUpdater().update_plan_timings(
        steps, activity_time_rigidity_by_activity={"other": 0.0}, enabled=True,
    ).collect()

    assert result["departure_time"].to_list() == [8.0, 17.0, 10.0, 9.0, 18.0]
    assert result["next_departure_time"].to_list() == [17.0, 20.0, 22.0, 18.0, 21.0]


def test_activity_activity_time_rigidity_defaults_and_overrides():
    """Home is flexible by default; other anchors and overrides retain their rules."""
    activities = [
        HomeActivity(),
        OtherActivity(
            opportunities=pl.DataFrame({"to": [1], "n_opp": [1.0]}).to_pandas(),
        ),
        WorkActivity(parameters=WorkParameters(activity_time_rigidity=0.25)),
    ]

    rigidity_by_activity = resolve_activity_time_rigidity(activities, iteration=1)

    assert rigidity_by_activity["home"] == 0.0
    assert rigidity_by_activity["other"] == 0.0
    assert rigidity_by_activity["work"] == 0.25

    assert resolve_activity_time_rigidity([WorkActivity()], iteration=1)["work"] == 1.0
    assert resolve_activity_time_rigidity(
        [HomeActivity(parameters=HomeParameters(activity_time_rigidity=1.0))], iteration=1,
    )["home"] == 1.0
    # Runs made with the previous unset default must not reuse the new result.
    assert hash_inputs({"home": HomeActivity()}) != hash_inputs({
        "home": HomeActivity(parameters=HomeParameters(activity_time_rigidity=None)),
    })


def test_longer_trip_home_keeps_departure_and_delays_arrival():
    """The default home setting leaves work at its planned departure time."""
    steps = _make_plan_steps({
        "demand_group_id": [1, 1],
        "activity_seq_id": [10, 10],
        "time_seq_id": [100, 100],
        "dest_seq_id": [1000, 1000],
        "mode_seq_id": [10000, 10000],
        "seq_step_index": [0, 1],
        "activity": ["work", "home"],
        "departure_time": [8.0, 17.0],
        "arrival_time": [9.0, 17.5],
        "next_departure_time": [17.0, 24.0],
        "duration_per_pers": [8.0, 6.5],
        "time": [1.0, 1.0],
    })
    rigidity = resolve_activity_time_rigidity([WorkActivity(), HomeActivity()], iteration=1)

    result = PlanScheduleUpdater().update_plan_timings(
        steps, activity_time_rigidity_by_activity=rigidity, enabled=True,
    ).collect()

    assert result["departure_time"].to_list() == [8.0, 17.0]
    assert result["arrival_time"].to_list() == [9.0, 18.0]
    assert result["duration_per_pers"].to_list() == [8.0, 6.0]


@pytest.mark.parametrize("origin,destination,departure_share", [
    (0.0, 0.0, 0.5), (1.0, 1.0, 0.5), (1.0, 0.0, 0.0),
    (0.0, 1.0, 1.0), (0.75, 0.25, 0.25),
])
@pytest.mark.parametrize("delta", [-0.25, 0.5])
def test_travel_time_change_is_shared_by_both_activities(origin, destination, departure_share, delta):
    steps = _make_plan_steps({
        "demand_group_id": [1, 1], "activity_seq_id": [1, 1], "time_seq_id": [1, 1],
        "dest_seq_id": [1, 1], "mode_seq_id": [1, 1], "seq_step_index": [0, 1],
        "activity": ["work", "studies"], "departure_time": [8.0, 12.0],
        "arrival_time": [9.0, 13.0], "next_departure_time": [12.0, 17.0],
        "duration_per_pers": [3.0, 4.0], "time": [1.0, 1.0 + delta],
    })
    result = PlanScheduleUpdater().update_plan_timings(
        steps, activity_time_rigidity_by_activity={"home": 0.0, "work": origin, "studies": destination},
        enabled=True,
    ).collect()
    assert result["departure_time"][1] == pytest.approx(12.0 - departure_share * delta)
    assert result["arrival_time"][1] == pytest.approx(13.0 + (1.0 - departure_share) * delta)
    assert result["duration_per_pers"][0] == pytest.approx(3.0 - departure_share * delta)
    assert result["duration_per_pers"][1] == pytest.approx(4.0 - (1.0 - departure_share) * delta)


def test_long_trip_between_rigid_activities_can_make_the_first_one_infeasible():
    steps = _make_plan_steps({
        "demand_group_id": [1, 1], "activity_seq_id": [1, 1], "time_seq_id": [1, 1],
        "dest_seq_id": [1, 1], "mode_seq_id": [1, 1], "seq_step_index": [0, 1],
        "activity": ["work", "studies"], "departure_time": [8.0, 12.0],
        "arrival_time": [9.0, 13.0], "next_departure_time": [12.0, 17.0],
        "duration_per_pers": [3.0, 4.0], "time": [1.0, 8.0],
    })
    result = PlanScheduleUpdater().update_plan_timings(
        steps, activity_time_rigidity_by_activity={"home": 0.0, "work": 1.0, "studies": 1.0},
        enabled=True,
    ).collect()
    assert result["next_departure_time"][0] == pytest.approx(8.5)
    assert result["arrival_time"][0] == pytest.approx(9.0)
    assert result["next_departure_time"][0] < result["arrival_time"][0]
