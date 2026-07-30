import os
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from mobility.activities.activity import ActivityParameters
from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsPlanUpdateParameters,
)
from mobility.trips.group_day_trips.plans.destination_sequences import (
    DestinationSequences,
)
from mobility.trips.group_day_trips.plans.destination_plan_search import (
    RAW_CONTEXT_COLUMNS,
)


SNAPSHOT_FILES = {
    "activity_sequences": (
        "activity-sequences/"
        "31568cd267580e15713a8458b22b0687-activity_sequences_5.parquet"
    ),
    "transport_costs": (
        "iteration-transport-costs/"
        "0d1f446fa31903f5e734195585678c76-transport_costs_5.parquet"
    ),
    "destination_saturation": (
        "iteration-state-cache/"
        "50489de7b4b351be3778dad7894caef0-destination_saturation_5.parquet"
    ),
    "activity_durations": (
        "iteration-state-cache/"
        "50489de7b4b351be3778dad7894caef0-activity_dur_5.parquet"
    ),
    "demand_groups": (
        "iteration-state-cache/"
        "50489de7b4b351be3778dad7894caef0-demand_groups_5.parquet"
    ),
}


def _grand_geneve_snapshot() -> dict[str, Path]:
    project_folder = os.environ.get("MOBILITY_GRAND_GENEVE_PROJECT_FOLDER")
    if project_folder is None:
        pytest.skip(
            "Set MOBILITY_GRAND_GENEVE_PROJECT_FOLDER to run the real-data "
            "destination-plan integration test."
        )

    group_day_trips_folder = Path(project_folder) / "group_day_trips"
    files = {
        name: group_day_trips_folder / relative_path
        for name, relative_path in SNAPSHOT_FILES.items()
    }
    missing = [path for path in files.values() if not path.exists()]
    if missing:
        pytest.fail(
            "The Grand Geneve iteration-5 snapshot is incomplete: "
            + ", ".join(str(path) for path in missing)
        )
    return files


def test_grand_geneve_destination_plan_search_returns_complete_chains(
    tmp_path,
):
    """Run Mobility's production adapter on a real Grand Geneve cache sample."""
    files = _grand_geneve_snapshot()
    activity_sequences = pl.read_parquet(files["activity_sequences"])
    selected_contexts = (
        activity_sequences
        .group_by(RAW_CONTEXT_COLUMNS)
        .agg(context_steps=pl.len())
        .sort(
            ["context_steps"] + RAW_CONTEXT_COLUMNS,
            descending=[True] + [False] * len(RAW_CONTEXT_COLUMNS),
        )
        .head(25)
        .select(RAW_CONTEXT_COLUMNS)
    )
    activity_sequences = activity_sequences.join(
        selected_contexts,
        on=RAW_CONTEXT_COLUMNS,
        how="semi",
    )
    demand_groups = pl.read_parquet(files["demand_groups"])

    # The Grand Geneve home-zone table covers the modeled zone countries used
    # to apply the French and Swiss destination-value coefficients.
    zone_countries = (
        demand_groups
        .select(
            pl.col("home_zone_id").alias("transport_zone_id"),
            pl.col("country").cast(pl.String),
        )
        .unique()
        .to_pandas()
    )
    transport_zones = SimpleNamespace(get=lambda: zone_countries)
    activities = [
        SimpleNamespace(name="home", is_anchor=True),
        SimpleNamespace(name="work", is_anchor=True),
        SimpleNamespace(name="studies", is_anchor=True),
        SimpleNamespace(name="shopping", is_anchor=False),
        SimpleNamespace(name="leisure", is_anchor=False),
        SimpleNamespace(name="other", is_anchor=False),
    ]
    value_of_time = {
        "home": 3.5,
        "work": 2.0,
        "studies": 3.0,
        "shopping": 8.0,
        "leisure": 4.0,
        "other": 5.0,
    }
    resolved_activity_parameters = {
        activity.name: ActivityParameters(
            value_of_time=value_of_time[activity.name],
            country_value_coefficients=(
                {"fr": 1.0, "ch": 1.5}
                if activity.name == "work"
                else None
            ),
            arrival_time_rigidity=None,
        )
        for activity in activities
    }

    mode_costs = pl.read_parquet(files["transport_costs"])
    parameters = GroupDayTripsParameters(
        destination_sequences=GroupDayTripsDestinationSequenceParameters(
            use_destination_plan_search=True,
            k_destination_sequences=3,
        ),
        plan_update=GroupDayTripsPlanUpdateParameters(
            update_plan_timings_from_modeled_travel_times=True,
            use_destination_shadow_prices=True,
            min_activity_time_constant=2.0,
            transition_logit_scale=0.25,
        ),
    )
    transport_costs = SimpleNamespace(
        get_costs_by_od_and_mode=lambda metrics: mode_costs
    )
    # Build only the runtime side of the asset. The real cached dependencies
    # are plain test doubles here and are not valid content-addressed assets.
    destination_sequences = object.__new__(DestinationSequences)
    destination_sequences.iteration = 5
    destination_sequences.activities = activities
    destination_sequences.resolved_activity_parameters = (
        resolved_activity_parameters
    )
    destination_sequences.transport_zones = transport_zones
    destination_sequences.transport_costs = transport_costs
    destination_sequences.destination_saturation = pl.read_parquet(
        files["destination_saturation"]
    )
    destination_sequences.activity_durations = pl.read_parquet(
        files["activity_durations"]
    )
    destination_sequences.previous_destination_sequences = None
    destination_sequences.cache_path = {
        "index": tmp_path / "destination_sequence_index_5.parquet"
    }
    result = destination_sequences.run(
        activities=activities,
        transport_zones=transport_zones,
        destination_saturation=destination_sequences.destination_saturation,
        activity_sequences=activity_sequences,
        demand_groups=demand_groups,
        costs=pl.DataFrame(),
        parameters=parameters,
        seed=17,
    )

    plan_columns = RAW_CONTEXT_COLUMNS + ["dest_seq_id"]
    expected_counts = (
        activity_sequences
        .group_by(RAW_CONTEXT_COLUMNS)
        .agg(expected_step_count=pl.len())
    )
    initial_locations = demand_groups.select(
        RAW_CONTEXT_COLUMNS[:2]
        + [pl.col("home_zone_id").alias("initial_zone")]
    )
    plan_summary = (
        result
        .sort(plan_columns + ["seq_step_index"])
        .group_by(plan_columns)
        .agg(
            actual_step_count=pl.len(),
            first_origin=pl.col("from").first(),
            terminal_destination=pl.col("to").last(),
        )
        .join(expected_counts, on=RAW_CONTEXT_COLUMNS)
        .join(initial_locations, on=RAW_CONTEXT_COLUMNS[:2])
    )
    returned_context_count = result.select(RAW_CONTEXT_COLUMNS).unique().height
    assert 0 < returned_context_count <= selected_contexts.height
    assert plan_summary["expected_step_count"].max() >= 6
    assert (
        plan_summary
        .group_by(RAW_CONTEXT_COLUMNS)
        .agg(pl.len().alias("destination_plans"))
        ["destination_plans"]
        .max()
        <= 3
    )
    assert (
        plan_summary["actual_step_count"]
        == plan_summary["expected_step_count"]
    ).all()
    assert (
        plan_summary["first_origin"] == plan_summary["initial_zone"]
    ).all()
    assert (
        plan_summary["terminal_destination"] == plan_summary["initial_zone"]
    ).all()

    ordered = (
        result
        .sort(plan_columns + ["seq_step_index"])
        .with_columns(
            previous_destination=pl.col("to").shift(1).over(plan_columns),
            first_step=(
                pl.col("seq_step_index")
                == pl.col("seq_step_index").min().over(plan_columns)
            ),
        )
    )
    discontinuities = ordered.filter(
        (~pl.col("first_step"))
        & (pl.col("from") != pl.col("previous_destination"))
    )
    assert discontinuities.height == 0
