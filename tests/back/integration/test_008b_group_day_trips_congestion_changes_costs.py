import pytest
import polars as pl

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsPeriodParameters,
    GroupDayTripsRunParameters,
    PopulationGroupDayTrips,
)
from mobility.surveys.france import EMPMobilitySurvey


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008b_group_day_trips_congestion_changes_costs(test_data, gtfs_sources_folder):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    emp = EMPMobilitySurvey()
    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    baseline_car_mode = mobility.CarMode(transport_zones)
    baseline_walk_mode = mobility.WalkMode(transport_zones)
    baseline_bicycle_mode = mobility.BicycleMode(transport_zones)
    baseline_mode_registry = mobility.ModeRegistry(
        [baseline_car_mode, baseline_walk_mode, baseline_bicycle_mode]
    )
    baseline_public_transport_mode = mobility.PublicTransportMode(
        transport_zones,
        mode_registry=baseline_mode_registry,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            gtfs_reference_date="2026-01-01",
            gtfs_sources_folder=gtfs_sources_folder,
        ),
    )

    baseline = PopulationGroupDayTrips(
        population=pop,
        modes=[
            baseline_car_mode,
            baseline_walk_mode,
            baseline_bicycle_mode,
            baseline_public_transport_mode,
        ],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=pop)],
        surveys=[emp],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=1,
                n_iter_per_cost_update=0,
            ),
            periods=GroupDayTripsPeriodParameters(simulate_weekend=False),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                dest_prob_cutoff=0.9,
                cost_uncertainty_sd=1.0,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(
                k_mode_sequences=6,
                mode_sequence_search_parallel=False,
            ),
        ),
    )

    congested_car_mode = mobility.CarMode(
        transport_zones,
        congestion=True,
        congestion_flows_scaling_factor=1.0,
    )
    congested_walk_mode = mobility.WalkMode(transport_zones)
    congested_bicycle_mode = mobility.BicycleMode(transport_zones)
    congested_mode_registry = mobility.ModeRegistry(
        [congested_car_mode, congested_walk_mode, congested_bicycle_mode]
    )
    congested_public_transport_mode = mobility.PublicTransportMode(
        transport_zones,
        mode_registry=congested_mode_registry,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            gtfs_reference_date="2026-01-01",
            gtfs_sources_folder=gtfs_sources_folder,
        ),
    )

    congested = PopulationGroupDayTrips(
        population=pop,
        modes=[
            congested_car_mode,
            congested_walk_mode,
            congested_bicycle_mode,
            congested_public_transport_mode,
        ],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=pop)],
        surveys=[emp],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=2,
                n_iter_per_cost_update=1,
                seed=0,
            ),
            periods=GroupDayTripsPeriodParameters(simulate_weekend=False),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                dest_prob_cutoff=0.9,
                cost_uncertainty_sd=1.0,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(
                k_mode_sequences=6,
                mode_sequence_search_parallel=False,
            ),
        ),
    )

    baseline_result = baseline.run("weekday").get()
    congested_run = congested.run("weekday")
    congested_result = congested_run.get()

    baseline_plan_steps = baseline_result["plan_steps"].collect()
    congested_plan_steps = congested_result["plan_steps"].collect()
    baseline_costs = baseline_result["costs"].collect()
    congested_costs = congested_result["costs"].collect()
    road_flow_asset = congested_run.iteration_transport_cost_assets[1].congestion_flows.get()

    assert baseline_plan_steps.height > 0
    assert congested_plan_steps.height > 0
    assert baseline_costs.height > 0
    assert congested_costs.height > 0
    assert road_flow_asset is not None
    assert road_flow_asset.cache_path.exists()

    joined_costs = baseline_costs.join(
        congested_costs.select(["from", "to", "mode", "cost"]).rename({"cost": "cost_congested"}),
        on=["from", "to", "mode"],
        how="inner",
    )

    assert joined_costs.height > 0
    assert (
        joined_costs
        .filter((pl.col("cost") - pl.col("cost_congested")).abs() > 1e-9)
        .height
        > 0
    )
