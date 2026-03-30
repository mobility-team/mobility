import os
import pathlib

import polars as pl
import pytest

import mobility
from mobility.activities import Home, Other, Work
from mobility.trips.group_day_trips import GroupDayTrips, Parameters
from mobility.surveys.france import EMPMobilitySurvey


def _select_subway_endpoints(transport_zones: mobility.TransportZones) -> tuple[dict, dict]:
    zones = transport_zones.get().to_crs(4326)
    center_local_admin_unit_id = transport_zones.inputs["parameters"].local_admin_unit_id

    center_candidates = zones.loc[zones["local_admin_unit_id"] == center_local_admin_unit_id].copy()
    if center_candidates.empty:
        raise ValueError("Could not find the central local admin unit in transport zones.")

    center_zone = center_candidates.iloc[0]
    other_zones = zones.loc[zones["transport_zone_id"] != center_zone["transport_zone_id"]].copy()
    if other_zones.empty:
        raise ValueError("Need at least two transport zones to build a synthetic GTFS line.")

    other_zones["distance_to_center"] = other_zones.geometry.distance(center_zone.geometry)
    target_zone = other_zones.sort_values("distance_to_center", ascending=False).iloc[0]

    return center_zone, target_zone


def _build_ultra_fast_gtfs_zip(
    transport_zones: mobility.TransportZones,
    *,
    output_name: str,
) -> tuple[str, int, int]:
    center_zone, target_zone = _select_subway_endpoints(transport_zones)

    center_centroid = center_zone.geometry.centroid
    target_centroid = target_zone.geometry.centroid

    feed = mobility.GTFSFeedSpec(
        agency_id="test_subway",
        agency_name="Test Subway",
        route_id="test_subway_route",
        route_short_name="TS1",
        route_type="train",
        service_id="test_subway_service",
        stops={
            "center": mobility.GTFSStopSpec(
                lon=float(center_centroid.x),
                lat=float(center_centroid.y),
                name=f"Center {center_zone['transport_zone_id']}",
            ),
            "target": mobility.GTFSStopSpec(
                lon=float(target_centroid.x),
                lat=float(target_centroid.y),
                name=f"Target {target_zone['transport_zone_id']}",
            ),
        },
        lines=[
            mobility.GTFSLineSpec(
                stop_ids=["center", "target"],
                segment_travel_times=[60.0],
                start_time=6.0 * 3600.0,
                end_time=9.0 * 3600.0,
                period=300.0,
                bidirectional=True,
            )
        ],
    )

    output_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / output_name
    gtfs_zip = mobility.build_gtfs_zip(feed, output_path)

    return str(gtfs_zip), int(center_zone["transport_zone_id"]), int(target_zone["transport_zone_id"])


def _build_modes(transport_zones: mobility.TransportZones, additional_gtfs_files):
    car_mode = mobility.Car(transport_zones)
    walk_mode = mobility.Walk(transport_zones)
    bicycle_mode = mobility.Bicycle(transport_zones)
    mode_registry = mobility.ModeRegistry([car_mode, walk_mode, bicycle_mode])
    public_transport_mode = mobility.PublicTransport(
        transport_zones,
        mode_registry=mode_registry,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            additional_gtfs_files=additional_gtfs_files,
            max_traveltime=10.0,
            max_perceived_time=10.0,
        ),
    )
    return [car_mode, walk_mode, bicycle_mode, public_transport_mode]


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008d_group_day_trips_pt_gtfs_profiles_change_iteration_2(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    gtfs_zip, center_zone_id, target_zone_id = _build_ultra_fast_gtfs_zip(
        transport_zones,
        output_name="test-fast-subway-line.zip",
    )

    emp = EMPMobilitySurvey()
    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    common_parameters = Parameters(
        n_iterations=2,
        n_iter_per_cost_update=0,
        alpha=0.01,
        dest_prob_cutoff=0.9,
        k_mode_sequences=6,
        cost_uncertainty_sd=1.0,
        mode_sequence_search_parallel=False,
        simulate_weekend=False,
        seed=0,
    )

    static = GroupDayTrips(
        population=pop,
        modes=_build_modes(transport_zones, additional_gtfs_files=[]),
        activities=[
            Home(),
            Work(value_of_time=5.0),
            Other(population=pop),
        ],
        surveys=[emp],
        parameters=common_parameters,
    )

    dynamic = GroupDayTrips(
        population=pop,
        modes=_build_modes(
            transport_zones,
            additional_gtfs_files=mobility.ListParameterProfile(
                points={
                    1: [],
                    2: [gtfs_zip],
                }
            ),
        ),
        activities=[
            Home(),
            Work(value_of_time=5.0),
            Other(population=pop),
        ],
        surveys=[emp],
        parameters=common_parameters,
    )

    static_result = static.get()
    dynamic_result = dynamic.get()

    static_costs = static_result["weekday_costs"].collect()
    dynamic_costs = dynamic_result["weekday_costs"].collect()
    dynamic_transitions = dynamic_result["weekday_transitions"].collect()

    pt_mode_name = "walk/public_transport/walk"
    od_filter = (
        (pl.col("from") == center_zone_id)
        & (pl.col("to") == target_zone_id)
        & (pl.col("mode") == pt_mode_name)
    )

    static_od = static_costs.filter(od_filter)
    dynamic_od = dynamic_costs.filter(od_filter)

    assert static_costs.height > 0
    assert dynamic_costs.height > 0
    assert dynamic_transitions.height > 0
    assert static_od.height == 1
    assert dynamic_od.height == 1

    static_cost = static_od["cost"].item()
    dynamic_cost = dynamic_od["cost"].item()
    static_time = static_od["time"].item()
    dynamic_time = dynamic_od["time"].item()

    assert dynamic_cost < static_cost
    assert dynamic_time < static_time
