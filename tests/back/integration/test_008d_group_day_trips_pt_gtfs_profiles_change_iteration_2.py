import os
import pathlib

import geopandas as gpd
import polars as pl
import pytest
import mobility
from mobility.activities import Home, Other, Work
from mobility.trips.group_day_trips import GroupDayTrips, Parameters
from mobility.surveys.france import EMPMobilitySurvey


def _select_subway_endpoints(transport_zones: mobility.TransportZones) -> tuple[dict, dict, str, str]:
    transport_zones_df = transport_zones.get()
    center_local_admin_unit_id = str(transport_zones.inputs["parameters"].local_admin_unit_id)

    center_lau = transport_zones_df.loc[
        transport_zones_df["local_admin_unit_id"] == center_local_admin_unit_id
    ].iloc[[0]].copy()
    center_lau_id = str(center_lau.iloc[0]["local_admin_unit_id"])
    other_laus = transport_zones_df.loc[transport_zones_df["local_admin_unit_id"] != center_lau_id].copy()
    assert not other_laus.empty, "Need at least two local admin units to build a synthetic GTFS line."

    center_centroid = center_lau.geometry.iloc[0].centroid
    other_laus_projected = other_laus.copy()
    other_laus_projected["distance_to_center"] = other_laus_projected.geometry.centroid.distance(center_centroid)
    target_lau = other_laus.loc[other_laus_projected.sort_values("distance_to_center", ascending=True).index[:1]].copy()

    return center_lau, target_lau, center_lau_id, str(target_lau.iloc[0]["local_admin_unit_id"])


def _build_gtfs_zip(
    transport_zones: mobility.TransportZones,
    *,
    output_name: str,
    segment_travel_time: float,
) -> tuple[str, str, str]:
    center_lau, target_lau, center_lau_id, target_lau_id = _select_subway_endpoints(transport_zones)

    centroids = gpd.GeoDataFrame(
        geometry=[
            center_lau.geometry.centroid.iloc[0],
            target_lau.geometry.centroid.iloc[0],
        ],
        crs=3035,
    ).to_crs(4326)
    center_centroid = centroids.geometry.iloc[0]
    target_centroid = centroids.geometry.iloc[1]
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
                name="Center",
            ),
            "target": mobility.GTFSStopSpec(
                lon=float(target_centroid.x),
                lat=float(target_centroid.y),
                name="Target",
            ),
        },
        lines=[
            mobility.GTFSLineSpec(
                stop_ids=["center", "target"],
                segment_travel_times=[segment_travel_time],
                start_time=6.0 * 3600.0,
                end_time=9.0 * 3600.0,
                period=300.0,
                bidirectional=True,
            )
        ],
    )

    output_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / output_name
    gtfs_zip = mobility.build_gtfs_zip(feed, output_path)

    return (
        str(gtfs_zip),
        center_lau_id,
        target_lau_id,
    )


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
    slow_gtfs_zip, center_lau_id, target_lau_id = _build_gtfs_zip(
        transport_zones,
        output_name="test-slow-subway-line.zip",
        segment_travel_time=900.0,
    )
    fast_gtfs_zip, _, _ = _build_gtfs_zip(
        transport_zones,
        output_name="test-fast-subway-line.zip",
        segment_travel_time=60.0,
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
        modes=_build_modes(
            transport_zones,
            additional_gtfs_files=mobility.ListParameterProfile(
                points={
                    1: [slow_gtfs_zip],
                    2: [slow_gtfs_zip],
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

    dynamic = GroupDayTrips(
        population=pop,
        modes=_build_modes(
            transport_zones,
            additional_gtfs_files=mobility.ListParameterProfile(
                points={
                    1: [slow_gtfs_zip],
                    2: [fast_gtfs_zip],
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
    zones = pl.from_pandas(
        transport_zones.get()[["transport_zone_id", "local_admin_unit_id"]]
    ).with_columns(
        transport_zone_id=pl.col("transport_zone_id").cast(pl.Int32),
        local_admin_unit_id=pl.col("local_admin_unit_id").cast(pl.String),
    )

    pt_mode_name = "walk/public_transport/walk"
    def aggregate_lau_pair(costs: pl.DataFrame) -> pl.DataFrame:
        return (
            costs.filter(pl.col("mode") == pt_mode_name)
            .join(
                zones.rename(
                    {
                        "transport_zone_id": "from",
                        "local_admin_unit_id": "from_local_admin_unit_id",
                    }
                ),
                on="from",
                how="left",
            )
            .join(
                zones.rename(
                    {
                        "transport_zone_id": "to",
                        "local_admin_unit_id": "to_local_admin_unit_id",
                    }
                ),
                on="to",
                how="left",
            )
            .filter(
                (pl.col("from_local_admin_unit_id") == center_lau_id)
                & (pl.col("to_local_admin_unit_id") == target_lau_id)
            )
            .group_by(["from_local_admin_unit_id", "to_local_admin_unit_id"])
            .agg(
                min_cost=pl.col("cost").min(),
                min_time=pl.col("time").min(),
                pair_count=pl.len(),
            )
        )

    static_od = aggregate_lau_pair(static_costs)
    dynamic_od = aggregate_lau_pair(dynamic_costs)

    assert static_costs.height > 0
    assert dynamic_costs.height > 0
    assert dynamic_transitions.height > 0
    assert static_od.height == 1
    assert dynamic_od.height == 1

    static_cost = static_od["min_cost"].item()
    dynamic_cost = dynamic_od["min_cost"].item()
    static_time = static_od["min_time"].item()
    dynamic_time = dynamic_od["min_time"].item()

    assert dynamic_cost < static_cost
    assert dynamic_time < static_time
