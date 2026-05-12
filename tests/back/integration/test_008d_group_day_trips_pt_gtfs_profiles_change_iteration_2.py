import os
import pathlib

import geopandas as gpd
import polars as pl
import pytest
import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.trips.group_day_trips import PopulationGroupDayTrips, Parameters
from mobility.surveys.france import EMPMobilitySurvey
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter


def _select_subway_endpoints(transport_zones: mobility.TransportZones) -> tuple[dict, dict]:
    transport_zones_df = transport_zones.get()[["transport_zone_id", "local_admin_unit_id", "geometry"]].copy()
    center_local_admin_unit_id = str(transport_zones.inputs["parameters"].local_admin_unit_id)

    center_zones = transport_zones_df.loc[
        transport_zones_df["local_admin_unit_id"] == center_local_admin_unit_id
    ].copy()
    assert not center_zones.empty, "Need at least one transport zone in the center local admin unit."

    other_zones = transport_zones_df.loc[
        transport_zones_df["local_admin_unit_id"] != center_local_admin_unit_id
    ].copy()
    assert not other_zones.empty, "Need at least two local admin units to build a synthetic GTFS line."

    center_centroids = center_zones.geometry.centroid
    other_centroids = other_zones.geometry.centroid

    closest_pair = None
    closest_distance = None
    for center_idx, center_centroid in center_centroids.items():
        distances = other_centroids.distance(center_centroid)
        target_idx = distances.idxmin()
        target_distance = float(distances.loc[target_idx])
        if closest_distance is None or target_distance < closest_distance:
            closest_pair = (center_idx, target_idx)
            closest_distance = target_distance

    assert closest_pair is not None, "Need one center/target transport-zone pair to build a synthetic GTFS line."

    center_zone = center_zones.loc[[closest_pair[0]]].copy()
    target_zone = other_zones.loc[[closest_pair[1]]].copy()

    return center_zone, target_zone


def _build_gtfs_zip(
    transport_zones: mobility.TransportZones,
    *,
    output_name: str,
    segment_travel_time: float,
) -> str:
    center_zone, target_zone = _select_subway_endpoints(transport_zones)

    centroids = gpd.GeoDataFrame(
        geometry=[
            center_zone.geometry.centroid.iloc[0],
            target_zone.geometry.centroid.iloc[0],
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

    return str(gtfs_zip)


def _build_modes(transport_zones: mobility.TransportZones, additional_gtfs_files):
    car_mode = mobility.CarMode(transport_zones)
    walk_mode = mobility.WalkMode(transport_zones)
    bicycle_mode = mobility.BicycleMode(transport_zones)
    mode_registry = mobility.ModeRegistry([car_mode, walk_mode, bicycle_mode])
    public_transport_mode = mobility.PublicTransportMode(
        transport_zones,
        mode_registry=mode_registry,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            additional_gtfs_files=additional_gtfs_files,
            max_traveltime=10.0,
            max_perceived_time=10.0,
        ),
    )
    return [car_mode, walk_mode, bicycle_mode, public_transport_mode]


def _read_cppr_graph_edges(graph_path: str | pathlib.Path, output_path: pathlib.Path) -> pl.DataFrame:
    script = RScriptRunner(pathlib.Path(__file__).with_name("read_cppr_graph_edges.R"))
    script.run(args=[str(graph_path), str(output_path)])
    return pl.read_parquet(output_path)


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008d_group_day_trips_pt_intermodal_travel_times_change_with_gtfs_profiles(
    test_data,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(GTFSRouter, "get_gtfs_files", lambda self, stops: [])

    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    slow_gtfs_zip = _build_gtfs_zip(
        transport_zones,
        output_name="test-slow-subway-line.zip",
        segment_travel_time=900.0,
    )
    fast_gtfs_zip = _build_gtfs_zip(
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
        dest_prob_cutoff=0.9,
        k_mode_sequences=6,
        cost_uncertainty_sd=1.0,
        mode_sequence_search_parallel=False,
        persist_iteration_artifacts=True,
        save_transition_events=True,
        simulate_weekend=False,
        seed=0,
    )

    static = PopulationGroupDayTrips(
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
            HomeActivity(),
            WorkActivity(value_of_time=5.0),
            OtherActivity(population=pop),
        ],
        surveys=[emp],
        parameters=common_parameters,
    )

    dynamic = PopulationGroupDayTrips(
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
            HomeActivity(),
            WorkActivity(value_of_time=5.0),
            OtherActivity(population=pop),
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
    static_pt_mode = next(
        mode for mode in static.weekday_run.transport_costs.modes
        if mode.inputs["parameters"].name == pt_mode_name
    ).for_iteration(2)
    dynamic_pt_mode = next(
        mode for mode in dynamic.weekday_run.transport_costs.modes
        if mode.inputs["parameters"].name == pt_mode_name
    ).for_iteration(2)

    static_intermodal_graph = static_pt_mode.inputs["travel_costs"].inputs["intermodal_graph"].get()
    dynamic_intermodal_graph = dynamic_pt_mode.inputs["travel_costs"].inputs["intermodal_graph"].get()

    static_edges = _read_cppr_graph_edges(static_intermodal_graph, tmp_path / "static-edges.parquet")
    dynamic_edges = _read_cppr_graph_edges(dynamic_intermodal_graph, tmp_path / "dynamic-edges.parquet")

    edge_deltas = (
        static_edges.join(
            dynamic_edges,
            on=["from_vertex", "to_vertex"],
            how="inner",
            suffix="_dynamic",
        )
        .with_columns(
            dist_delta=(pl.col("dist_dynamic") - pl.col("dist")).abs(),
            real_time_delta=(pl.col("real_time_dynamic") - pl.col("real_time")).abs(),
        )
    )

    assert static_costs.height > 0
    assert dynamic_costs.height > 0
    assert dynamic_transitions.height > 0
    assert static_edges.height > 0
    assert dynamic_edges.height > 0
    assert edge_deltas.height > 0
    assert edge_deltas.filter(
        (pl.col("dist_dynamic") < pl.col("dist"))
        & (pl.col("real_time_dynamic") < pl.col("real_time"))
        & ((pl.col("dist_delta") > 0.0) | (pl.col("real_time_delta") > 0.0))
    ).height > 0
