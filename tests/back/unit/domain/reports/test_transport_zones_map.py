import geopandas as gpd
import pandas as pd
import polars as pl
import pytest
from shapely.geometry import box

import mobility.reports.transport_zones as transport_zones_module
from mobility.reports.transport_zones import (
    TransportZoneMaps,
    _add_head_tail_classes,
    _align_flow_zone_ids,
    _align_metric_zone_ids,
    _continuous_zone_colors,
    _filter_labels_to_zones,
    _format_significant,
    _metric_flow_values_frame,
    _metric_value_zones,
    _metric_values_frame,
    _prepare_metric_flow_lines,
    _select_city_labels,
    opportunity_density_map,
    population_density_map,
    transport_zones_map,
)


def _zones() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "transport_zone_id": [1, 2, 3],
            "local_admin_unit_id": ["city-a", "city-b", "city-c"],
            "local_admin_unit_name": ["Alpha", "Beta", "Gamma"],
            "is_inner_zone": [True, False, True],
        },
        geometry=[
            box(0, 0, 1000, 1000),
            box(3000, 0, 4000, 1000),
            box(7000, 0, 8000, 1000),
        ],
        crs="EPSG:3035",
    )


def _plotted_zone_ids(fig):
    zone_ids = set()
    for trace in fig.data:
        locations = getattr(trace, "locations", None)
        if locations is not None:
            zone_ids.update(int(location) for location in locations)
    return zone_ids


def test_transport_zones_map_accepts_geodataframe():
    fig = transport_zones_map(_zones(), labels=False)

    assert fig.layout.title.text is None
    assert len(fig.data) == 2
    assert fig.layout.geo.projection.type == "azimuthal equal area"


def test_transport_zone_maps_keeps_internal_zones_in_epsg_3035():
    zones = _zones().to_crs("EPSG:4326")

    maps = TransportZoneMaps(zones)

    assert maps._map_data.zones.crs.to_epsg() == 3035
    assert maps._map_data.plotly_zones.crs.to_epsg() == 4326


def test_transport_zone_maps_assumes_missing_crs_is_epsg_3035():
    zones = _zones().set_crs(None, allow_override=True)

    maps = TransportZoneMaps(zones)

    assert maps._map_data.zones.crs.to_epsg() == 3035


def test_transport_zones_map_uses_title_when_provided():
    fig = transport_zones_map(_zones(), labels=False, title="Transport zones")

    assert fig.layout.title.text == "Transport zones"


def test_transport_zones_map_adds_labels_by_default():
    fig = transport_zones_map(_zones())

    assert len(fig.data) == 5
    assert all(
        text_lat > point_lat
        for text_lat, point_lat in zip(fig.data[-1].lat, fig.data[-3].lat)
    )
    assert all(
        text_lon > point_lon
        for text_lon, point_lon in zip(fig.data[-1].lon, fig.data[-3].lon)
    )


def test_transport_zones_map_accepts_asset_like_input():
    class TransportZonesLike:
        def get(self):
            return _zones()

    fig = transport_zones_map(TransportZonesLike(), labels=False)

    assert fig.layout.title.text is None


def test_transport_zone_maps_reuses_transport_zones(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class TransportZonesLike:
        calls = 0

        def get(self):
            self.calls += 1
            return _zones()

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    transport_zones = TransportZonesLike()
    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame({"transport_zone_id": [1], "weight": [100.0]}),
    )

    maps = TransportZoneMaps(transport_zones)
    maps.transport_zones(labels=False)
    maps.population_density(PopulationLike(), labels=False)

    assert transport_zones.calls == 1


def test_transport_zones_map_uses_study_area_names_when_zones_only_have_ids():
    class StudyAreaLike:
        def get(self):
            return pd.DataFrame(
                {
                    "local_admin_unit_id": ["city-a", "city-b", "city-c"],
                    "local_admin_unit_name": ["Alpha", "Beta", "Gamma"],
                }
            )

    class TransportZonesLike:
        study_area = StudyAreaLike()

        def get(self):
            return _zones().drop(columns="local_admin_unit_name")

    fig = transport_zones_map(TransportZonesLike(), labels=False)

    assert "Alpha" in fig.data[0].hovertext
    assert "city-a" not in fig.data[0].hovertext


def test_transport_zones_map_requires_inner_zone_column():
    zones = _zones().drop(columns="is_inner_zone")

    with pytest.raises(ValueError, match="is_inner_zone"):
        transport_zones_map(zones, labels=False)


def test_transport_zones_map_writes_svg_to_project_folder_when_asked(monkeypatch, tmp_path):
    class TransportZonesLike:
        inputs_hash = "abc123"

        def get(self):
            return _zones()

    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    transport_zones_map(TransportZonesLike(), save_to_file=True, labels=False)

    assert (tmp_path / "abc123-transport-zones-map.svg").exists()


def test_transport_zones_map_passes_title_to_static_export(monkeypatch, tmp_path):
    class TransportZonesLike:
        inputs_hash = "abc123"

        def get(self):
            return _zones()

    seen = {}

    def fake_save_zone_choropleth_svg(**kwargs):
        seen["frame_title"] = kwargs["frame_title"]
        seen["output_path"] = kwargs["output_path"]

    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(
        transport_zones_module,
        "_save_zone_choropleth_svg",
        fake_save_zone_choropleth_svg,
    )

    transport_zones_map(
        TransportZonesLike(),
        save_to_file=True,
        labels=False,
        title="Transport zones",
    )

    assert seen == {
        "frame_title": "Transport zones",
        "output_path": tmp_path / "abc123-transport-zones-map.svg",
    }


def test_population_density_map_computes_population_per_square_km(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    def fake_read_parquet(path):
        assert path == population_groups_path
        return pd.DataFrame(
            {
                "transport_zone_id": [1, 1, 2],
                "weight": [100.0, 50.0, 300.0],
            }
        )

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    fig = population_density_map(_zones(), PopulationLike(), labels=False)

    assert _plotted_zone_ids(fig) == {1, 2, 3}
    assert len([trace for trace in fig.data if getattr(trace, "locations", None) is not None]) <= 5


def test_transport_zone_maps_population_density_reuses_constructor_population(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame({"transport_zone_id": [1], "weight": [100.0]}),
    )

    maps = TransportZoneMaps(_zones(), population=PopulationLike())
    fig = maps.population_density(labels=False)

    assert _plotted_zone_ids(fig) == {1, 2, 3}


def test_metric_map_can_use_continuous_delta_colors():
    values = pl.DataFrame(
        {
            "transport_zone_id": [1, 2, 3],
            "car_modal_share_delta": [-0.2, 0.0, 0.15],
        }
    )

    fig = TransportZoneMaps(_zones()).metric(
        values,
        value_column="car_modal_share_delta",
        save_name="car-modal-share-delta-map",
        labels=False,
        classify=False,
        color_continuous_scale="RdBu_r",
        color_continuous_midpoint=0.0,
        range_color=(-0.2, 0.2),
        colorbar_tickformat=".0%",
    )

    assert _plotted_zone_ids(fig) == {1, 2, 3}
    assert fig.layout.coloraxis.cmid == 0.0
    assert fig.layout.coloraxis.cmin == -0.2
    assert fig.layout.coloraxis.cmax == 0.2
    assert fig.layout.coloraxis.colorbar.tickformat == ".0%"
    assert fig.layout.legend.title.text is None


def test_metric_map_can_plot_only_inner_transport_zones():
    values = pl.DataFrame(
        {
            "transport_zone_id": [1, 2, 3],
            "car_modal_share_delta": [-0.2, 0.0, 0.15],
        }
    )

    fig = TransportZoneMaps(_zones()).metric(
        values,
        value_column="car_modal_share_delta",
        save_name="car-modal-share-delta-map",
        labels=False,
        classify=False,
        inner_zones_only=True,
        color_continuous_midpoint=0.0,
    )

    assert _plotted_zone_ids(fig) == {1, 3}


def test_metric_facets_align_string_metric_zone_ids_to_integer_map_ids():
    values = pl.DataFrame(
        {
            "transport_zone_id": ["1", "2", "3", "1", "2", "3"],
            "scenario": ["default", "default", "default", "project", "project", "project"],
            "trip_count_per_person": [2.0, 4.0, 6.0, 3.0, 5.0, 7.0],
        }
    )

    fig = TransportZoneMaps(_zones()).metric_facets(
        values,
        value_column="trip_count_per_person",
        facet_column="scenario",
        save_name="trip-count-map",
        labels=False,
        classify=False,
    )

    assert _plotted_zone_ids(fig) == {1, 2, 3}
    assert fig.layout.width >= 1240
    assert fig.layout.height >= 620
    assert fig.layout.geo.projection.type == "azimuthal equal area"
    assert fig.layout.geo2.projection.type == "azimuthal equal area"


def test_metric_facets_filter_labels_to_zones_with_metric_values():
    values = pl.DataFrame(
        {
            "transport_zone_id": [1],
            "scenario": ["default"],
            "trip_count": [100.0],
        }
    )

    fig = TransportZoneMaps(_zones()).metric_facets(
        values,
        value_column="trip_count",
        facet_column="scenario",
        save_name="trip-count-map",
        labels=True,
        classify=False,
    )

    text_traces = [
        trace
        for trace in fig.data
        if trace.type == "scattergeo" and trace.mode == "text"
    ]
    assert list(text_traces[-1].text) == ["Alpha"]


def test_metric_grid_creates_scenario_rows_and_variable_columns():
    values = pl.DataFrame(
        {
            "transport_zone_id": ["1", "2", "1", "2", "1", "2", "1", "2"],
            "scenario": ["default", "default", "default", "default", "project", "project", "project", "project"],
            "mode": ["car", "car", "walk", "walk", "car", "car", "walk", "walk"],
            "trip_count_share": [0.8, 0.2, 0.2, 0.8, 0.7, 0.3, 0.3, 0.7],
        }
    )

    fig = TransportZoneMaps(_zones()).metric_grid(
        values,
        value_column="trip_count_share",
        row_column="scenario",
        column_column="mode",
        save_name="modal-share-map",
        labels=False,
        classify=False,
        range_color=(0.0, 1.0),
        colorbar_tickformat=".0%",
    )

    assert _plotted_zone_ids(fig) == {1, 2}
    assert fig.layout.width >= 1240
    assert fig.layout.height >= 1240
    assert fig.layout.coloraxis.cmin == 0.0
    assert fig.layout.coloraxis.cmax == 1.0
    assert fig.layout.coloraxis.colorbar.tickformat == ".0%"
    assert fig.layout.geo.projection.type == "azimuthal equal area"
    assert fig.layout.geo4.projection.type == "azimuthal equal area"


def test_metric_flows_draws_top_od_lines_with_proportional_widths():
    values = pl.DataFrame(
        {
            "origin_zone_id": ["1", "1", "2", "3"],
            "destination_zone_id": ["2", "3", "3", "1"],
            "scenario": ["default", "default", "default", "project"],
            "travel_time": [100.0, 50.0, 1.0, 25.0],
            "travel_time_std": [10.0, 5.0, 0.1, 2.5],
        }
    )

    fig = TransportZoneMaps(_zones()).metric_flows(
        values,
        value_column="travel_time",
        origin_column="origin_zone_id",
        destination_column="destination_zone_id",
        facet_column="scenario",
        save_name="flow-map",
        n_largest=2,
        max_line_width=8.0,
        min_line_width=0.1,
        hover_columns=["travel_time_std"],
        labels=False,
    )

    flow_traces = [
        trace
        for trace in fig.data
        if trace.type == "scattergeo" and not trace.showlegend
    ]
    legend_traces = [
        trace
        for trace in fig.data
        if trace.type == "scattergeo" and trace.showlegend
    ]
    assert len(flow_traces) == 3
    assert sorted(trace.line.width for trace in flow_traces) == pytest.approx([2.0, 4.0, 8.0])
    assert [trace.name for trace in legend_traces] == ["25", "50", "100"]
    assert [trace.line.width for trace in legend_traces] == pytest.approx([2.0, 4.0, 8.0])
    assert [trace.marker.size for trace in legend_traces] == pytest.approx([2.0, 4.0, 8.0])
    assert fig.layout.legend.title.text == "travel_time"
    assert all("travel_time_std" in trace.text for trace in flow_traces)
    assert fig.layout.width >= 1240
    assert fig.layout.geo.projection.type == "azimuthal equal area"
    assert fig.layout.geo2.projection.type == "azimuthal equal area"


def test_metric_flows_drops_lines_below_render_width_threshold():
    values = pl.DataFrame(
        {
            "origin_zone_id": ["1", "1"],
            "destination_zone_id": ["2", "3"],
            "scenario": ["default", "default"],
            "ghg_emissions": [100.0, 1.0],
        }
    )

    fig = TransportZoneMaps(_zones()).metric_flows(
        values,
        value_column="ghg_emissions",
        origin_column="origin_zone_id",
        destination_column="destination_zone_id",
        facet_column="scenario",
        save_name="flow-map",
        n_largest=None,
        max_line_width=8.0,
        min_line_width=0.1,
        labels=False,
    )

    flow_traces = [
        trace
        for trace in fig.data
        if trace.type == "scattergeo" and not trace.showlegend
    ]
    assert len(flow_traces) == 1
    assert flow_traces[0].line.width == pytest.approx(8.0)


def test_metric_flows_draws_intrazonal_flows_as_proportional_dots():
    values = pl.DataFrame(
        {
            "origin_zone_id": ["1", "1"],
            "destination_zone_id": ["1", "2"],
            "scenario": ["default", "default"],
            "trip_count": [100.0, 50.0],
        }
    )

    fig = TransportZoneMaps(_zones()).metric_flows(
        values,
        value_column="trip_count",
        origin_column="origin_zone_id",
        destination_column="destination_zone_id",
        facet_column="scenario",
        save_name="flow-map",
        n_largest=None,
        max_line_width=8.0,
        min_line_width=0.1,
        labels=False,
    )

    dot_traces = [
        trace
        for trace in fig.data
        if trace.type == "scattergeo" and trace.mode == "markers" and not trace.showlegend
    ]
    line_traces = [
        trace
        for trace in fig.data
        if trace.type == "scattergeo" and trace.mode == "lines" and not trace.showlegend
    ]
    assert len(dot_traces) == 1
    assert len(line_traces) == 1
    assert dot_traces[0].marker.size == pytest.approx(8.0)
    assert line_traces[0].line.width == pytest.approx(4.0)


def test_metric_map_filters_outer_labels_when_plotting_only_inner_zones(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame(
            {
                "transport_zone_id": [1, 2, 3],
                "weight": [10.0, 1_000.0, 20.0],
            }
        ),
    )
    values = pl.DataFrame(
        {
            "transport_zone_id": [1, 2, 3],
            "car_modal_share_delta": [-0.2, 0.0, 0.15],
        }
    )

    fig = TransportZoneMaps(_zones(), population=PopulationLike()).metric(
        values,
        value_column="car_modal_share_delta",
        save_name="car-modal-share-delta-map",
        inner_zones_only=True,
        classify=False,
    )

    label_text = list(fig.data[-1].text)
    assert "Beta" not in label_text
    assert set(label_text).issubset({"Alpha", "Gamma"})


def test_metric_map_writes_continuous_svg_with_report_export(monkeypatch, tmp_path):
    class TransportZonesLike:
        inputs_hash = "abc123"

        def get(self):
            return _zones()

    values = pl.DataFrame(
        {
            "transport_zone_id": [1, 2, 3],
            "car_modal_share_delta": [-0.02, 0.0, 0.01],
        }
    )
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    TransportZoneMaps(TransportZonesLike()).metric(
        values,
        value_column="car_modal_share_delta",
        save_name="car-modal-share-delta-map",
        save_to_file=True,
        labels=False,
        classify=False,
        color_continuous_scale=[
            [0.0, "#44546A"],
            [0.5, "#FFFFFF"],
            [1.0, "#D71A1C"],
        ],
        color_continuous_midpoint=0.0,
        range_color=(-0.03, 0.03),
        colorbar_tickformat=".0%",
    )

    assert (tmp_path / "abc123-car-modal-share-delta-map.svg").exists()


def test_metric_map_can_write_to_explicit_png_path(tmp_path):
    values = pl.DataFrame(
        {
            "transport_zone_id": [1, 2, 3],
            "car_modal_share_delta": [-0.02, 0.0, 0.01],
        }
    )
    output_path = tmp_path / "frame.png"

    TransportZoneMaps(_zones()).metric(
        values,
        value_column="car_modal_share_delta",
        save_name="unused-name",
        output_path=output_path,
        labels=False,
        classify=False,
        color_continuous_midpoint=0.0,
        range_color=(-0.03, 0.03),
        colorbar_tickformat=".0%",
    )

    assert output_path.exists()


def test_population_density_map_writes_expected_svg(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class TransportZonesLike:
        inputs_hash = "abc123"

        def get(self):
            return _zones()

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame({"transport_zone_id": [1], "weight": [100.0]}),
    )
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    population_density_map(TransportZonesLike(), PopulationLike(), save_to_file=True, labels=False)

    assert (tmp_path / "abc123-population-density-map.svg").exists()


def test_opportunity_density_map_uses_run_opportunity_capacity(tmp_path):
    opportunities_path = tmp_path / "opportunities.parquet"
    pl.DataFrame(
        {
            "to": [1, 2, 2],
            "activity": ["work", "work", "leisure"],
            "opportunity_capacity": [10.0, 30.0, 1000.0],
        }
    ).write_parquet(opportunities_path)

    class ActivityLike:
        name = "work"
        has_opportunities = True

    class RunLike:
        cache_path = {"opportunities": opportunities_path}

    fig = opportunity_density_map(_zones(), ActivityLike(), RunLike(), labels=False)

    assert _plotted_zone_ids(fig) == {1, 2, 3}
    assert len([trace for trace in fig.data if getattr(trace, "locations", None) is not None]) <= 5
    assert fig.layout.legend.title.text == "Opportunity hours/km2"


def test_opportunity_density_map_runs_model_when_opportunities_are_missing(tmp_path):
    opportunities_path = tmp_path / "opportunities.parquet"

    class ActivityLike:
        name = "work"
        has_opportunities = True

    class RunLike:
        cache_path = {"opportunities": opportunities_path}
        calls = 0

        def get(self):
            self.calls += 1
            pl.DataFrame(
                {
                    "to": [1],
                    "activity": ["work"],
                    "opportunity_capacity": [10.0],
                }
            ).write_parquet(opportunities_path)

    run = RunLike()

    opportunity_density_map(_zones(), ActivityLike(), run, labels=False)

    assert run.calls == 1
    assert opportunities_path.exists()


def test_opportunity_density_map_rejects_missing_run_cache_path():
    class ActivityLike:
        name = "work"
        has_opportunities = True

    with pytest.raises(TypeError, match="cache_path"):
        opportunity_density_map(_zones(), ActivityLike(), object(), labels=False)


def test_model_opportunity_density_requires_capacity_column(tmp_path):
    opportunities_path = tmp_path / "opportunities.parquet"
    pl.DataFrame(
        {
            "to": [1],
            "activity": ["work"],
            "n_opp": [10.0],
        }
    ).write_parquet(opportunities_path)

    class ActivityLike:
        name = "work"
        has_opportunities = True

    class RunLike:
        cache_path = {"opportunities": opportunities_path}

    with pytest.raises(ValueError, match="opportunity_capacity"):
        opportunity_density_map(_zones(), ActivityLike(), RunLike(), labels=False)


def test_opportunity_density_map_writes_expected_svg(monkeypatch, tmp_path):
    opportunities_path = tmp_path / "opportunities.parquet"
    pl.DataFrame(
        {
            "to": [1],
            "activity": ["work"],
            "opportunity_capacity": [25.0],
        }
    ).write_parquet(opportunities_path)

    class TransportZonesLike:
        inputs_hash = "abc123"

        def get(self):
            return _zones()

    class ActivityLike:
        name = "work"
        has_opportunities = True

    class RunLike:
        cache_path = {"opportunities": opportunities_path}

    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    opportunity_density_map(
        TransportZonesLike(),
        ActivityLike(),
        RunLike(),
        save_to_file=True,
        labels=False,
    )

    assert (tmp_path / "abc123-work-opportunity-density-map.svg").exists()


def test_opportunity_density_map_rejects_activity_without_opportunities():
    class ActivityLike:
        name = "home"
        has_opportunities = False

    with pytest.raises(ValueError, match="no destination opportunities"):
        opportunity_density_map(_zones(), ActivityLike(), object(), labels=False)


def test_labels_are_the_same_across_report_maps(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"
    opportunities_path = tmp_path / "opportunities.parquet"
    pl.DataFrame(
        {
            "to": [1, 2],
            "activity": ["work", "work"],
            "opportunity_capacity": [25.0, 75.0],
        }
    ).write_parquet(opportunities_path)

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    class ActivityLike:
        name = "work"
        has_opportunities = True

    class RunLike:
        cache_path = {"opportunities": opportunities_path}

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame(
            {"transport_zone_id": [1, 2], "weight": [100.0, 300.0]}
        ),
    )

    maps = TransportZoneMaps(_zones(), population=PopulationLike())
    transport_fig = maps.transport_zones()
    population_fig = maps.population_density(PopulationLike())
    opportunity_fig = maps.opportunity_density(ActivityLike(), RunLike())

    assert transport_fig.data[-1].text == population_fig.data[-1].text
    assert transport_fig.data[-1].text == opportunity_fig.data[-1].text


def test_transport_zones_map_save_requires_asset_hash():
    with pytest.raises(ValueError, match="input hash"):
        transport_zones_map(_zones(), save_to_file=True, labels=False)


def test_city_labels_without_population_prefer_inner_dense_communes():
    labels = _select_city_labels(_zones(), population=None, max_labels=2)

    assert labels["local_admin_unit_id"].to_list() == ["city-a", "city-c"]
    assert labels["label"].to_list() == ["Alpha", "Gamma"]


def test_city_labels_are_filtered_by_plotted_local_admin_units():
    labels = pd.DataFrame(
        {
            "local_admin_unit_id": ["city-a", "city-b"],
            "label": ["Alpha", "Beta"],
            "lon": [0.0, 1.0],
            "lat": [0.0, 1.0],
            "font_size": [9, 7],
        }
    )
    plotted_zones = _zones().loc[_zones()["local_admin_unit_id"] == "city-a"]

    filtered = _filter_labels_to_zones(labels, plotted_zones)

    assert filtered["label"].to_list() == ["Alpha"]


def test_city_labels_without_population_do_not_prefer_large_sparse_communes():
    zones = gpd.GeoDataFrame(
        {
            "transport_zone_id": [1, 2, 3, 4],
            "local_admin_unit_id": ["dense", "dense", "dense", "large"],
            "local_admin_unit_name": ["Dense", "Dense", "Dense", "Large"],
            "is_inner_zone": [True, True, True, False],
        },
        geometry=[
            box(0, 0, 1000, 1000),
            box(1000, 0, 2000, 1000),
            box(0, 1000, 1000, 2000),
            box(10_000, 0, 50_000, 40_000),
        ],
        crs="EPSG:3035",
    )

    labels = _select_city_labels(zones, population=None, max_labels=1)

    assert labels.iloc[0]["label"] == "Dense"


def test_city_labels_prefer_population_when_available(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame(
            {
                "transport_zone_id": [1, 2, 3],
                "weight": [10.0, 1_000.0, 20.0],
            }
        ),
    )

    labels = _select_city_labels(_zones(), population=PopulationLike(), max_labels=2)

    assert labels.iloc[0]["label"] == "Beta"


def test_transport_zone_maps_can_rank_labels_by_population(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda path: pd.DataFrame(
            {
                "transport_zone_id": [1, 2, 3],
                "weight": [10.0, 1_000.0, 20.0],
            }
        ),
    )

    fig = TransportZoneMaps(_zones(), population=PopulationLike()).transport_zones()

    assert fig.data[-1].text[0] == "Beta"


def test_large_legend_numbers_keep_thousands_separator():
    assert _format_significant(15000) == "15,000"


def test_density_classes_use_yellow_orange_red_palette():
    zones, _, colors = _add_head_tail_classes(
        _zones().assign(value=[1.0, 10.0, 100.0]),
        "value",
    )

    assert len(zones) == 3
    assert list(colors.values())[0] == "#ffffcc"


def test_metric_values_frame_keeps_extra_columns_and_validates_inputs():
    values = pd.DataFrame(
        {
            "zone": ["1"],
            "value": [2.0],
            "scenario": ["default"],
        }
    )

    frame = _metric_values_frame(values, id_column="zone", value_column="value")

    assert frame.to_dict("records") == [
        {
            "transport_zone_id": "1",
            "value": 2.0,
            "scenario": "default",
        }
    ]
    with pytest.raises(TypeError, match="pandas or polars"):
        _metric_values_frame([], id_column="zone", value_column="value")
    with pytest.raises(ValueError, match="missing column"):
        _metric_values_frame(pd.DataFrame({"zone": [1]}), "zone", "value")


def test_metric_flow_values_frame_keeps_extra_columns_and_validates_inputs():
    values = pl.DataFrame(
        {
            "origin": [1],
            "destination": [2],
            "trips": [5.0],
            "scenario": ["project"],
        }
    )

    frame = _metric_flow_values_frame(
        values,
        origin_column="origin",
        destination_column="destination",
        value_column="trips",
    )

    assert frame.to_dict("records") == [
        {
            "origin": 1,
            "destination": 2,
            "trips": 5.0,
            "scenario": "project",
        }
    ]
    with pytest.raises(TypeError, match="pandas or polars"):
        _metric_flow_values_frame([], "origin", "destination", "trips")
    with pytest.raises(ValueError, match="missing column"):
        _metric_flow_values_frame(pd.DataFrame({"origin": [1]}), "origin", "destination", "trips")


def test_metric_zone_id_alignment_matches_integer_and_string_zone_ids():
    integer_zones = _zones()
    value_frame = pd.DataFrame({"transport_zone_id": ["1", "2"], "value": [1.0, 2.0]})

    aligned = _align_metric_zone_ids(value_frame, integer_zones)

    assert aligned["transport_zone_id"].tolist() == [1, 2]
    assert aligned["transport_zone_id"].dtype == integer_zones["transport_zone_id"].dtype

    string_zones = integer_zones.assign(transport_zone_id=["z1", "z2", "z3"])
    flow_frame = pd.DataFrame({"origin": [1], "destination": [2], "value": [3.0]})

    aligned_flow = _align_flow_zone_ids(
        flow_frame,
        string_zones,
        origin_column="origin",
        destination_column="destination",
    )

    assert aligned_flow[["origin", "destination"]].to_dict("records") == [
        {"origin": "1", "destination": "2"}
    ]


def test_metric_value_zones_keeps_only_zones_with_values():
    value_frame = pd.DataFrame(
        {
            "transport_zone_id": [1, 2],
            "value": [5.0, None],
        }
    )

    valued_zones = _metric_value_zones(_zones(), value_frame=value_frame, value_column="value")
    fallback_zones = _metric_value_zones(
        _zones(),
        value_frame=value_frame,
        value_column="missing",
    )

    assert valued_zones["transport_zone_id"].to_list() == [1]
    assert fallback_zones["transport_zone_id"].to_list() == [1, 2, 3]


def test_prepare_metric_flow_lines_filters_values_and_adds_coordinates():
    map_data = TransportZoneMaps(_zones())._map_data
    flow_frame = pd.DataFrame(
        {
            "origin": [1, 1, 2, 2, 3],
            "destination": [2, 3, 3, 1, 1],
            "scenario": ["default", "default", "default", "default", "project"],
            "trips": [100.0, 40.0, 5.0, -1.0, None],
        }
    )

    flows = _prepare_metric_flow_lines(
        flow_frame,
        map_data=map_data,
        value_column="trips",
        origin_column="origin",
        destination_column="destination",
        facet_column="scenario",
        n_largest=2,
        min_value=10.0,
        min_share=0.2,
        max_line_width=8.0,
        min_line_width=1.0,
    )

    assert flows[["origin", "destination", "trips"]].to_dict("records") == [
        {"origin": 1, "destination": 2, "trips": 100.0},
        {"origin": 1, "destination": 3, "trips": 40.0},
    ]
    assert flows["_line_width"].to_list() == pytest.approx([8.0, 3.2])
    assert {"_origin_lon", "_origin_lat", "_destination_lon", "_destination_lat"}.issubset(
        flows.columns
    )


def test_prepare_metric_flow_lines_validates_width_and_filter_settings():
    map_data = TransportZoneMaps(_zones())._map_data
    flow_frame = pd.DataFrame(
        {
            "origin": [1],
            "destination": [2],
            "scenario": ["default"],
            "trips": [1.0],
        }
    )

    with pytest.raises(ValueError, match="max_line_width"):
        _prepare_metric_flow_lines(
            flow_frame,
            map_data=map_data,
            value_column="trips",
            origin_column="origin",
            destination_column="destination",
            facet_column="scenario",
            n_largest=None,
            min_value=None,
            min_share=None,
            max_line_width=0.0,
            min_line_width=0.1,
        )
    with pytest.raises(ValueError, match="min_share"):
        _prepare_metric_flow_lines(
            flow_frame,
            map_data=map_data,
            value_column="trips",
            origin_column="origin",
            destination_column="destination",
            facet_column="scenario",
            n_largest=None,
            min_value=None,
            min_share=2.0,
            max_line_width=8.0,
            min_line_width=0.1,
        )


def test_continuous_zone_colors_support_midpoints_and_validate_colormap_names():
    colors, _, norm = _continuous_zone_colors(
        pd.Series([-1.0, 0.0, 1.0, None]),
        color_continuous_scale=[
            [0.0, "#000000"],
            [0.5, "#FFFFFF"],
            [1.0, "#FF0000"],
        ],
        color_continuous_midpoint=0.0,
        range_color=None,
    )

    assert len(colors) == 4
    assert norm.vmin == pytest.approx(-1.0)
    assert norm.vmax == pytest.approx(1.0)
    with pytest.raises(ValueError, match="matplotlib colormap"):
        _continuous_zone_colors(
            pd.Series([1.0]),
            color_continuous_scale="not-a-colormap",
            color_continuous_midpoint=None,
            range_color=None,
        )


def test_metric_facet_and_grid_validate_missing_or_empty_facet_columns():
    maps = TransportZoneMaps(_zones())

    with pytest.raises(ValueError, match="scenario"):
        maps.metric_facets(
            pd.DataFrame({"transport_zone_id": [1], "value": [1.0]}),
            value_column="value",
            facet_column="scenario",
            save_name="metric",
        )
    with pytest.raises(ValueError, match="row"):
        maps.metric_grid(
            pd.DataFrame({"transport_zone_id": [1], "value": [1.0], "column": ["car"]}),
            value_column="value",
            row_column="row",
            column_column="column",
            save_name="metric",
        )
