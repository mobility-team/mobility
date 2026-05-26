import geopandas as gpd
import pandas as pd
import polars as pl
import pytest
from shapely.geometry import box

import mobility.reports.transport_zones as transport_zones_module
from mobility.reports.transport_zones import (
    TransportZoneMaps,
    _add_head_tail_classes,
    _format_significant,
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

    assert labels["label"].to_list() == ["Alpha", "Gamma"]


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
