import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from mobility.reports.transport_zones import (
    _select_city_labels,
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


def test_transport_zones_map_accepts_geodataframe():
    fig = transport_zones_map(_zones(), labels=False)

    assert fig.layout.title.text is None
    assert len(fig.data) == 2


def test_transport_zones_map_adds_labels_by_default():
    fig = transport_zones_map(_zones())

    assert len(fig.data) == 4


def test_transport_zones_map_accepts_asset_like_input():
    class TransportZonesLike:
        def get(self):
            return _zones()

    fig = transport_zones_map(TransportZonesLike(), labels=False)

    assert fig.layout.title.text is None


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


def test_transport_zones_map_save_requires_asset_hash():
    with pytest.raises(ValueError, match="input hash"):
        transport_zones_map(_zones(), save_to_file=True, labels=False)


def test_city_labels_prefer_population_weight(monkeypatch, tmp_path):
    population_groups_path = tmp_path / "population_groups.parquet"

    class PopulationLike:
        def get(self):
            return {"population_groups": population_groups_path}

    def fake_read_parquet(path):
        assert path == population_groups_path
        return pd.DataFrame(
            {
                "transport_zone_id": [1, 2, 3],
                "weight": [10.0, 1000.0, 20.0],
            }
        )

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    labels = _select_city_labels(_zones(), population=PopulationLike(), max_labels=2)

    assert labels.iloc[0]["label"] == "Beta"
