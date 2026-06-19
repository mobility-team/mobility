import pathlib
import re

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from mobility.runtime.assets.asset import Asset
from mobility.spatial.admin_datasets import AdminExpressDataset
from mobility.spatial.admin_units import FrenchAdminUnits
from mobility.spatial.local_admin_units import LocalAdminUnits
from mobility.spatial.local_admin_units_categories import LocalAdminUnitsCategories
from mobility.spatial.study_area import StudyArea
from mobility.spatial.switzerland import SwissLocalAdminUnitsCategories


def test_001_admin_express_dataset_requires_expected_extracted_files(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    dataset = AdminExpressDataset()
    dataset.cache_path.write_text("ready", encoding="utf-8")
    dataset.extracted_path.mkdir(parents=True)

    assert dataset.assets_missing()

    for expected_file in dataset.expected_files:
        expected_file.write_text("", encoding="utf-8")

    assert not dataset.assets_missing()


def test_001_french_admin_units_prepare_communes_with_parent_codes(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(AdminExpressDataset, "get", lambda self: pathlib.Path("admin-express"))

    communes = gpd.GeoDataFrame(
        {
            "INSEE_COM": ["75056", "33063"],
            "INSEE_CAN": ["NR", "01"],
            "NOM": ["Paris", "Bordeaux"],
            "SIREN_EPCI": ["20005478100123", "243300316"],
            "INSEE_DEP": ["75", "33"],
            "INSEE_REG": ["11", "75"],
        },
        geometry=[box(2.2, 48.8, 2.4, 48.9), box(-0.7, 44.8, -0.5, 44.9)],
        crs=4326,
    )
    arrondissements = gpd.GeoDataFrame(
        {
            "INSEE_COM": ["75056"],
            "INSEE_ARM": ["75101"],
            "NOM": ["Paris 1er Arrondissement"],
        },
        geometry=[box(2.3, 48.85, 2.36, 48.87)],
        crs=4326,
    )

    def fake_read_file(path):
        if pathlib.Path(path).name == "COMMUNE.shp":
            return communes
        if pathlib.Path(path).name == "ARRONDISSEMENT_MUNICIPAL.shp":
            return arrondissements
        raise AssertionError(f"Unexpected layer: {path}")

    monkeypatch.setattr("mobility.spatial.admin_units.gpd.read_file", fake_read_file)

    admin_units = FrenchAdminUnits(level="commune").create_and_get_asset()

    assert set(admin_units["admin_id"]) == {"fr-75101", "fr-33063"}

    paris = admin_units.loc[admin_units["admin_id"] == "fr-75101"].iloc[0]
    assert paris["parent_commune_id"] == "fr-75056"
    assert paris["commune_id"] == "fr-75101"
    assert paris["canton_id"] == "75ZZ"
    assert paris["epci_id"] == "fr-200054781"
    assert paris["departement_id"] == "fr-75"
    assert paris["region_id"] == "fr-11"

    assert {"minx", "miny", "maxx", "maxy"}.issubset(admin_units.columns)
    assert admin_units.crs.to_epsg() == 3035
    assert admin_units["minx"].between(-1.0, 3.0).all()
    assert admin_units["maxy"].between(44.0, 50.0).all()


def test_001_french_admin_units_fail_on_missing_commune_columns(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(AdminExpressDataset, "get", lambda self: pathlib.Path("admin-express"))

    incomplete_communes = gpd.GeoDataFrame(
        {"INSEE_COM": ["33063"], "NOM": ["Bordeaux"], "INSEE_CAN": ["01"]},
        geometry=[box(-0.7, 44.8, -0.5, 44.9)],
        crs=4326,
    )
    arrondissements = gpd.GeoDataFrame(
        {"INSEE_COM": [], "INSEE_ARM": [], "NOM": []},
        geometry=[],
        crs=4326,
    )

    def fake_read_file(path):
        if pathlib.Path(path).name == "COMMUNE.shp":
            return incomplete_communes
        if pathlib.Path(path).name == "ARRONDISSEMENT_MUNICIPAL.shp":
            return arrondissements
        raise AssertionError(f"Unexpected layer: {path}")

    monkeypatch.setattr("mobility.spatial.admin_units.gpd.read_file", fake_read_file)

    with pytest.raises(ValueError, match="COMMUNE.shp is missing columns"):
        FrenchAdminUnits(level="commune").create_and_get_asset()


def test_001_local_admin_units_tracks_country_admin_assets_as_inputs(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    local_admin_units = LocalAdminUnits()

    assert set(local_admin_units.inputs["admin_units_by_country"]) == {"fr", "ch"}
    assert local_admin_units.inputs["categories"].inputs["countries"] == []
    assert set(local_admin_units.inputs["categories"].inputs["categories_by_country"]) == {"fr", "ch"}


def test_001_local_admin_unit_categories_load_selected_ids(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    class FakeFrenchCategories(Asset):
        requested_ids = []

        def __init__(self):
            super().__init__({"country": "fr"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Selected local admin units should use get_by_ids.")

        def get_by_ids(self, local_admin_unit_ids):
            self.requested_ids.append(tuple(local_admin_unit_ids))
            return pd.DataFrame(
                {
                    "local_admin_unit_id": ["fr-75056"],
                    "urban_unit_category": ["C"],
                }
            )

    class FakeSwissCategories(Asset):
        requested_ids = []

        def __init__(self):
            super().__init__({"country": "ch"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Selected local admin units should use get_by_ids.")

        def get_by_ids(self, local_admin_unit_ids):
            self.requested_ids.append(tuple(local_admin_unit_ids))
            return pd.DataFrame(
                {
                    "local_admin_unit_id": ["ch-6621"],
                    "urban_unit_category": ["B"],
                }
            )

    monkeypatch.setattr(
        "mobility.spatial.local_admin_units_categories.available_local_admin_unit_categories",
        lambda: {"fr": FakeFrenchCategories(), "ch": FakeSwissCategories()},
    )

    categories = LocalAdminUnitsCategories().get_by_ids(["ch-6621", "fr-75056"])

    assert set(categories["local_admin_unit_id"]) == {"fr-75056", "ch-6621"}
    assert FakeFrenchCategories.requested_ids == [("ch-6621", "fr-75056")]
    assert FakeSwissCategories.requested_ids == [("ch-6621", "fr-75056")]


def test_001_swiss_local_admin_unit_categories_use_bfs_type_codes(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    def fake_download_file(url, file_path):
        return pathlib.Path(file_path)

    def fake_read_excel(file_path, skiprows, skipfooter):
        return pd.DataFrame(
            {
                "bfs_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009],
                "unused": [""] * 9,
                "typology": [
                    "Urban wording can vary (11)",
                    "Urban wording with another apostrophe (12)",
                    "Small urban area wording can vary (13)",
                    "Periurban high density wording can vary (21)",
                    "Periurban middle density wording can vary (22)",
                    "Periurban low density wording can vary (23)",
                    "Rural centre wording can vary (31)",
                    "Central rural wording can vary (32)",
                    "Peripheral rural wording can vary (33)",
                ],
            }
        )

    monkeypatch.setattr("mobility.spatial.switzerland.download_file", fake_download_file)
    monkeypatch.setattr("mobility.spatial.switzerland.pd.read_excel", fake_read_excel)

    categories = SwissLocalAdminUnitsCategories().create_and_get_asset()

    assert categories["urban_unit_category"].tolist() == [
        "C",
        "C",
        "I",
        "B",
        "B",
        "B",
        "R",
        "R",
        "R",
    ]


def test_001_local_admin_units_loads_selected_admin_units(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    class FakeFrenchAdminUnits(Asset):
        requested_ids = []

        def __init__(self, level):
            super().__init__({"level": level, "country": "fr"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Explicit local admin unit lists should use get_by_ids.")

        def get_by_ids(self, admin_ids):
            self.requested_ids.append(tuple(admin_ids))
            return gpd.GeoDataFrame(
                {
                    "admin_id": ["fr-75056"],
                    "admin_name": ["Paris"],
                    "country": ["fr"],
                },
                geometry=[box(0, 0, 1, 1)],
                crs=3035,
            )

    class FakeSwissAdminUnits(Asset):
        requested_ids = []

        def __init__(self, level):
            super().__init__({"level": level, "country": "ch"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Explicit local admin unit lists should use get_by_ids.")

        def get_by_ids(self, admin_ids):
            self.requested_ids.append(tuple(admin_ids))
            return gpd.GeoDataFrame(
                {
                    "admin_id": ["ch-6621"],
                    "admin_name": ["Geneve"],
                    "country": ["ch"],
                },
                geometry=[box(1, 1, 2, 2)],
                crs=3035,
            )

    def fake_categories_get_by_ids(self, local_admin_unit_ids):
        assert set(local_admin_unit_ids) == {"fr-75056", "ch-6621"}
        return pd.DataFrame(
            {
                "local_admin_unit_id": ["fr-75056", "ch-6621"],
                "urban_unit_category": ["C", "B"],
            }
        )

    monkeypatch.setattr(
        "mobility.spatial.local_admin_units.available_admin_units",
        lambda: {
            "fr": (FakeFrenchAdminUnits, "commune"),
            "ch": (FakeSwissAdminUnits, "municipality"),
        },
    )
    monkeypatch.setattr(LocalAdminUnitsCategories, "get_by_ids", fake_categories_get_by_ids)

    local_admin_units = LocalAdminUnits(
        local_admin_unit_ids=["ch-6621", "fr-75056"],
    ).create_and_get_asset()

    assert set(local_admin_units["local_admin_unit_id"]) == {"fr-75056", "ch-6621"}
    assert FakeFrenchAdminUnits.requested_ids == [("ch-6621", "fr-75056")]
    assert FakeSwissAdminUnits.requested_ids == [("ch-6621", "fr-75056")]


def test_001_local_admin_units_fails_when_selected_admin_unit_is_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    class FakeAdminUnits(Asset):
        def __init__(self, level):
            super().__init__({"level": level, "country": "fr"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Explicit local admin unit lists should use get_by_ids.")

        def get_by_ids(self, admin_ids):
            return gpd.GeoDataFrame(
                {"admin_id": [], "admin_name": [], "country": []},
                geometry=[],
                crs=3035,
            )

    monkeypatch.setattr(
        "mobility.spatial.local_admin_units.available_admin_units",
        lambda: {"fr": (FakeAdminUnits, "commune")},
    )

    with pytest.raises(ValueError, match="No local admin unit found"):
        LocalAdminUnits(local_admin_unit_ids=["fr-00000"]).create_and_get_asset()

    def fake_categories_get_by_ids(self, local_admin_unit_ids):
        return pd.DataFrame(columns=["local_admin_unit_id", "urban_unit_category"])

    monkeypatch.setattr(
        "mobility.spatial.local_admin_units.available_admin_units",
        lambda: {"fr": (FakeAdminUnits, "commune")},
    )
    monkeypatch.setattr(LocalAdminUnitsCategories, "get_by_ids", fake_categories_get_by_ids)

    with pytest.raises(ValueError, match=re.escape("No local admin unit found for: ['fr-75056'].")):
        LocalAdminUnits(local_admin_unit_ids=["fr-75056"]).create_and_get_asset()


def test_001_local_admin_units_loads_admin_units_near_bounds(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    class FakeFrenchAdminUnits(Asset):
        requested_bounds = []

        def __init__(self, level):
            super().__init__({"level": level, "country": "fr"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Bounds should be used before loading all admin units.")

        def get_within_bounds(self, bounds):
            self.requested_bounds.append(bounds)
            return gpd.GeoDataFrame(
                {
                    "admin_id": ["fr-75056"],
                    "admin_name": ["Paris"],
                    "country": ["fr"],
                },
                geometry=[box(0, 0, 1, 1)],
                crs=3035,
            )

    class FakeSwissAdminUnits(Asset):
        requested_bounds = []

        def __init__(self, level):
            super().__init__({"level": level, "country": "ch"})

        def get_cached_hash(self):
            return self.inputs_hash

        def get(self):
            raise AssertionError("Bounds should be used before loading all admin units.")

        def get_within_bounds(self, bounds):
            self.requested_bounds.append(bounds)
            return gpd.GeoDataFrame(
                {"admin_id": [], "admin_name": [], "country": []},
                geometry=[],
                crs=3035,
            )

    def fake_categories_get_by_ids(self, local_admin_unit_ids):
        assert local_admin_unit_ids == ["fr-75056"]
        return pd.DataFrame(
            {
                "local_admin_unit_id": ["fr-75056"],
                "urban_unit_category": ["C"],
            }
        )

    monkeypatch.setattr(
        "mobility.spatial.local_admin_units.available_admin_units",
        lambda: {
            "fr": (FakeFrenchAdminUnits, "commune"),
            "ch": (FakeSwissAdminUnits, "municipality"),
        },
    )
    monkeypatch.setattr(LocalAdminUnitsCategories, "get_by_ids", fake_categories_get_by_ids)

    local_admin_units = LocalAdminUnits(bounds=(1.0, 2.0, 3.0, 4.0)).create_and_get_asset()

    assert local_admin_units["local_admin_unit_id"].tolist() == ["fr-75056"]
    assert FakeFrenchAdminUnits.requested_bounds == [(1.0, 2.0, 3.0, 4.0)]
    assert FakeSwissAdminUnits.requested_bounds == [(1.0, 2.0, 3.0, 4.0)]


def test_001_study_area_radius_mode_tracks_only_the_center_unit_first(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    study_area = StudyArea(local_admin_unit_id="fr-75056", radius=20.0)

    assert study_area.inputs["local_admin_units"].inputs["radius"] == 20.0
    assert (
        study_area.inputs["local_admin_units"]
        .inputs["center_local_admin_unit"]
        .inputs["local_admin_unit_ids"]
        == ["fr-75056"]
    )


def test_001_population_commune_boundaries_use_admin_units(monkeypatch):
    class FakeFrenchAdminUnits:
        def __init__(self, level):
            assert level == "commune"

        def get(self):
            return gpd.GeoDataFrame(
                {
                    "admin_id": ["fr-75101"],
                    "canton_id": ["75ZZ"],
                    "epci_id": ["fr-200054781"],
                    "admin_name": ["Paris 1er Arrondissement"],
                },
                geometry=[box(2.3, 48.85, 2.36, 48.87)],
                crs=3035,
            )

    monkeypatch.setattr(
        FrenchAdminUnits,
        "get",
        lambda self: FakeFrenchAdminUnits("commune").get(),
    )

    cities = FrenchAdminUnits.get_population_commune_boundaries()

    assert cities.columns.tolist() == [
        "INSEE_COM",
        "INSEE_CAN",
        "SIREN_EPCI",
        "NOM",
        "geometry",
    ]
    assert cities.iloc[0]["INSEE_COM"] == "fr-75101"
    assert cities.iloc[0]["SIREN_EPCI"] == "200054781"
