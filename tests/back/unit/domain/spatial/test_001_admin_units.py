import pathlib

import geopandas as gpd
import pytest
from shapely.geometry import box

from mobility.spatial.admin_datasets import AdminExpressDataset
from mobility.spatial.admin_units import FrenchAdminUnits
from mobility.spatial.local_admin_units import LocalAdminUnits


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

    assert "french_local_admin_units" in local_admin_units.inputs
    assert "swiss_local_admin_units" in local_admin_units.inputs


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
