import importlib.util
import json
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import pytest
from shapely.geometry import Point

from mobility.spatial.osm.geofabrik_regions import GeofabrikRegions

spec = importlib.util.spec_from_file_location(
    "ci_input_bundle", Path(__file__).resolve().parents[3] / ".github/scripts/ci_input_bundle.py"
)
bundle_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle_script)


def test_bundle_restores_a_usable_cache_without_downloading(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(source))
    asset = GeofabrikRegions()
    gpd.GeoDataFrame(
        {"url": [f"https://example.test/{i}-260101.osm.pbf" for i in range(23)]},
        geometry=[Point(i, i) for i in range(23)], crs=4326,
    ).to_file(asset.cache_path)
    archive = tmp_path / "bundle.zip"
    bundle_script.create(archive, source)
    destination = tmp_path / "destination"
    bundle_script.restore(archive, destination)
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(destination))
    monkeypatch.setattr(GeofabrikRegions, "create_and_get_asset", lambda self: pytest.fail("Downloaded"))
    restored = GeofabrikRegions()
    assert not restored.is_update_needed()
    assert len(restored.get()) == 23


def test_bundle_rejects_incompatible_identity(tmp_path):
    archive = tmp_path / "bundle.zip"
    with ZipFile(archive, "w") as bundle:
        bundle.writestr("manifest.json", json.dumps({"format": 1, "inputs_hash": "wrong"}))
    with pytest.raises(ValueError, match="incompatible"):
        bundle_script.restore(archive, tmp_path / "destination")
