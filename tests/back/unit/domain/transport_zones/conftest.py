import pathlib

import pytest

import pandas as pd
import geopandas as gpd


# -----------------------------------------------------------
# Patch the exact FileAsset used by mobility.spatial.transport_zones
# -----------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_transportzones_fileasset_init(monkeypatch):
    """
    Patch the *exact* FileAsset class used by mobility.spatial.transport_zones at import time.
    This avoids guessing module paths or MRO tricks and guarantees interception.

    Behavior:
      - never calls .get()
      - sets .inputs, .cache_path, .hash_path, .value
      - exposes inputs as attributes on self
    """
    import mobility.spatial.transport_zones as tz_module

    # Grab the class object that TransportZones actually extends
    FileAssetClass = tz_module.FileAsset

    def fake_init(self, *args, **kwargs):
        # TransportZones uses super().__init__(inputs, cache_path)
        if len(args) >= 2:
            inputs, cache_path = args[0], args[1]
        elif "inputs" in kwargs and "cache_path" in kwargs:
            inputs, cache_path = kwargs["inputs"], kwargs["cache_path"]
        else:
            raise AssertionError(
                f"Unexpected FileAsset.__init__ signature in tests: args={args}, kwargs={kwargs}"
            )

        if isinstance(cache_path, dict):
            cache_path = cache_path["polygons"]

        cache_path = pathlib.Path(cache_path)

        # Set required attributes
        self.inputs = inputs
        self.cache_path = cache_path
        self.hash_path = cache_path
        self.value = None

        # Surface inputs as attributes on self (e.g., self.study_area, self.osm_buildings)
        if isinstance(self.inputs, dict):
            for key, value in self.inputs.items():
                setattr(self, key, value)

    # Monkeypatch the *exact* class used by TransportZones
    monkeypatch.setattr(FileAssetClass, "__init__", fake_init, raising=True)


# ----------------------------------------------------------
# Fake minimal geodataframe for transport zones expectations
# ----------------------------------------------------------

@pytest.fixture
def fake_transport_zones():
    """
    Minimal GeoDataFrame with columns typical code would expect.
    Geometry can be None for simplicity; CRS added for consistency.

    Important: include 'local_admin_unit_id' and numeric 'x','y'
    so flag_inner_zones() and any x/y-based logic can run.
    """
    df = pd.DataFrame(
        {
            "transport_zone_id": [1, 2],
            "urban_unit_category": ["core", "peripheral"],
            "local_admin_unit_id": ["ch-6621", "ch-6621"],  # required by flag_inner_zones
            "x": [0.0, 1.0],  # required by downstream selection logic
            "y": [0.0, 1.0],  # required by downstream selection logic
            "geometry": [None, None],
        }
    )
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return gdf


