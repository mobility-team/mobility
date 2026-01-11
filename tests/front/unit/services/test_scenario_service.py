# tests/front/unit/scenario_service/test_scenario_service.py
import sys
import types
import builtins

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyproj import CRS

import app.services.scenario_service as scn  # <-- adjust if your path differs


# ---------- helpers ----------

def _install_fake_mobility(monkeypatch, n=3):
    """
    Install a fake 'mobility' module in sys.modules to drive the non-fallback path.
    Creates n transport zones with simple point geometries in EPSG:4326.
    """
    class _FakeTZ:
        def __init__(self, local_admin_unit_id, radius, level_of_detail):
            self.lau = local_admin_unit_id
            self.radius = radius
            self.lod = level_of_detail

        def get(self):
            pts = [(1.0 + 0.01 * i, 43.0 + 0.01 * i) for i in range(n)]
            df = gpd.GeoDataFrame(
                {
                    "transport_zone_id": [f"Z{i+1}" for i in range(n)],
                    "geometry": [Point(x, y) for (x, y) in pts],
                    "local_admin_unit_id": [self.lau] * n,
                },
                geometry="geometry",
                crs=4326,
            )
            return df

    fake = types.ModuleType("mobility")
    fake.set_params = lambda **kwargs: None
    fake.TransportZones = _FakeTZ
    monkeypatch.setitem(sys.modules, "mobility", fake)


def _remove_mobility(monkeypatch):
    """Force mobility import to fail, so we exercise the fallback branch."""
    if "mobility" in sys.modules:
        monkeypatch.delitem(sys.modules, "mobility", raising=False)

    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "mobility":
            raise ImportError("Simulated missing mobility")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)


# ---------- tests ----------

def test_fallback_used_when_mobility_missing(monkeypatch):
    scn.clear_scenario_cache()
    _remove_mobility(monkeypatch)

    out = scn.get_scenario(local_admin_unit_id="31555", radius=40)
    zones = out["zones_gdf"]
    flows = out["flows_df"]
    lookup = out["zones_lookup"]

    # Shape/schema sanity
    assert isinstance(zones, gpd.GeoDataFrame)
    assert isinstance(flows, pd.DataFrame)
    assert isinstance(lookup, gpd.GeoDataFrame)
    assert zones.crs is not None and CRS(zones.crs).equals(CRS.from_epsg(4326))
    assert lookup.crs is not None and CRS(lookup.crs).equals(CRS.from_epsg(4326))

    # All modal columns should exist
    cols = {
        "share_car", "share_bicycle", "share_walk", "share_carpool",
        "share_pt_walk", "share_pt_car", "share_pt_bicycle", "share_public_transport",
        "average_travel_time", "total_dist_km", "local_admin_unit_id",
    }
    assert cols.issubset(set(zones.columns))

    # Shares well-formed (sum of all modal columns = 1)
    row_sums = zones[[
        "share_car", "share_bicycle", "share_walk", "share_carpool",
        "share_pt_walk", "share_pt_car", "share_pt_bicycle"
    ]].sum(axis=1)
    assert np.allclose(row_sums.values, 1.0, atol=1e-6)


def test_normalized_key_and_cache(monkeypatch):
    scn.clear_scenario_cache()
    _remove_mobility(monkeypatch)

    # _normalized_key behavior
    assert scn._normalized_key("31555", 40) == ("fr-31555", 40.0)
    assert scn._normalized_key("fr-31555", 40.00001) == ("fr-31555", 40.0)

    # Count compute calls via monkeypatched _compute_scenario
    calls = {"n": 0}

    def fake_compute(local_admin_unit_id, radius, transport_modes_params):
        calls["n"] += 1
        return scn._fallback_scenario()

    monkeypatch.setattr(scn, "_compute_scenario", fake_compute)

    # No params -> cached
    scn.get_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params=None)
    scn.get_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params=None)
    assert calls["n"] == 1, "Second call without params should hit cache"

    # With params -> bypass cache each time
    scn.get_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params={"car": {"active": True}})
    scn.get_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params={"car": {"active": True}})
    assert calls["n"] == 3, "Calls with params should call compute each time"


def test_non_fallback_renormalization_and_crs(monkeypatch):
    scn.clear_scenario_cache()
    _install_fake_mobility(monkeypatch, n=3)

    # Only bicycle active → its share should be 1, others 0 after renormalization
    params = {
        "car": {"active": False},
        "bicycle": {"active": True},
        "walk": {"active": False},
        "carpool": {"active": False},
        "public_transport": {"active": False},
    }
    out = scn.get_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params=params)
    zones = out["zones_gdf"]

    # CRS robust equality to WGS84
    assert zones.crs is not None
    assert CRS(zones.crs).equals(CRS.from_epsg(4326))

    # Row-wise sum over all share columns ≈ 1
    cols = [
        "share_car", "share_bicycle", "share_walk", "share_carpool",
        "share_pt_walk", "share_pt_car", "share_pt_bicycle"
    ]
    row_sums = zones[cols].sum(axis=1).to_numpy()
    assert np.allclose(row_sums, 1.0, atol=1e-5)

    # Bicycle should be 1 everywhere; others 0
    assert np.allclose(zones["share_bicycle"].to_numpy(), 1.0, atol=1e-6)
    others = zones[["share_car", "share_walk", "share_carpool",
                    "share_pt_walk", "share_pt_car", "share_pt_bicycle"]].to_numpy()
    assert np.allclose(others, 0.0, atol=1e-6)


def test_pt_submodes_selection(monkeypatch):
    scn.clear_scenario_cache()
    _install_fake_mobility(monkeypatch, n=4)

    # PT active but only 'car_pt' enabled
    params = {
        "car": {"active": False},
        "bicycle": {"active": False},
        "walk": {"active": False},
        "carpool": {"active": False},
        "public_transport": {"active": True, "pt_walk": False, "pt_bicycle": False, "pt_car": True},
    }
    out = scn.get_scenario(local_admin_unit_id="31555", radius=30.0, transport_modes_params=params)
    zones = out["zones_gdf"]

    # public_transport share == share_pt_car ; others zero
    assert np.all(zones["share_pt_walk"].values == 0.0)
    assert np.all(zones["share_pt_bicycle"].values == 0.0)
    assert np.all(zones["share_public_transport"].values == zones["share_pt_car"].values)

    # Total over active columns must be 1
    sums = zones[["share_pt_car"]].sum(axis=1)
    assert np.allclose(sums.values, 1.0, atol=1e-6)


def test_lau_normalization_variants(monkeypatch):
    scn.clear_scenario_cache()
    _install_fake_mobility(monkeypatch, n=2)

    out_a = scn.get_scenario(local_admin_unit_id="31555", radius=40.0, transport_modes_params=None)
    out_b = scn.get_scenario(local_admin_unit_id="fr-31555", radius=40.0, transport_modes_params=None)

    # Both normalize to ("fr-31555", 40.0) and use the same cached object
    assert out_a is out_b
