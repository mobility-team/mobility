import importlib
import json
from typing import List, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import dash
from dash import no_update
from dash.development.base_component import Component

import front.app.pages.main.main as main


def _output_pairs(outputs_obj) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []

  
    for out in outputs_obj:
        cid = getattr(out, "component_id", None)
        if cid is None and hasattr(out, "get"):
            cid = out.get("id")
        prop = getattr(out, "component_property", None)
        if prop is None and hasattr(out, "get"):
            prop = out.get("property")
        if cid is not None and prop is not None:
            pairs.append((cid, prop))
    return pairs


def _find_callback(captured, want_pairs: List[Tuple[str, str]]):
 
    want = set(want_pairs)
    for outputs_obj, _kwargs, func in captured:
        outs = set(_output_pairs(outputs_obj))
        if want.issubset(outs):
            return func
    raise AssertionError(f"Callback not found for outputs {want_pairs}")


def test_callbacks_via_decorator_capture(monkeypatch):
    captured = []  # list of tuples: (outputs_obj, kwargs, func)

    # Wrap Dash.callback to record every callback registration
    real_callback = dash.Dash.callback

    def recording_callback(self, *outputs, **kwargs):
        def decorator(func):
            captured.append((outputs, kwargs, func))
            return func  # important: return the original for Dash
        return decorator

    monkeypatch.setattr(dash.Dash, "callback", recording_callback, raising=True)

    # Reload module & build app (this registers all callbacks and gets captured)
    importlib.reload(main)
    app = main.create_app()

    # Restore the original callback (optional hygiene)
    monkeypatch.setattr(dash.Dash, "callback", real_callback, raising=True)

    # -------- find the 3 callbacks by their outputs --------
    cb_slider_to_input = _find_callback(
        captured, [(f"{main.MAPP}-radius-input", "value")]
    )
    cb_input_to_slider = _find_callback(
        captured, [(f"{main.MAPP}-radius-slider", "value")]
    )
    cb_run_sim = _find_callback(
        captured,
        [
            (f"{main.MAPP}-deck-map", "data"),
            (f"{main.MAPP}-summary-wrapper", "children"),
        ],
    )

    # -------- test sync callbacks --------
    # slider -> input: args order = [Inputs..., States...]
    assert cb_slider_to_input(40, 40) is no_update
    assert cb_slider_to_input(42, 40) == 42

    # input -> slider
    assert cb_input_to_slider(40, 40) is no_update
    assert cb_input_to_slider(38, 40) == 38

    # -------- success path for run simulation --------
    poly = Polygon([(1.43, 43.60), (1.45, 43.60), (1.45, 43.62), (1.43, 43.62), (1.43, 43.60)])
    zones_gdf = gpd.GeoDataFrame(
        {
            "transport_zone_id": ["Z1"],
            "local_admin_unit_id": ["31555"],
            "average_travel_time": [32.4],
            "total_dist_km": [7.8],
            "total_time_min": [45.0],
            "share_car": [0.52],
            "share_bicycle": [0.18],
            "share_walk": [0.30],
            "geometry": [poly],
        },
        crs="EPSG:4326",
    )
    flows_df = pd.DataFrame({"from": [], "to": [], "flow_volume": []})
    zones_lookup = zones_gdf[["transport_zone_id", "geometry"]].copy()

    def fake_get_scenario(radius=40, local_admin_unit_id="31555"):
        return {"zones_gdf": zones_gdf, "flows_df": flows_df, "zones_lookup": zones_lookup}

    monkeypatch.setattr("front.app.pages.main.main.get_scenario", fake_get_scenario, raising=True)

    def fake_make(scn):
        return json.dumps({"initialViewState": {}, "layers": []})
    monkeypatch.setattr("front.app.pages.main.main._make_deck_json_from_scn", fake_make, raising=True)

    deck_json, summary = cb_run_sim(1, 30, "31555")
    assert isinstance(deck_json, str)
    parsed = json.loads(deck_json)
    assert "initialViewState" in parsed and "layers" in parsed
    assert isinstance(summary, Component)
    props_id = summary.to_plotly_json().get("props", {}).get("id", "")
    assert props_id.endswith("-study-summary")

    # -------- error path for run simulation --------
    def boom_get_scenario(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr("front.app.pages.main.main.get_scenario", boom_get_scenario, raising=True)

    deck_json2, panel = cb_run_sim(1, 40, "31555")
    assert deck_json2 is no_update
    assert isinstance(panel, Component)
