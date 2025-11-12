# tests/front/unit/main/test_main_callbacks_sync_via_decorator_capture.py
import importlib
import json
from typing import List, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import dash
from dash import no_update
from dash.development.base_component import Component
from dash.dependencies import ALL, MATCH, ALLSMALLER  # pour reconnaître les wildcards

import front.app.pages.main.main as main


# --- helpers: rendre les IDs hashables et stables (support des wildcards) ---
def _canon(value):
    """Transforme value en représentation immuable/hashable/stable.
    - Gère les wildcards Dash (ALL/MATCH/ALLSMALLER)
    - Gère dict/list/tuple récursivement
    - Évite toute sérialisation JSON (qui casserait avec _Wildcard)
    """
    if value is ALL:
        return ("<ALL>",)
    if value is MATCH:
        return ("<MATCH>",)
    if value is ALLSMALLER:
        return ("<ALLSMALLER>",)

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    if isinstance(value, dict):
        # Trier par clé pour stabilité
        return tuple(sorted((str(k), _canon(v)) for k, v in value.items()))

    if isinstance(value, (list, tuple)):
        return tuple(_canon(v) for v in value)

    # Fallback : représentation texte stable
    return ("<REPR>", repr(value))


def _canon_id(cid):
    return _canon(cid)


def _output_pairs(outputs_obj) -> List[Tuple[object, str]]:
    pairs: List[Tuple[object, str]] = []
    for out in outputs_obj:
        cid = getattr(out, "component_id", None)
        if cid is None and hasattr(out, "get"):
            cid = out.get("id")
        prop = getattr(out, "component_property", None)
        if prop is None and hasattr(out, "get"):
            prop = out.get("property")
        if cid is not None and prop is not None:
            pairs.append((_canon_id(cid), prop))
    return pairs


def _find_callback(captured, want_pairs: List[Tuple[object, str]]):
    want = set((_canon_id(cid), prop) for cid, prop in want_pairs)
    for outputs_obj, _kwargs, func in captured:
        outs = set(_output_pairs(outputs_obj))
        if want.issubset(outs):
            return func
    raise AssertionError(f"Callback not found for outputs {want_pairs}")


def test_callbacks_via_decorator_capture(monkeypatch):
    captured = []  # list of tuples: (outputs_obj, kwargs, func)

    # 1) Enveloppe Dash.callback pour capturer les enregistrements
    real_callback = dash.Dash.callback

    def recording_callback(self, *outputs, **kwargs):
        def decorator(func):
            captured.append((outputs, kwargs, func))
            return func
        return decorator

    monkeypatch.setattr(dash.Dash, "callback", recording_callback, raising=True)

    # 2) Reload du module et build de l'app (les callbacks sont enregistrés et capturés)
    importlib.reload(main)
    app = main.create_app()

    # 3) Restaure le callback d'origine (hygiène)
    monkeypatch.setattr(dash.Dash, "callback", real_callback, raising=True)

    # -------- retrouver les 3 callbacks par leurs sorties --------
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
            (f"{main.MAPP}-deck-map", "key"),
            (f"{main.MAPP}-summary-wrapper", "children"),
            (f"{main.MAPP}-deck-memo", "data"),
        ],
    )

    # -------- tests des callbacks de synchro --------
    assert cb_slider_to_input(40, 40) is no_update
    assert cb_slider_to_input(42, 40) == 42

    assert cb_input_to_slider(40, 40) is no_update
    assert cb_input_to_slider(38, 40) == 38

    # -------- chemin "success" du run simulation --------
    poly = Polygon([(1.43, 43.60), (1.45, 43.60), (1.45, 43.62), (1.43, 43.62), (1.43, 43.60)])
    zones_gdf = gpd.GeoDataFrame(
        {
            "transport_zone_id": ["Z1"],
            "local_admin_unit_id": ["fr-31555"],  # normalisé comme dans l'app
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

    # 👉 Monkeypatch directement les globals du callback enregistré
    def fake_get_scenario(radius=40.0, local_admin_unit_id="fr-31555", transport_modes_params=None):
        lau = (local_admin_unit_id or "").lower()
        if lau == "31555":
            lau = "fr-31555"
        assert isinstance(radius, (int, float))
        return {"zones_gdf": zones_gdf, "flows_df": flows_df, "zones_lookup": zones_lookup}

    def fake_make_deck(scn):
        return json.dumps({"initialViewState": {}, "layers": []})

    monkeypatch.setitem(cb_run_sim.__globals__, "get_scenario", fake_get_scenario)
    monkeypatch.setitem(cb_run_sim.__globals__, "_make_deck_json_from_scn", fake_make_deck)

    # Ordre des args = [Inputs..., States...]
    n_clicks = 1
    radius_val = 30
    lau_val = "31555"
    active_values = []
    active_ids = []
    vars_values = []
    vars_ids = []
    pt_checked_vals = []
    pt_checked_ids = []
    deck_memo = {"key": "keep-me", "lau": "fr-31555"}  # même LAU => key conservée

    deck_json, key, summary, memo = cb_run_sim(
        n_clicks,
        radius_val,
        lau_val,
        active_values,
        active_ids,
        vars_values,
        vars_ids,
        pt_checked_vals,
        pt_checked_ids,
        deck_memo,
    )

    assert isinstance(deck_json, str)
    parsed = json.loads(deck_json)
    assert "initialViewState" in parsed and "layers" in parsed

    assert key == "keep-me"  # caméra conservée si LAU identique
    assert isinstance(summary, Component)
    props_id = summary.to_plotly_json().get("props", {}).get("id", "")
    assert props_id.endswith("-study-summary")
    assert isinstance(memo, dict)
    assert memo.get("lau") == "fr-31555"

    # -------- chemin "erreur" du run simulation --------
    def boom_get_scenario(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setitem(cb_run_sim.__globals__, "get_scenario", boom_get_scenario)

    deck_json2, key2, panel, memo2 = cb_run_sim(
        n_clicks,
        radius_val,
        lau_val,
        active_values,
        active_ids,
        vars_values,
        vars_ids,
        pt_checked_vals,
        pt_checked_ids,
        deck_memo,
    )

    assert deck_json2 is no_update
    assert key2 is no_update
    assert isinstance(panel, Component)  # l'Alert Mantine
    assert memo2 is no_update
