"""
test_callbacks_simulation.py
============================

Tests de la logique de simulation associée au callback `_run_simulation`
sans recourir à Selenium / dash_duo.

L’objectif est de :
- monkeypatcher `get_scenario` pour obtenir un scénario stable et déterministe ;
- exécuter un helper (`compute_simulation_outputs_test`) qui reproduit la logique
  du callback (construction de `deck_json` + `StudyAreaSummary`) ;
- vérifier que la spécification Deck.gl produite est valide et que le composant
  résumé est bien un composant Dash sérialisable.
"""

import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from dash.development.base_component import Component

# On importe les briques "front" utilisées par le callback
import front.app.services.scenario_service as scn_mod
from app.components.features.map.config import DeckOptions
from app.components.features.map.deck_factory import make_deck_json
from app.components.features.study_area_summary import StudyAreaSummary

MAPP = "map"  # doit matcher l'id_prefix de la Map


def compute_simulation_outputs_test(radius_val, lau_val, id_prefix=MAPP):
    """Reproduit la logique du callback `_run_simulation` dans un contexte de test.

    Ce helper permet de tester la génération de la carte et du panneau de résumé
    sans démarrer le serveur Dash ni utiliser Selenium. Il :
      - normalise le rayon et le code INSEE/LAU ;
      - appelle `get_scenario` (qui peut être monkeypatché dans le test) ;
      - construit la spec Deck.gl JSON via `make_deck_json` ;
      - construit le composant `StudyAreaSummary`.

    Args:
        radius_val: Valeur du rayon (slider / input) telle que fournie par l’UI.
        lau_val: Code INSEE/LAU saisi (ex. "31555").
        id_prefix (str, optional): Préfixe d’identifiants pour le composant
            `StudyAreaSummary`. Doit être cohérent avec `MAPP`. Par défaut `"map"`.

    Returns:
        Tuple[str, Component]:  
            - `deck_json`: spécification Deck.gl sérialisée en JSON,  
            - `summary`: composant Dash `StudyAreaSummary`.
    """
    r = 40 if radius_val is None else int(radius_val)
    lau = (lau_val or "").strip() or "31555"
    scn = scn_mod.get_scenario(radius=r, local_admin_unit_id=lau)
    deck_json = make_deck_json(scn, DeckOptions())
    summary = StudyAreaSummary(scn["zones_gdf"], visible=True, id_prefix=id_prefix)
    return deck_json, summary


def test_compute_simulation_outputs_smoke(monkeypatch):
    # --- 1) scénario stable via monkeypatch ---
    poly = Polygon([
        (1.43, 43.60), (1.45, 43.60),
        (1.45, 43.62), (1.43, 43.62),
        (1.43, 43.60),
    ])

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
        return {
            "zones_gdf": zones_gdf,
            "flows_df": flows_df,
            "zones_lookup": zones_lookup,
        }

    monkeypatch.setattr(scn_mod, "get_scenario", fake_get_scenario, raising=True)

    # --- 2) exécute la logique "callback-like" ---
    deck_json, summary = compute_simulation_outputs_test(30, "31555", id_prefix=MAPP)

    # --- 3) assertions : deck_json DeckGL valide ---
    assert isinstance(deck_json, str)
    deck = json.loads(deck_json)
    assert "initialViewState" in deck
    assert isinstance(deck.get("layers", []), list)

    # --- 4) assertions : summary est un composant Dash sérialisable ---
    assert isinstance(summary, Component)
    payload = summary.to_plotly_json()
    assert isinstance(payload, dict)
    # On peut vérifier l'ID racine utilisé dans StudyAreaSummary
    assert payload.get("props", {}).get("id", "").endswith("-study-summary")
