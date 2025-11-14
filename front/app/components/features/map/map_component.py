"""
map.py
======

Assemblage de la page cartographique (Deck.gl + panneaux latéraux).

- **Option A (service)** : si `app.services.map_service` est disponible,
  on récupère la spécification Deck.gl JSON et les zones via
  `get_map_deck_json` / `get_map_zones_gdf`.
- **Option B (fallback)** : sinon, on calcule localement la carte à partir
  d’un scénario (`scenario_service.get_scenario`) et de la fabrique Deck (`make_deck_json`).

Composants intégrés :
- `DeckMap` : rendu Deck.gl plein écran
- `SummaryPanelWrapper` : panneau de résumé (droite)
- `ControlsSidebarWrapper` : barre de contrôles (gauche)
"""

from dash import html
from .config import DeckOptions
from .components import DeckMap, ControlsSidebarWrapper, SummaryPanelWrapper

# — Option A : via map_service s’il existe
try:
    from app.services.map_service import get_map_deck_json, get_map_zones_gdf
    _USE_SERVICE = True
except Exception:
    _USE_SERVICE = False

# — Option B : fallback direct si map_service absent
if not _USE_SERVICE:
    from app.services.scenario_service import get_scenario
    from .deck_factory import make_deck_json

    def get_map_deck_json(id_prefix: str, opts: DeckOptions) -> str:
        """Construit la spec Deck.gl au format JSON à partir d’un scénario local.

        Args:
            id_prefix (str): Préfixe d’identifiant (réservé pour compat).
            opts (DeckOptions): Options d’affichage (zoom, pitch, style, etc.).

        Returns:
            str: Spécification Deck.gl sérialisée (JSON).
        """
        scn = get_scenario()
        return make_deck_json(scn, opts)

    def get_map_zones_gdf():
        """Récupère les zones du scénario local (fallback)."""
        scn = get_scenario()
        return scn["zones_gdf"]


def Map(id_prefix: str = "map"):
    """Assemble la vue cartographique : carte, résumé et sidebar de contrôles.

    Le rendu s’appuie sur `DeckOptions()` pour initialiser l’état de la vue,
    puis crée :
      - le composant Deck.gl (`DeckMap`) avec la spec JSON,
      - le panneau de résumé (`SummaryPanelWrapper`) à droite,
      - la barre de contrôles (`ControlsSidebarWrapper`) à gauche.

    Le layout final est un conteneur `Div` en position relative, sur toute
    la hauteur de la fenêtre.

    Args:
        id_prefix (str, optional): Préfixe d’identifiants pour les composants
            associés à la carte. Par défaut `"map"`.

    Returns:
        dash.html.Div: Conteneur principal de la page cartographique.
    """
    opts = DeckOptions()
    deck_json = get_map_deck_json(id_prefix=id_prefix, opts=opts)
    zones_gdf = get_map_zones_gdf()

    deckgl = DeckMap(id_prefix=id_prefix, deck_json=deck_json)
    summary = SummaryPanelWrapper(zones_gdf, id_prefix=id_prefix)
    controls_sidebar = ControlsSidebarWrapper(id_prefix=id_prefix)

    return html.Div(
        [deckgl, summary, controls_sidebar],
        style={
            "position": "relative",
            "width": "100%",
            "height": "100vh",
            "background": "#fff",
            "overflow": "hidden",
        },
    )
