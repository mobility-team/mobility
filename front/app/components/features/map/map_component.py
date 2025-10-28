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
        scn = get_scenario()
        return make_deck_json(scn, opts)
    def get_map_zones_gdf():
        scn = get_scenario()
        return scn["zones_gdf"]

def Map(id_prefix: str = "map"):
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
