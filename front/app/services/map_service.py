from __future__ import annotations
from functools import lru_cache

from front.app.services.scenario_service import get_scenario
from app.components.features.map.config import DeckOptions
from app.components.features.map.deck_factory import make_deck_json

@lru_cache(maxsize=8)
def _scenario_snapshot_key() -> int:
    """
    Clé de cache grossière : on peut brancher ici une version/horodatage de scénario
    si `get_scenario()` l’expose ; sinon on renvoie 0 pour désactiver le cache fin.
    """
    return 0

def get_map_deck_json(id_prefix: str, opts: DeckOptions) -> str:
    # éventuellement invalider le cache selon _scenario_snapshot_key()
    scn = get_scenario()
    return make_deck_json(scn, opts)

def get_map_zones_gdf():
    scn = get_scenario()
    return scn["zones_gdf"]
