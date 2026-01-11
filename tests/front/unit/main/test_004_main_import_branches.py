# --- Service vs Fallback: API attendue par les tests ---
from app.components.features.map.config import DeckOptions
from app.components.features.map.deck_factory import make_deck_json as _fallback_make_deck_json

try:
    # IMPORTANT : chemin d'import attendu par les tests
    from front.app.services.map_service import get_map_deck_json_from_scn as _svc_get_map_deck_json_from_scn
    USE_MAP_SERVICE = True
except Exception:
    _svc_get_map_deck_json_from_scn = None
    USE_MAP_SERVICE = False

def _make_deck_json_from_scn(scn, opts: DeckOptions | None = None) -> str:
    """Garde une API stable pour tests_004_main_import_branches.py"""
    opts = opts or DeckOptions()
    if USE_MAP_SERVICE and _svc_get_map_deck_json_from_scn is not None:
        return _svc_get_map_deck_json_from_scn(scn, opts)
    return _fallback_make_deck_json(scn, opts)
