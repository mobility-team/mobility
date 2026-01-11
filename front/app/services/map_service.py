"""
map_service.py
==============

Service d’intégration entre le backend de scénario (`get_scenario`) et
les composants carte (Deck.gl) du front.

Rôles principaux :
- Récupérer un scénario via `get_scenario()` ;
- Construire la spécification Deck.gl JSON via `make_deck_json` ;
- Exposer les données géographiques des zones pour la carte (`get_map_zones_gdf`).

Un point d’extension `_scenario_snapshot_key()` est prévu pour, à terme,
brancher une logique de versionnement ou d’horodatage des scénarios et
affiner le cache si nécessaire.
"""

from __future__ import annotations
from functools import lru_cache

from front.app.services.scenario_service import get_scenario
from app.components.features.map.config import DeckOptions
from app.components.features.map.deck_factory import make_deck_json


@lru_cache(maxsize=8)
def _scenario_snapshot_key() -> int:
    """Clé de cache grossière pour un futur versionnement des scénarios.

    Pour l’instant, renvoie toujours `0`, ce qui revient à ne pas exploiter
    finement le cache. Si `get_scenario()` expose un identifiant de version,
    un hash ou un horodatage, on pourra l’utiliser ici pour invalider ou
    différencier les résultats en fonction de l’évolution des données.

    Returns:
        int: Identifiant de snapshot de scénario (actuellement toujours `0`).
    """
    return 0


def get_map_deck_json_from_scn(scn: dict, opts: DeckOptions | None = None) -> str:
    """Construit la spec Deck.gl JSON à partir d’un scénario déjà calculé.

    Ce helper est utile lorsque le scénario `scn` a été obtenu en amont
    (par exemple dans un service ou un callback) et que l’on souhaite
    simplement générer la configuration de carte correspondante.

    Args:
        scn (dict): Scénario contenant au minimum `zones_gdf`.
        opts (DeckOptions | None, optional): Options d’affichage de la carte
            (zoom, pitch, style, etc.). Si `None`, utilise `DeckOptions()`.

    Returns:
        str: Spécification Deck.gl sérialisée au format JSON.
    """
    opts = opts or DeckOptions()
    return make_deck_json(scn, opts)


def get_map_deck_json(id_prefix: str, opts: DeckOptions) -> str:
    """Construit la spec Deck.gl JSON en récupérant un scénario via `get_scenario()`.

    Le paramètre `id_prefix` est présent pour homogénéité avec d’autres couches
    de l’application, mais n’est pas utilisé directement ici. À terme, il pourrait
    servir si la config de carte dépend de plusieurs instances ou contextes.

    Args:
        id_prefix (str): Préfixe d’identifiants lié à la carte (non utilisé ici).
        opts (DeckOptions): Options d’affichage Deck.gl (zoom, pitch, style, etc.).

    Returns:
        str: Spécification Deck.gl sérialisée au format JSON.
    """
    # Éventuellement invalider le cache selon _scenario_snapshot_key() plus tard.
    scn = get_scenario()
    return make_deck_json(scn, opts)


def get_map_zones_gdf():
    """Retourne le GeoDataFrame des zones issu du scénario courant.

    Récupère un scénario via `get_scenario()` et renvoie le champ `zones_gdf`,
    utilisé comme base pour les couches cartographiques et les résumés.

    Returns:
        geopandas.GeoDataFrame: Données géographiques des zones d’étude.
    """
    scn = get_scenario()
    return scn["zones_gdf"]
