"""
deck_factory.py
================

Fabrique d’objets Deck.gl (pydeck) pour l’affichage cartographique.

Ce module assemble :
- l’échelle de couleurs dérivée des temps moyens (`fit_color_scale`) ;
- la couche de zones (`build_zones_layer`) ;
- la vue et l’état initial (centre, zoom, pitch, bearing) ;
- la sérialisation JSON pour intégration dans l’UI Dash.

Fonctions principales
---------------------
- `make_layers(zones_gdf)`: construit la liste de couches pydeck.
- `make_deck(scn, opts)`: assemble un objet `pdk.Deck` prêt à l’affichage.
- `make_deck_json(scn, opts)`: renvoie la spécification Deck.gl au format JSON.
"""

import pydeck as pdk
import pandas as pd
import geopandas as gpd

from .config import FALLBACK_CENTER, DeckOptions
from .geo_utils import safe_center
from .color_scale import fit_color_scale
from .layers import build_zones_layer


def make_layers(zones_gdf: gpd.GeoDataFrame):
    """Construit les couches pydeck à partir des zones.

    Utilise `fit_color_scale` pour calibrer la palette sur la colonne
    `average_travel_time`, puis crée la couche polygonale via `build_zones_layer`.

    Args:
        zones_gdf (gpd.GeoDataFrame): GeoDataFrame des zones avec au moins
            la géométrie et, si possible, `average_travel_time`.

    Returns:
        List[pdk.Layer]: Liste des couches construites (vide si aucune géométrie valide).
    """
    # Palette "classique" (ton ancienne), la fonction fit_color_scale existante suffit
    scale = fit_color_scale(zones_gdf.get("average_travel_time", pd.Series(dtype="float64")))
    layers = []
    zl = build_zones_layer(zones_gdf, scale)
    if zl is not None:
        layers.append(zl)
    return layers


def make_deck(scn: dict, opts: DeckOptions) -> pdk.Deck:
    """Assemble un objet Deck.gl complet (couches + vue initiale).

    Détermine le centre de la vue à partir des géométries des zones (via
    `safe_center`) et retombe sur `FALLBACK_CENTER` si indisponible.

    Args:
        scn (dict): Scénario contenant `zones_gdf` (GeoDataFrame des zones).
        opts (DeckOptions): Options de vue et de style (zoom, pitch, bearing, map_style).

    Returns:
        pdk.Deck: Instance pydeck prête à être rendue.
    """
    zones_gdf: gpd.GeoDataFrame = scn["zones_gdf"].copy()

    layers = make_layers(zones_gdf)
    lon, lat = safe_center(zones_gdf) or FALLBACK_CENTER

    view_state = pdk.ViewState(
        longitude=lon,
        latitude=lat,
        zoom=opts.zoom,
        pitch=opts.pitch,
        bearing=opts.bearing,
    )

    return pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_provider="carto",
        map_style=opts.map_style,
        views=[pdk.View(type="MapView", controller=True)],
    )


def make_deck_json(scn: dict, opts: DeckOptions) -> str:
    """Sérialise la configuration Deck.gl en JSON.

    Pratique pour passer la spec au composant Dash `dash_deck.DeckGL`.

    Args:
        scn (dict): Scénario contenant `zones_gdf`.
        opts (DeckOptions): Options de vue/style utilisées par `make_deck`.

    Returns:
        str: Chaîne JSON représentant l’objet Deck.gl.
    """
    return make_deck(scn, opts).to_json()
