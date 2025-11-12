import pydeck as pdk
import pandas as pd
import geopandas as gpd

from .config import FALLBACK_CENTER, DeckOptions
from .geo_utils import safe_center
from .color_scale import fit_color_scale
from .layers import build_zones_layer

def make_layers(zones_gdf: gpd.GeoDataFrame):
    # Palette "classique" (ton ancienne), la fonction fit_color_scale existante suffit
    scale = fit_color_scale(zones_gdf.get("average_travel_time", pd.Series(dtype="float64")))
    layers = []
    zl = build_zones_layer(zones_gdf, scale)
    if zl is not None:
        layers.append(zl)
    return layers

def make_deck(scn: dict, opts: DeckOptions) -> pdk.Deck:
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
    return make_deck(scn, opts).to_json()
