import pydeck as pdk
import pandas as pd
import geopandas as gpd

from .config import FALLBACK_CENTER, DeckOptions
from .geo_utils import safe_center
from .color_scale import fit_color_scale
from .layers import build_zones_layer, build_flows_layer

def make_layers(zones_gdf: gpd.GeoDataFrame, flows_df: pd.DataFrame, zones_lookup: gpd.GeoDataFrame):
    scale = fit_color_scale(zones_gdf.get("average_travel_time", pd.Series(dtype="float64")))
    layers = []
    zl = build_zones_layer(zones_gdf, scale)
    if zl is not None:
        layers.append(zl)
    fl = build_flows_layer(flows_df, zones_lookup)
    if fl is not None:
        layers.append(fl)
    return layers

def make_deck(scn: dict, opts: DeckOptions) -> pdk.Deck:
    zones_gdf: gpd.GeoDataFrame = scn["zones_gdf"].copy()
    flows_df: pd.DataFrame = scn["flows_df"].copy()
    zones_lookup: gpd.GeoDataFrame = scn["zones_lookup"].copy()

    layers = make_layers(zones_gdf, flows_df, zones_lookup)
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
