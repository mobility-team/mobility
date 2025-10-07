import pydeck as pdk
import dash_deck
from dash import html

CARTO_POSITRON_GL = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

def _deck_json():
    points = [
        {"lon": 2.3522, "lat": 48.8566, "name": "Paris", "color": [255, 140, 0]},
        {"lon": 4.8357, "lat": 45.7640, "name": "Lyon",  "color": [0, 200, 255]},
    ]

    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=points,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius=12000,
        pickable=True,
    )

    # Static camera over France (moderate perspective)
    view_state = pdk.ViewState(
        longitude=2.2,
        latitude=46.5,
        zoom=5.8,
        pitch=35,     # change if you want flatter/steeper
        bearing=-15,
    )

    deck = pdk.Deck(
        layers=[scatter],
        initial_view_state=view_state,
        views=[pdk.View(type="MapView", controller=True)],
        map_style=CARTO_POSITRON_GL,   
        tooltip={"text": "{name}"},
    )
    return deck.to_json()

def Map():
    deckgl = dash_deck.DeckGL(
        id="deck-map",
        data=_deck_json(),
        tooltip=True,
        style={"position": "absolute", "inset": 0},
    )

    return html.Div(
        deckgl,
        style={
            "position": "relative",
            "width": "100%",
            "height": "100%",
            "background": "#fff",
        },
    )
