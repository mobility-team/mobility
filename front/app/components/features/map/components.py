import dash_deck
from dash import html
import dash_mantine_components as dmc

from .config import HEADER_OFFSET_PX, SIDEBAR_WIDTH
from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.scenario_controls import ScenarioControlsPanel

from .tooltip import default_tooltip

def DeckMap(id_prefix: str, deck_json: str) -> dash_deck.DeckGL:
    return dash_deck.DeckGL(
        id=f"{id_prefix}-deck-map",
        data=deck_json,
        tooltip=default_tooltip(),
        mapboxKey="",
        style={
            "position": "absolute",
            "inset": 0,
            "height": "100vh",
            "width": "100%",
        },
    )

def SummaryPanelWrapper(zones_gdf, id_prefix: str):
    return html.Div(
        id=f"{id_prefix}-summary-wrapper",
        children=StudyAreaSummary(zones_gdf, visible=True, id_prefix=id_prefix),
    )

def ControlsSidebarWrapper(id_prefix: str):
     return html.Div(
        dmc.Paper(
            children=[
                dmc.Stack(
                    [
                        ScenarioControlsPanel(
                            id_prefix=id_prefix,
                            min_radius=15,
                            max_radius=50,
                            step=1,
                            default=40,
                            default_insee="31555",
                        )
                    ],
                    gap="md",
                )
            ],
            withBorder=True,
            shadow="md",
            radius="md",
            p="md",
            style={
                "width": "100%",
                "height": "100%",
                "overflowY": "auto",
                "overflowX": "hidden",
                "background": "#ffffffee",
                "boxSizing": "border-box",
            },
        ),
        id=f"{id_prefix}-controls-sidebar",
        style={
            "position": "absolute",
            "top": f"{HEADER_OFFSET_PX}px",
            "left": "0px",
            "bottom": "0px",
            "width": f"{SIDEBAR_WIDTH}px",
            "zIndex": 1200,
            "pointerEvents": "auto",
            "overflow": "hidden",
        },
    )
