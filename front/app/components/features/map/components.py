"""
layout.py
=========

Composants de haut niveau pour la page cartographique :
- `DeckMap` : rendu principal Deck.gl (fond de carte + couches)
- `SummaryPanelWrapper` : panneau latéral droit affichant le résumé d’étude
- `ControlsSidebarWrapper` : barre latérale gauche des contrôles de scénario

Ce module assemble des éléments d’UI (Dash + Mantine) et des composants
applicatifs (`StudyAreaSummary`, `ScenarioControlsPanel`) afin de proposer
une mise en page complète : carte plein écran, résumé latéral et sidebar.
"""

import dash_deck
from dash import html
import dash_mantine_components as dmc

from .config import HEADER_OFFSET_PX, SIDEBAR_WIDTH
from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.scenario_controls import ScenarioControlsPanel

from .tooltip import default_tooltip


def DeckMap(id_prefix: str, deck_json: str) -> dash_deck.DeckGL:
    """Crée le composant Deck.gl plein écran.

    Args:
        id_prefix (str): Préfixe utilisé pour l’identifiant Dash.
        deck_json (str): Spécification Deck.gl sérialisée (JSON) incluant
            carte de fond, couches, vues, etc.

    Returns:
        dash_deck.DeckGL: Composant Deck.gl prêt à l’affichage (pickable, tooltips).
    """
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
    """Enveloppe le panneau de résumé global à droite de la carte.

    Args:
        zones_gdf: GeoDataFrame (ou équivalent) contenant les colonnes utilisées
            par `StudyAreaSummary` (temps moyen, parts modales, etc.).
        id_prefix (str): Préfixe d’identifiant pour les composants liés à la carte.

    Returns:
        dash.html.Div: Conteneur du panneau de résumé (`StudyAreaSummary`).
    """
    return html.Div(
        id=f"{id_prefix}-summary-wrapper",
        children=StudyAreaSummary(zones_gdf, visible=True, id_prefix=id_prefix),
    )


def ControlsSidebarWrapper(id_prefix: str):
    """Construit la barre latérale gauche contenant les contrôles du scénario.

    La sidebar est positionnée sous l’en-tête principal (offset vertical défini
    par `HEADER_OFFSET_PX`) et utilise une largeur fixe `SIDEBAR_WIDTH`. Elle
    embarque le panneau `ScenarioControlsPanel` (rayon, zone INSEE, modes, bouton).

    Args:
        id_prefix (str): Préfixe d’identifiant pour éviter les collisions Dash.

    Returns:
        dash.html.Div: Conteneur sidebar avec un `dmc.Paper` et le panneau de contrôles.
    """
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
