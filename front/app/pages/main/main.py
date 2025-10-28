# app/pages/main/main.py
from pathlib import Path
import os

from dash import Dash, html, no_update
import dash_mantine_components as dmc
from dash.dependencies import Input, Output, State

from app.components.layout.header.header import Header
from app.components.features.map import Map
from app.components.layout.footer.footer import Footer
from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.map.config import DeckOptions
from front.app.services.scenario_service import get_scenario

# Utilise map_service si dispo : on lui passe le scénario construit
try:
    from front.app.services.map_service import get_map_deck_json_from_scn
    USE_MAP_SERVICE = True
except Exception:
    from app.components.features.map.deck_factory import make_deck_json
    USE_MAP_SERVICE = False

ASSETS_PATH = Path(__file__).resolve().parents[3] / "assets"
MAPP = "map"  # doit matcher Map(id_prefix="map")

def _make_deck_json_from_scn(scn: dict) -> str:
    if USE_MAP_SERVICE:
        return get_map_deck_json_from_scn(scn, DeckOptions())
    return make_deck_json(scn, DeckOptions())

def create_app() -> Dash:
    app = Dash(
        __name__,
        suppress_callback_exceptions=True,
        assets_folder=str(ASSETS_PATH),
        assets_url_path="/assets",
    )

    app.layout = dmc.MantineProvider(
        dmc.AppShell(
            children=[
                Header("MOBILITY"),

                dmc.AppShellMain(
                    html.Div(
                        Map(id_prefix=MAPP),
                        style={
                            "height": "100%",
                            "width": "100%",
                            "position": "relative",
                            "overflow": "hidden",
                            "margin": 0,
                            "padding": 0,
                        },
                    ),
                    style={
                        "flex": "1 1 auto",
                        "minHeight": 0,
                        "padding": 0,
                        "margin": 0,
                        "overflow": "hidden",
                    },
                ),

                html.Div(
                    Footer(),
                    style={
                        "flexShrink": "0",
                        "display": "flex",
                        "alignItems": "center",
                    },
                ),
            ],
            padding=0,
            styles={
                "root": {"height": "100vh", "overflow": "hidden"},
                "main": {"padding": 0, "margin": 0, "overflow": "hidden"},
            },
            style={"height": "100vh", "overflow": "hidden"},
        )
    )

    # --------- CALLBACKS ---------
    @app.callback(
        Output(f"{MAPP}-radius-input", "value"),
        Input(f"{MAPP}-radius-slider", "value"),
        State(f"{MAPP}-radius-input", "value"),
    )
    def _sync_input_from_slider(slider_val, current_input):
        if slider_val is None or slider_val == current_input:
            return no_update
        return slider_val

    @app.callback(
        Output(f"{MAPP}-radius-slider", "value"),
        Input(f"{MAPP}-radius-input", "value"),
        State(f"{MAPP}-radius-slider", "value"),
    )
    def _sync_slider_from_input(input_val, current_slider):
        if input_val is None or input_val == current_slider:
            return no_update
        return input_val

    @app.callback(
        Output(f"{MAPP}-deck-map", "data"),
        Output(f"{MAPP}-summary-wrapper", "children"),
        Input(f"{MAPP}-run-btn", "n_clicks"),
        State(f"{MAPP}-radius-input", "value"),
        State(f"{MAPP}-lau-input", "value"),
        prevent_initial_call=True,
    )
    def _run_simulation(n_clicks, radius_val, lau_val):
        r = 40 if radius_val is None else int(radius_val)
        lau = (lau_val or "").strip() or "31555"
        try:
            scn = get_scenario(radius=r, local_admin_unit_id=lau)
            deck_json = _make_deck_json_from_scn(scn)
            summary = StudyAreaSummary(scn["zones_gdf"], visible=True, id_prefix=MAPP)
            return deck_json, summary
        except Exception as e:
            err = dmc.Alert(
                f"Une erreur est survenue pendant la simulation : {e}",
                color="red",
                variant="filled",
                radius="md",
            )
            return no_update, err

    return app

# Exécution locale
app = create_app()

if __name__ == "__main__": #pragma: no cover
    port = int(os.environ.get("PORT", "8050"))
    app.run(debug=True, dev_tools_ui=False, port=port, host="127.0.0.1")
