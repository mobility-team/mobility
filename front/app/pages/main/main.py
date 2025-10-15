from pathlib import Path
from dash import Dash, html, no_update
import dash_mantine_components as dmc
from dash.dependencies import Input, Output, State

from app.components.layout.header.header import Header
from app.components.features.map.map import Map, _deck_json
from app.components.layout.footer.footer import Footer
from app.components.features.study_area_summary import StudyAreaSummary
from app.scenario.scenario_001_from_docs import load_scenario


ASSETS_PATH = Path(__file__).resolve().parents[3] / "assets"
HEADER_HEIGHT = 60

MAPP = "map"  # doit matcher Map(id_prefix="map")

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
                        "position": "relative",
                        "width": "100%",
                        "height": "100%",
                        "background": "#fff",
                        "margin": "0",
                        "padding": "0",
                    },
                ),
                style={
                    "height": f"calc(100vh - {HEADER_HEIGHT}px)",
                    "padding": 0,
                    "margin": 0,
                    "overflow": "hidden",
                },
            ),
            Footer(),
        ],
        padding=0,
        styles={"main": {"padding": 0}},
    )
)

# ---- Callbacks (les contrôles sont dans la Map) ----

# Sync slider -> number (UX)
@app.callback(
    Output(f"{MAPP}-radius-input", "value"),
    Input(f"{MAPP}-radius-slider", "value"),
    State(f"{MAPP}-radius-input", "value"),
)
def _sync_input_from_slider(slider_val, current_input):
    if slider_val is None or slider_val == current_input:
        return no_update
    return slider_val

# Sync number -> slider (UX)
@app.callback(
    Output(f"{MAPP}-radius-slider", "value"),
    Input(f"{MAPP}-radius-input", "value"),
    State(f"{MAPP}-radius-slider", "value"),
)
def _sync_slider_from_input(input_val, current_slider):
    if input_val is None or input_val == current_slider:
        return no_update
    return input_val

# Lancer la simulation uniquement au clic
@app.callback(
    Output(f"{MAPP}-deck-map", "data"),
    Output(f"{MAPP}-summary-wrapper", "children"),
    Input(f"{MAPP}-run-btn", "n_clicks"),
    State(f"{MAPP}-radius-input", "value"),
    prevent_initial_call=True,
)
def _run_simulation(n_clicks, radius_val):
    r = radius_val if radius_val is not None else 40
    scn = load_scenario(radius=r)
    return _deck_json(scn), StudyAreaSummary(scn["zones_gdf"], visible=True, id_prefix=MAPP)


if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=False)
