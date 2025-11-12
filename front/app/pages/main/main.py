# main.py (extrait)
from pathlib import Path
import os
import uuid
from dash import Dash, html, no_update, dcc
from dash import Input, Output, State, ALL, ctx
import dash_mantine_components as dmc

from app.components.layout.header.header import Header
from app.components.features.map import Map
from app.components.layout.footer.footer import Footer
from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.map.config import DeckOptions
from app.services.scenario_service import get_scenario
from app.pages.main.callbacks import register_callbacks

ASSETS_PATH = Path(__file__).resolve().parents[3] / "assets"
MAPP = "map"

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
                dcc.Store(id=f"{MAPP}-deck-memo", data={"key": str(uuid.uuid4()), "lau": "fr-31555"}),
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
                html.Div(Footer(), style={"flexShrink": "0"}),
            ],
            padding=0,
            styles={
                "root": {"height": "100vh", "overflow": "hidden"},
                "main": {"padding": 0, "margin": 0, "overflow": "hidden"},
            },
        )
    )

    # <<< Enregistre tous les callbacks déplacés
    register_callbacks(app, MAPP=MAPP)

    return app

app = create_app()

if __name__ == "__main__":  # pragma: no cover
    port = int(os.environ.get("PORT", "8050"))
    app.run(debug=True, dev_tools_ui=False, port=port, host="127.0.0.1")
