"""
main.py
=======

Point d’entrée de l’application Dash.

- Construit et configure l’UI globale (entête, carte, panneau de résumé, pied de page).
- Initialise l’état applicatif (Store pour la carte, options Deck.gl).
- Enregistre les callbacks via `register_callbacks`.
- Expose `app` et lance le serveur en exécution directe.

Notes:
    - Les assets statiques (CSS, images, etc.) sont servis depuis `ASSETS_PATH`.
    - Le préfixe d’identifiants de la carte est `MAPP = "map"`.
"""

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
    """Crée et configure l'application Dash principale.

    Assemble la structure de page avec Mantine AppShell :
      - `Header` : entête de l'application.
      - `dcc.Store` : état mémorisé pour la carte (`{key, lau}`).
      - `AppShellMain` : contenu principal avec la vue `Map`.
      - `Footer` : pied de page.
    Enregistre ensuite l'ensemble des callbacks via `register_callbacks`.

    Returns:
        Dash: Instance configurée de l'application Dash.
    """
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

    # <<< Enregistre tous les callbacks déplacés (navigation, interactions carte/UI, etc.)
    register_callbacks(app, MAPP=MAPP)

    return app


# Application globale (utile pour gunicorn / uvicorn)
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    # Lance le serveur de développement local.
    # PORT peut être surchargé via la variable d'environnement PORT.
    port = int(os.environ.get("PORT", "8050"))
    app.run(debug=True, dev_tools_ui=False, port=port, host="127.0.0.1")
