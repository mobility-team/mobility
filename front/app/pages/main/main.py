# app/pages/main/main.py
from pathlib import Path
import os

from dash import Dash, html, no_update, Input, Output, State, ALL, ctx
import dash_mantine_components as dmc

from app.components.layout.header.header import Header
from app.components.features.map import Map
from app.components.layout.footer.footer import Footer
from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.map.config import DeckOptions
from app.services.scenario_service import get_scenario

# Utilise map_service si dispo
try:
    from app.services.map_service import get_map_deck_json_from_scn
    USE_MAP_SERVICE = True
except Exception:
    from app.components.features.map.deck_factory import make_deck_json
    USE_MAP_SERVICE = False

ASSETS_PATH = Path(__file__).resolve().parents[3] / "assets"
MAPP = "map"   # id_prefix pour la carte
TM = "tm"      # id_prefix pour les modes de transport

# Conversion UI -> noms internes du service
UI_TO_INTERNAL = {
    "À pied": "walk",
    "A pied": "walk",
    "Vélo": "bicycle",
    "Voiture": "car",
    "Covoiturage": "carpool", 
}


def _make_deck_json_from_scn(scn: dict) -> str:
    if USE_MAP_SERVICE:
        return get_map_deck_json_from_scn(scn, DeckOptions())
    return make_deck_json(scn, DeckOptions())


# ---------------------------------------------------------------------
# Application Dash
# ---------------------------------------------------------------------
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

    # -----------------------------------------------------------------
    # CALLBACKS
    # -----------------------------------------------------------------

    # Synchronisation slider / input
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

    # -----------------------------------------------------------------
    # CALLBACK : Empêche de décocher tous les modes (sans notification)
    # -----------------------------------------------------------------
    @app.callback(
        Output({'type': 'mode-active', 'index': ALL}, 'checked'),
        Input({'type': 'mode-active', 'index': ALL}, 'checked'),
        State({'type': 'mode-active', 'index': ALL}, 'id'),
        prevent_initial_call=True,
    )
    def ensure_one_mode_checked(values, ids):
        """
        Garantit qu'au moins un mode reste coché.
        Si tous passent à False, on réactive automatiquement celui qui vient d'être décoché (ou le premier).
        """
        if not values or not ids:
            return no_update

        if any(values):
            return values

        new_values = list(values)
        triggered = ctx.triggered_id

        if isinstance(triggered, dict) and "index" in triggered:
            triggered_name = triggered["index"]
            for i, id_ in enumerate(ids):
                if id_["index"] == triggered_name:
                    new_values[i] = True
                    break
            else:
                new_values[0] = True
        else:
            new_values[0] = True

        return new_values

    # -----------------------------------------------------------------
    # Simulation principale
    # -----------------------------------------------------------------
    @app.callback(
        Output(f"{MAPP}-deck-map", "data"),
        Output(f"{MAPP}-summary-wrapper", "children"),
        Input(f"{MAPP}-run-btn", "n_clicks"),
        State(f"{MAPP}-radius-input", "value"),
        State(f"{MAPP}-lau-input", "value"),
        State({"type": "mode-active", "index": ALL}, "checked"),
        State({"type": "mode-active", "index": ALL}, "id"),
        State({"type": "mode-var", "mode": ALL, "var": ALL}, "value"),
        State({"type": "mode-var", "mode": ALL, "var": ALL}, "id"),
        prevent_initial_call=True,
    )
    def _run_simulation(
        n_clicks,
        radius_val,
        lau_val,
        active_values,
        active_ids,
        vars_values,
        vars_ids,
    ):
        """Callback : exécute la simulation avec les paramètres du formulaire."""
        try:
            r = 40 if radius_val is None else float(radius_val)
            lau = (lau_val or "").strip() or "31555"

            # Reconstitue un dictionnaire transport_modes_params
            params = {}
            # 1. checkboxes (active/inactive)
            for aid, val in zip(active_ids, active_values):
                mode_label = aid["index"]
                internal_key = UI_TO_INTERNAL.get(mode_label)
                if internal_key:
                    params.setdefault(internal_key, {})["active"] = bool(val)

            # 2. variables numériques
            for vid, val in zip(vars_ids, vars_values):
                mode_label = vid["mode"]
                var_label = vid["var"]
                internal_key = UI_TO_INTERNAL.get(mode_label)
                if not internal_key:
                    continue
                p = params.setdefault(internal_key, {"active": True})
                if "temps" in var_label.lower():
                    p["cost_of_time_eur_per_h"] = float(val or 0)
                elif "distance" in var_label.lower():
                    p["cost_of_distance_eur_per_km"] = float(val or 0)
                elif "constante" in var_label.lower():
                    p["cost_constant"] = float(val or 0)

            # Appel au service
            scn = get_scenario(
                local_admin_unit_id=lau,
                radius=r,
                transport_modes_params=params,
            )
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


# ---------------------------------------------------------------------
# Exécution locale
# ---------------------------------------------------------------------
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    port = int(os.environ.get("PORT", "8050"))
    app.run(debug=True, dev_tools_ui=False, port=port, host="127.0.0.1")
