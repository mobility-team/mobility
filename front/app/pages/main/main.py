# app/pages/main/main.py
from pathlib import Path
import os

from dash import Dash, html, no_update, Input, Output, State, ALL
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

    # --- Simulation principale (tolérante et typée pour React) ---
    @app.callback(
        Output(f"{MAPP}-deck-map", "data"),
        Output(f"{MAPP}-summary-wrapper", "children"),
        Input(f"{MAPP}-run-btn", "n_clicks"),
        State(f"{MAPP}-radius-input", "value"),
        State(f"{MAPP}-lau-input", "value"),
        # pattern-matching states (peuvent être vides si accordéons repliés)
        State({"type": "mode-active", "index": ALL}, "checked"),
        State({"type": "mode-active", "index": ALL}, "id"),
        State({"type": "mode-var", "mode": ALL, "var": ALL}, "value"),
        State({"type": "mode-var", "mode": ALL, "var": ALL}, "id"),
        # état courant (pour fallback sans rien casser)
        State(f"{MAPP}-deck-map", "data"),
        State(f"{MAPP}-summary-wrapper", "children"),
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
        prev_deck,
        prev_summary,
    ):
        """
        Exécute la simulation avec les paramètres du formulaire.
        - Convertit tout 'children' en liste de composants Dash.
        - En cas d'erreur, réaffiche l'ancien panneau + une alerte.
        """

        # Helpers internes
        def _as_children(obj):
            """Force un retour children en liste de composants Dash/HTML."""
            if obj is None:
                return []
            if isinstance(obj, list):
                return obj
            if isinstance(obj, tuple):
                return list(obj)
            # Un seul composant -> encapsule
            return [obj]

        def _build_params(active_values, active_ids, vars_values, vars_ids):
            """Reconstruit transport_modes_params en tolérant les listes vides/mal typées."""
            # Defaults si rien n’est monté
            params = {
                "walk":    {"active": True, "cost_of_time_eur_per_h": 12.0, "cost_of_distance_eur_per_km": 0.01, "cost_constant": 1.0},
                "bicycle": {"active": True, "cost_of_time_eur_per_h": 12.0, "cost_of_distance_eur_per_km": 0.01, "cost_constant": 1.0},
                "car":     {"active": True, "cost_of_time_eur_per_h": 12.0, "cost_of_distance_eur_per_km": 0.01, "cost_constant": 1.0},
            }

            active_values = active_values or []
            active_ids    = active_ids or []
            vars_values   = vars_values or []
            vars_ids      = vars_ids or []

            # Checkboxes
            n = min(len(active_ids), len(active_values))
            for i in range(n):
                aid = active_ids[i]
                checked = bool(active_values[i])
                if not isinstance(aid, dict):
                    continue
                ui_label = aid.get("index")
                internal = UI_TO_INTERNAL.get(ui_label or "")
                if internal:
                    params[internal]["active"] = checked

            # Inputs numériques
            m = min(len(vars_ids), len(vars_values))
            for i in range(m):
                vid = vars_ids[i]
                val = vars_values[i]
                if not isinstance(vid, dict):
                    continue
                ui_mode = vid.get("mode")
                var_lbl = (vid.get("var") or "").lower()
                internal = UI_TO_INTERNAL.get(ui_mode or "")
                if not internal:
                    continue
                try:
                    fval = float(val) if val is not None else 0.0
                except Exception:
                    fval = 0.0
                if "temps" in var_lbl:
                    params[internal]["cost_of_time_eur_per_h"] = fval
                elif "distance" in var_lbl:
                    params[internal]["cost_of_distance_eur_per_km"] = fval
                elif "constante" in var_lbl:
                    params[internal]["cost_constant"] = fval

            return params

        try:
            r = 40.0 if radius_val is None else float(radius_val)
            lau = (lau_val or "").strip() or "31555"

            params = _build_params(active_values, active_ids, vars_values, vars_ids)

            scn = get_scenario(
                local_admin_unit_id=lau,
                radius=r,
                transport_modes_params=params,
            )
            deck_json = _make_deck_json_from_scn(scn)

            # Toujours renvoyer une LISTE de composants pour children
            summary_comp = StudyAreaSummary(scn["zones_gdf"], visible=True, id_prefix=MAPP)
            summary_children = _as_children(summary_comp)

            return deck_json, summary_children

        except Exception as e:
            # On garde l'état précédent et on affiche une alerte AU-DESSUS,
            # le tout converti en liste de composants Dash.
            alert = dmc.Alert(
                f"Une erreur est survenue pendant la simulation : {e}",
                color="red",
                variant="filled",
                radius="md",
                my="sm",
            )
            prev_children = _as_children(prev_summary)
            return prev_deck, [alert, *prev_children] if prev_children else [alert]

    return app


# ---------------------------------------------------------------------
# Exécution locale
# ---------------------------------------------------------------------
app = create_app()

if __name__ == "__main__":  # pragma: no cover
    port = int(os.environ.get("PORT", "8050"))
    app.run(debug=True, dev_tools_ui=False, port=port, host="127.0.0.1")
