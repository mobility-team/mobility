# app/callbacks.py
from dash import Input, Output, State, ALL, no_update, ctx
import uuid
import dash_mantine_components as dmc

from app.components.features.study_area_summary import StudyAreaSummary
from app.components.features.map.config import DeckOptions
from app.services.scenario_service import get_scenario

# Utilise map_service si dispo (même logique que dans ton code)
try:
    from app.services.map_service import get_map_deck_json_from_scn
    USE_MAP_SERVICE = True
except Exception:
    from app.components.features.map.deck_factory import make_deck_json
    USE_MAP_SERVICE = False

UI_TO_INTERNAL = {
    "À pied": "walk",
    "A pied": "walk",
    "Vélo": "bicycle",
    "Voiture": "car",
    "Covoiturage": "carpool",
    "Transport en commun": "public_transport",
}

def _normalize_lau(code: str) -> str:
    s = (code or "").strip().lower()
    if s.startswith("fr-"):
        return s
    if s.isdigit() and len(s) == 5:
        return f"fr-{s}"
    return s or "fr-31555"

def _make_deck_json_from_scn(scn: dict) -> str:
    if USE_MAP_SERVICE:
        return get_map_deck_json_from_scn(scn, DeckOptions())
    return make_deck_json(scn, DeckOptions())


def register_callbacks(app, MAPP: str = "map"):
    # -------------------- CALLBACKS --------------------

    # Synchronise slider <-> input
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

    # Lancer la simulation — forcer le refresh du deck
    # MAIS on conserve la caméra si le LAU n'a pas changé: on ne change pas la "key"
    @app.callback(
        Output(f"{MAPP}-deck-map", "data"),
        Output(f"{MAPP}-deck-map", "key"),
        Output(f"{MAPP}-summary-wrapper", "children"),
        Output(f"{MAPP}-deck-memo", "data"),
        Input(f"{MAPP}-run-btn", "n_clicks"),
        State(f"{MAPP}-radius-input", "value"),
        State(f"{MAPP}-lau-input", "value"),
        State({"type": "mode-active", "index": ALL}, "checked"),
        State({"type": "mode-active", "index": ALL}, "id"),
        State({"type": "mode-var", "mode": ALL, "var": ALL}, "value"),
        State({"type": "mode-var", "mode": ALL, "var": ALL}, "id"),
        State({"type": "pt-submode", "index": ALL}, "checked"),
        State({"type": "pt-submode", "index": ALL}, "id"),
        State(f"{MAPP}-deck-memo", "data"),   # mémo préalable
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
        pt_checked_vals,
        pt_checked_ids,
        deck_memo,
    ):
        try:
            r = 40.0 if radius_val is None else float(radius_val)
            lau_norm = _normalize_lau(lau_val or "31555")

            # Reconstruire transport_modes_params depuis l'UI
            params = {}

            # Actifs/inactifs
            for aid, val in zip(active_ids or [], active_values or []):
                label = aid["index"]
                key = UI_TO_INTERNAL.get(label)
                if key:
                    params.setdefault(key, {})["active"] = bool(val)

            # Variables
            for vid, val in zip(vars_ids or [], vars_values or []):
                key = UI_TO_INTERNAL.get(vid["mode"])
                if not key:
                    continue
                p = params.setdefault(key, {"active": True})
                vlabel = (vid["var"] or "").lower()
                if "temps" in vlabel:
                    p["cost_of_time_eur_per_h"] = float(val or 0)
                elif "distance" in vlabel:
                    p["cost_of_distance_eur_per_km"] = float(val or 0)
                elif "constante" in vlabel:
                    p["cost_constant"] = float(val or 0)

            # Sous-modes TC
            if pt_checked_ids and pt_checked_vals:
                pt_map = {"walk_pt": "pt_walk", "car_pt": "pt_car", "bicycle_pt": "pt_bicycle"}
                pt_cfg = params.setdefault(
                    "public_transport",
                    {"active": params.get("public_transport", {}).get("active", True)},
                )
                for pid, checked in zip(pt_checked_ids, pt_checked_vals):
                    alias = pt_map.get(pid["index"])
                    if alias:
                        pt_cfg[alias] = bool(checked)

            # Calcul scénario
            scn = get_scenario(local_admin_unit_id=lau_norm, radius=r, transport_modes_params=params)
            deck_json = _make_deck_json_from_scn(scn)
            summary = StudyAreaSummary(scn["zones_gdf"], visible=True, id_prefix=MAPP)

            # Conserver la caméra si le LAU ne change pas
            prev_key = (deck_memo or {}).get("key") or str(uuid.uuid4())
            prev_lau = (deck_memo or {}).get("lau")
            new_key = prev_key if prev_lau == lau_norm else str(uuid.uuid4())
            new_memo = {"key": new_key, "lau": lau_norm}

            return deck_json, new_key, summary, new_memo

        except Exception as e:
            err = dmc.Alert(
                f"Erreur pendant la simulation : {e}",
                color="red",
                variant="filled",
                radius="md",
            )
            return no_update, no_update, err, no_update

    # Forcer au moins un mode actif (tooltips)
    @app.callback(
        Output({"type": "mode-active", "index": ALL}, "checked"),
        Output({"type": "mode-tip", "index": ALL}, "opened"),
        Input({"type": "mode-active", "index": ALL}, "checked"),
        State({"type": "mode-active", "index": ALL}, "id"),
        prevent_initial_call=True,
    )
    def _enforce_one_mode(checked_list, ids):
        if not checked_list or not ids:
            return no_update, no_update
        n_checked = sum(bool(v) for v in checked_list)
        triggered = ctx.triggered_id
        if n_checked == 0 and triggered is not None:
            new_checked, new_opened = [], []
            for id_, val in zip(ids, checked_list):
                if id_ == triggered:
                    new_checked.append(True)
                    new_opened.append(True)
                else:
                    new_checked.append(bool(val))
                    new_opened.append(False)
            return new_checked, new_opened
        return [bool(v) for v in checked_list], [False] * len(ids)

    # Forcer au moins un sous-mode PT actif (tooltips)
    @app.callback(
        Output({"type": "pt-submode", "index": ALL}, "checked"),
        Output({"type": "pt-tip", "index": ALL}, "opened"),
        Input({"type": "pt-submode", "index": ALL}, "checked"),
        State({"type": "pt-submode", "index": ALL}, "id"),
        prevent_initial_call=True,
    )
    def _enforce_one_pt_submode(checked_list, ids):
        if not checked_list or not ids:
            return no_update, no_update
        n_checked = sum(bool(v) for v in checked_list)
        triggered = ctx.triggered_id
        if n_checked == 0 and triggered is not None:
            new_checked, new_opened = [], []
            for id_, val in zip(ids, checked_list):
                if id_ == triggered:
                    new_checked.append(True)
                    new_opened.append(True)
                else:
                    new_checked.append(bool(val))
                    new_opened.append(False)
            return new_checked, new_opened
        return [bool(v) for v in checked_list], [False] * len(ids)
