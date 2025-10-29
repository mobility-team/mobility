from dash import html
import dash_mantine_components as dmc
import numpy as np

from .utils import safe_mean
from .kpi import KPIStatGroup
from .modal_split import ModalSplitList
from .legend import LegendCompact


def _collect_modal_shares(zones_gdf):
    """
    Récupère les parts modales disponibles dans zones_gdf,
    supprime les modes absents/inactifs (colonne manquante ou NA),
    puis renormalise pour que la somme = 1.
    Retourne une liste de tuples (label, share_float entre 0 et 1).
    """
    # (label affiché, nom de colonne)
    CANDIDATES = [
        ("Voiture", "share_car"),
        ("Covoiturage", "share_carpool"),
        ("Vélo", "share_bicycle"),
        ("À pied", "share_walk"),
    ]

    items = []
    for label, col in CANDIDATES:
        if col in zones_gdf.columns:
            v = safe_mean(zones_gdf[col])
            # on considère absent si None/NaN
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                # borne pour éviter valeurs négatives ou >1 venant de bruit
                v = float(np.clip(v, 0.0, 1.0))
                items.append((label, v))

    if not items:
        return []

    # Renormalisation (ne pas diviser par 0)
    total = sum(v for _, v in items)
    if total > 0:
        items = [(label, v / total) for label, v in items]
    else:
        # tout est 0 -> on retourne tel quel
        pass

    # Optionnel: trier par part décroissante
    items.sort(key=lambda t: t[1], reverse=True)
    return items


def StudyAreaSummary(
    zones_gdf,
    visible: bool = True,
    id_prefix: str = "map",
    header_offset_px: int = 80,
    width_px: int = 340,
):
    """
    Panneau latéral droit affichant les agrégats globaux de la zone d'étude,
    avec légende enrichie (dégradé continu) et contexte (code INSEE/LAU).
    """
    comp_id = f"{id_prefix}-study-summary"

    if zones_gdf is None or getattr(zones_gdf, "empty", True):
        content = dmc.Text(
            "Données globales indisponibles.",
            size="sm",
            style={"fontStyle": "italic", "opacity": 0.8},
        )
    else:
        avg_time = safe_mean(zones_gdf.get("average_travel_time"))
        avg_dist = safe_mean(zones_gdf.get("total_dist_km"))

        # ⚠️ parts modales dynamiques: on ne garde que les modes vraiment présents
        modal_items = _collect_modal_shares(zones_gdf)

        content = dmc.Stack(
            [
                dmc.Text("Résumé global de la zone d'étude", fw=700, size="md"),
                dmc.Divider(),
                KPIStatGroup(avg_time_min=avg_time, avg_dist_km=avg_dist),
                dmc.Divider(),
                dmc.Text("Répartition modale", fw=600, size="sm"),
                # Passe la liste (label, value) au composant d'affichage
                ModalSplitList(items=modal_items),
                dmc.Divider(),
                LegendCompact(zones_gdf.get("average_travel_time")),
            ],
            gap="md",
        )

    return html.Div(
        id=comp_id,
        children=dmc.Paper(
            content,
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
        style={
            "display": "block" if visible else "none",
            "position": "absolute",
            "top": f"{header_offset_px}px",
            "right": "0px",
            "bottom": "0px",
            "width": f"{width_px}px",
            "zIndex": 1200,
            "pointerEvents": "auto",
            "overflow": "hidden",
        },
    )
