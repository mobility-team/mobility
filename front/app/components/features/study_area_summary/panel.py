from dash import html
import dash_mantine_components as dmc
from .utils import safe_mean
from .kpi import KPIStatGroup
from .modal_split import ModalSplitList
from .legend import LegendCompact

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
    API inchangée par rapport à l'ancien composant.
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
        share_car = safe_mean(zones_gdf.get("share_car"))
        share_bike = safe_mean(zones_gdf.get("share_bicycle"))
        share_walk = safe_mean(zones_gdf.get("share_walk"))

        content = dmc.Stack(
            [
                dmc.Text("Résumé global de la zone d'étude", fw=700, size="md"),
                dmc.Divider(),
                KPIStatGroup(avg_time_min=avg_time, avg_dist_km=avg_dist),
                dmc.Divider(),
                dmc.Text("Répartition modale", fw=600, size="sm"),
                ModalSplitList(share_car=share_car, share_bike=share_bike, share_walk=share_walk),
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
