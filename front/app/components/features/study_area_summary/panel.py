from dash import html
import dash_mantine_components as dmc
from .utils import safe_mean
from .kpi import KPIStatGroup
from .modal_split import ModalSplitList
from .legend import LegendCompact


def StudyAreaSummary(zones_gdf, visible=True, id_prefix="map", header_offset_px=80, width_px=340):
    """Crée le panneau de résumé global d'une zone d’étude.

    Ce composant affiche un résumé synthétique des indicateurs calculés pour la zone
    d’étude sélectionnée, tels que :
      - les temps et distances de déplacement moyens ;
      - la répartition modale (voiture, vélo, marche, covoiturage, transport collectif) ;
      - la légende de la carte (liée à la variable de temps de trajet moyen).

    Le panneau s’affiche à droite de la carte, avec une position et une taille fixes.
    Si `zones_gdf` est vide ou manquant, un message d’indisponibilité est affiché.

    Args:
        zones_gdf (GeoDataFrame | None): Données géographiques de la zone d’étude, 
            contenant au minimum les colonnes :
            - `average_travel_time`
            - `total_dist_km`
            - `share_car`, `share_bicycle`, `share_walk`, `share_carpool`
            - `share_public_transport`, `share_pt_walk`, `share_pt_car`, `share_pt_bicycle`
        visible (bool, optional): Définit si le panneau est visible ou masqué.
            Par défaut `True`.
        id_prefix (str, optional): Préfixe d’identifiant Dash pour éviter les collisions.  
            Par défaut `"map"`.
        header_offset_px (int, optional): Décalage vertical en pixels sous l’en-tête 
            principal de la page. Par défaut `80`.
        width_px (int, optional): Largeur du panneau latéral (en pixels).  
            Par défaut `340`.

    Returns:
        html.Div: Conteneur principal du panneau de résumé (`div` HTML) contenant un
        composant `dmc.Paper` avec les statistiques et graphiques de la zone.

    Notes:
        - Les moyennes sont calculées avec la fonction utilitaire `safe_mean()` pour
          éviter les erreurs sur valeurs manquantes ou NaN.
        - Si `zones_gdf` est vide, le contenu du panneau se limite à un texte indiquant
          l’absence de données globales.
    """
    comp_id = f"{id_prefix}-study-summary"

    if zones_gdf is None or getattr(zones_gdf, "empty", True):
        content = dmc.Text(
            "Données globales indisponibles.",
            size="sm",
            style={"fontStyle": "italic", "opacity": 0.8},
        )
    else:
        # Calcul des moyennes sécurisées
        avg_time = safe_mean(zones_gdf.get("average_travel_time"))
        avg_dist = safe_mean(zones_gdf.get("total_dist_km"))

        share_car = safe_mean(zones_gdf.get("share_car"))
        share_bike = safe_mean(zones_gdf.get("share_bicycle"))
        share_walk = safe_mean(zones_gdf.get("share_walk"))
        share_pool = safe_mean(zones_gdf.get("share_carpool"))

        share_pt = safe_mean(zones_gdf.get("share_public_transport"))
        share_pt_walk = safe_mean(zones_gdf.get("share_pt_walk"))
        share_pt_car = safe_mean(zones_gdf.get("share_pt_car"))
        share_pt_bicycle = safe_mean(zones_gdf.get("share_pt_bicycle"))

        # Construction du contenu principal
        content = dmc.Stack(
            [
                dmc.Text("Résumé global de la zone d'étude", fw=700, size="md"),
                dmc.Divider(),
                KPIStatGroup(avg_time_min=avg_time, avg_dist_km=avg_dist),
                dmc.Divider(),
                dmc.Text("Répartition modale", fw=600, size="sm"),
                ModalSplitList(
                    share_car=share_car,
                    share_bike=share_bike,
                    share_walk=share_walk,
                    share_carpool=share_pool,
                    share_pt=share_pt,
                    share_pt_walk=share_pt_walk,
                    share_pt_car=share_pt_car,
                    share_pt_bicycle=share_pt_bicycle,
                ),
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
