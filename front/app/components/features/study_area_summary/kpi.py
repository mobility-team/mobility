"""
kpi.py
======

Composants d’affichage des indicateurs clés de performance (KPI) pour la zone d’étude.

Ce module affiche les statistiques principales issues du scénario de mobilité :
- Temps moyen de trajet quotidien (en minutes)
- Distance totale moyenne (en kilomètres)

Ces éléments sont utilisés dans le panneau de résumé (`StudyAreaSummary`)
pour donner une vue synthétique des valeurs moyennes agrégées.
"""

from dash import html
import dash_mantine_components as dmc
from .utils import fmt_num


def KPIStat(label: str, value: str):
    """Crée une ligne d’affichage d’un indicateur clé (KPI).

    Affiche un libellé descriptif suivi de sa valeur formatée (texte mis en gras).
    Utilisé pour représenter une statistique simple telle qu’un temps moyen
    ou une distance totale.

    Args:
        label (str): Nom de l’indicateur (ex. "Temps moyen de trajet :").
        value (str): Valeur formatée à afficher (ex. "18.5 min/jour").

    Returns:
        dmc.Group: Ligne contenant le label et la valeur du KPI.
    """
    return dmc.Group(
        [dmc.Text(label, size="sm"), dmc.Text(value, fw=600, size="sm")],
        gap="xs",
    )


def KPIStatGroup(avg_time_min: float | None, avg_dist_km: float | None):
    """Construit le groupe d’indicateurs clés de la zone d’étude.

    Ce composant affiche :
      - Le temps moyen de trajet (en minutes par jour)
      - La distance totale moyenne (en kilomètres par jour)

    Si les valeurs sont `None`, elles sont formatées en `"N/A"` grâce à `fmt_num()`.

    Args:
        avg_time_min (float | None): Temps moyen de trajet en minutes.
        avg_dist_km (float | None): Distance totale moyenne en kilomètres.

    Returns:
        dmc.Stack: Bloc vertical contenant les deux statistiques principales.
    """
    return dmc.Stack(
        [
            KPIStat("Temps moyen de trajet :", f"{fmt_num(avg_time_min, 1)} min/jour"),
            KPIStat("Distance totale moyenne :", f"{fmt_num(avg_dist_km, 1)} km/jour"),
        ],
        gap="xs",
    )
