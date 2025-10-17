from dash import html
import dash_mantine_components as dmc
from .utils import fmt_num

def KPIStat(label: str, value: str):
    return dmc.Group(
        [dmc.Text(label, size="sm"), dmc.Text(value, fw=600, size="sm")],
        gap="xs",
    )

def KPIStatGroup(avg_time_min: float | None, avg_dist_km: float | None):
    return dmc.Stack(
        [
            KPIStat("Temps moyen de trajet :", f"{fmt_num(avg_time_min, 1)} min/jour"),
            KPIStat("Distance totale moyenne :", f"{fmt_num(avg_dist_km, 1)} km/jour"),
        ],
        gap="xs",
    )
