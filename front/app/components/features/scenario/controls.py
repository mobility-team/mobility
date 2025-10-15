# app/components/features/scenario/controls.py
import dash_mantine_components as dmc
from dash import html


def ScenarioControls(id_prefix="scenario", min_radius=1, max_radius=100, step=1, default=40):
    """
    Contrôles scénarios : libellé + slider (largeur fixe) + number input (compact)
    + bouton 'Lancer la simulation'. Les valeurs slider/input sont synchronisées
    ailleurs via callbacks, mais AUCUNE simulation n'est lancée sans cliquer le bouton.
    """
    return dmc.Group(
        [
            dmc.Text("Rayon (km)", fw=600, w=110, ta="right"),
            dmc.Slider(
                id=f"{id_prefix}-radius-slider",
                value=default,
                min=min_radius,
                max=max_radius,
                step=step,
                w=320,  # <-- largeur fixe pour éviter de prendre tout l'écran
                marks=[
                    {"value": min_radius, "label": str(min_radius)},
                    {"value": default, "label": str(default)},
                    {"value": max_radius, "label": str(max_radius)},
                ],
            ),
            dmc.NumberInput(
                id=f"{id_prefix}-radius-input",
                value=default,
                min=min_radius,
                max=max_radius,
                step=step,
                w=110,  # input compact
                styles={"input": {"textAlign": "center"}},
            ),
            dmc.Button(
                "Lancer la simulation",
                id=f"{id_prefix}-run-btn",
                variant="filled",
                size="sm",
            ),
        ],
        gap="md",
        align="center",
        justify="flex-start",
        wrap=False,  # garde tout sur une ligne (si possible)
        style={"width": "100%"},
    )
