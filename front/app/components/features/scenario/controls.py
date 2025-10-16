import dash_mantine_components as dmc
from dash import html


def ScenarioControls(
    id_prefix: str = "scenario",
    min_radius: int = 15,   # min = 15 km
    max_radius: int = 50,   # max = 50 km
    step: int = 1,
    default: int | float = 40,
    default_insee: str = "31555",
):
    """
    Panneau de contrôles vertical :
      - Rayon (slider + input)
      - Zone d’étude (INSEE)
      - Bouton 'Lancer la simulation'
    """
    return dmc.Stack(
        [
            # ---- Rayon ----
            dmc.Group(
                [
                    dmc.Text("Rayon (km)", fw=600, w=100, ta="right"),
                    dmc.Slider(
                        id=f"{id_prefix}-radius-slider",
                        value=default,
                        min=min_radius,
                        max=max_radius,
                        step=step,
                        w=280,
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
                        w=90,
                        styles={"input": {"textAlign": "center", "marginTop": "10px"}},
                    ),
                ],
                gap="md",
                align="center",
                justify="flex-start",
                wrap=False,
            ),

            # ---- Zone d’étude ----
            dmc.TextInput(
                id=f"{id_prefix}-lau-input",
                value=default_insee,
                label="Zone d’étude (INSEE)",
                placeholder="ex: 31555",
                w=250,
            ),

            # ---- Bouton ----
            dmc.Button(
                "Lancer la simulation",
                id=f"{id_prefix}-run-btn",
                variant="filled",
                style={
                    "marginTop": "10px",
                    "width": "fit-content",   
                    "alignSelf": "flex-start", 
                },
            ),
        ],
        gap="sm",
        style={
            "width": "fit-content",
            "padding": "8px",
        },
    )
