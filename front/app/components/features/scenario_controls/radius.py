import dash_mantine_components as dmc

def RadiusControl(
    id_prefix: str,
    *,
    min_radius: int = 15,
    max_radius: int = 50,
    step: int = 1,
    default: int | float = 40,
):
    """
    Contrôle de rayon : slider + number input.
    Conserve EXACTEMENT les mêmes IDs qu'avant.
    """
    return dmc.Group(
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
    )
