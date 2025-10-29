# app/components/features/.../transport_modes_inputs.py
import dash_mantine_components as dmc
from dash import html

# -------------------------
# Données mock : 3 modes de transport
# -------------------------
MOCK_MODES = [
    {
        "name": "À pied",
        "active": True,
        "vars": {
            "Valeur du temps (€/h)": 12,
            "Valeur de la distance (€/km)": 0.01,
            "Constante de mode (€)": 1,
        },
    },
    {
        "name": "Vélo",
        "active": True,
        "vars": {
            "Valeur du temps (€/h)": 12,
            "Valeur de la distance (€/km)": 0.01,
            "Constante de mode (€)": 1,
        },
    },
    {
        "name": "Voiture",
        "active": True,
        "vars": {
            "Valeur du temps (€/h)": 12,
            "Valeur de la distance (€/km)": 0.01,
            "Constante de mode (€)": 1,
        },
    },
    {
        "name": "Covoiturage",
        "active": True,
        "vars": {
            "Valeur du temps (€/h)": 12,
            "Valeur de la distance (€/km)": 0.01,
            "Constante de mode (€)": 1,
        },
    },
]

VAR_SPECS = {
    "Valeur du temps (€/h)": {"min": 0, "max": 50, "step": 1},
    "Valeur de la distance (€/km)": {"min": 0, "max": 2, "step": 0.1},
    "Constante de mode (€)": {"min": 0, "max": 20, "step": 1},
}

def _mode_header(mode):
    return dmc.Group(
        [
            dmc.Checkbox(checked=mode["active"], id={"type": "mode-active", "index": mode["name"]}),
            dmc.Text(mode["name"], fw=600),
        ],
        gap="sm",
        align="center",
        w="100%",
    )

def _mode_body(mode):
    rows = []
    for label, val in mode["vars"].items():
        spec = VAR_SPECS[label]
        rows.append(
            dmc.Group(
                [
                    dmc.Text(label),
                    dmc.NumberInput(
                        value=val,
                        min=spec["min"],
                        max=spec["max"],
                        step=spec["step"],
                        style={"width": 140},
                        id={"type": "mode-var", "mode": mode["name"], "var": label},
                    ),
                ],
                justify="space-between",
                align="center",
            )
        )
    return dmc.Stack(rows, gap="md")

def _modes_list():
    items = [
        dmc.AccordionItem(
            [dmc.AccordionControl(_mode_header(m)), dmc.AccordionPanel(_mode_body(m))],
            value=f"mode-{i}",
        )
        for i, m in enumerate(MOCK_MODES)
    ]
    return dmc.Accordion(
        children=items,
        multiple=True,
        value=[],                 # fermé par défaut
        chevronPosition="right",
        chevronSize=18,
        variant="separated",
        radius="md",
        styles={"control": {"paddingTop": 8, "paddingBottom": 8}},
    )

def TransportModesInputs(id_prefix="tm"):
    """Panneau principal 'MODES DE TRANSPORT' collapsable."""
    return dmc.Accordion(
        children=[
            dmc.AccordionItem(
                [
                    dmc.AccordionControl(
                        dmc.Group(
                            [dmc.Text("MODES DE TRANSPORT", fw=700), html.Div(style={"flex": 1})],
                            align="center",
                        )
                    ),
                    dmc.AccordionPanel(_modes_list()),
                ],
                value="tm-root",
            )
        ],
        multiple=True,
        value=[],                 # parent fermé par défaut
        chevronPosition="right",
        chevronSize=18,
        variant="separated",
        radius="lg",
        styles={"control": {"paddingTop": 10, "paddingBottom": 10}},
    )
