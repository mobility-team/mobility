import dash_mantine_components as dmc
from dash import html

# -------------------------
# Données mock : 5 modes (PT inclus) + sous-modes PT
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
    {
        "name": "Transport en commun",
        "active": True,  # coché par défaut
        "vars": {
            "Valeur du temps (€/h)": 12,
            "Valeur de la distance (€/km)": 0.01,
            "Constante de mode (€)": 1,
        },
        "pt_submodes": {
            # 3 sous-modes cochés par défaut
            "walk_pt": True,
            "car_pt": True,
            "bicycle_pt": True,
        },
    },
]

VAR_SPECS = {
    "Valeur du temps (€/h)": {"min": 0, "max": 50, "step": 1},
    "Valeur de la distance (€/km)": {"min": 0, "max": 2, "step": 0.1},
    "Constante de mode (€)": {"min": 0, "max": 20, "step": 1},
}

PT_SUB_LABELS = {
    "walk_pt": "Marche + TC",
    "car_pt": "Voiture + TC",
    "bicycle_pt": "Vélo + TC",
}

PT_COLOR = "#e5007d"  # rouge/magenta du logo AREP


def _mode_header(mode):
    """Case + nom, avec tooltip rouge contrôlé par main.py."""
    return dmc.Group(
        [
            dmc.Tooltip(
                label="Au moins un mode doit rester actif",
                position="right",
                withArrow=True,
                color=PT_COLOR,
                opened=False,     # ouvert via callback si nécessaire
                withinPortal=True,
                zIndex=9999,
                transitionProps={"transition": "fade", "duration": 300, "timingFunction": "ease-in-out"},
                id={"type": "mode-tip", "index": mode["name"]},
                children=dmc.Checkbox(
                    id={"type": "mode-active", "index": mode["name"]},
                    checked=mode["active"],
                ),
            ),
            dmc.Text(mode["name"], fw=700),  # tous en gras
        ],
        gap="sm",
        align="center",
        w="100%",
    )


def _pt_submodes_block(mode):
    """Bloc des sous-modes PT (coches + tooltip individuel)."""
    pt_cfg = mode.get("pt_submodes") or {}
    rows = []
    for key, label in PT_SUB_LABELS.items():
        rows.append(
            dmc.Group(
                [
                    dmc.Tooltip(
                        label="Au moins un sous-mode TC doit rester actif",
                        position="right",
                        withArrow=True,
                        color=PT_COLOR,
                        opened=False,
                        withinPortal=True,
                        zIndex=9999,
                        transitionProps={"transition": "fade", "duration": 300, "timingFunction": "ease-in-out"},
                        id={"type": "pt-tip", "index": key},
                        children=dmc.Checkbox(
                            id={"type": "pt-submode", "index": key},
                            checked=bool(pt_cfg.get(key, True)),
                        ),
                    ),
                    dmc.Text(label, size="sm"),
                ],
                gap="sm",
                align="center",
            )
        )
    return dmc.Stack(rows, gap="xs")


def _mode_body(mode):
    """Variables (NumberInput) + éventuels sous-modes PT."""
    rows = []
    # Variables
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
    # Sous-modes PT
    if mode["name"] == "Transport en commun":
        rows.append(dmc.Divider())
        rows.append(dmc.Text("Sous-modes (cumulatifs)", size="sm", fw=600))
        rows.append(_pt_submodes_block(mode))
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
