import dash_mantine_components as dmc

def RunButton(id_prefix: str, *, label: str = "Lancer la simulation"):
    """
    Bouton d’action principal. Conserve l’ID existant.
    """
    return dmc.Button(
        label,
        id=f"{id_prefix}-run-btn",
        variant="filled",
        style={
            "marginTop": "10px",
            "width": "fit-content",
            "alignSelf": "flex-start",
        },
    )
