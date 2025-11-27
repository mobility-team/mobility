import dash_mantine_components as dmc

def RunButton(id_prefix: str, *, label: str = "Lancer la simulation"):
    """Crée le bouton principal d’exécution du scénario.

    Ce bouton est utilisé pour lancer une simulation ou exécuter une action
    principale dans l’interface utilisateur.  
    Il est stylisé avec une apparence remplie (`variant="filled"`) et s’aligne
    à gauche du conteneur.

    Args:
        id_prefix (str): Préfixe pour l’identifiant du composant Dash.
            Le bouton aura un ID du type `"{id_prefix}-run-btn"`.
        label (str, optional): Texte affiché sur le bouton.
            Par défaut `"Lancer la simulation"`.

    Returns:
        dmc.Button: Composant Mantine représentant le bouton d’action.
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
