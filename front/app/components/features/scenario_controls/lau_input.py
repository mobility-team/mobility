import dash_mantine_components as dmc

def LauInput(id_prefix: str, *, default_insee: str = "31555"):
    """Crée un champ de saisie pour la zone d’étude (code INSEE ou LAU).

    Ce composant permet à l’utilisateur d’indiquer le code de la commune ou
    unité administrative locale utilisée comme point de référence pour le scénario.
    Le champ est pré-rempli avec un code par défaut (par exemple, Toulouse : `31555`)
    et conserve les identifiants Dash existants pour compatibilité avec les callbacks.

    Args:
        id_prefix (str): Préfixe pour l’identifiant du composant Dash.  
            L’ID généré est de la forme `"{id_prefix}-lau-input"`.
        default_insee (str, optional): Code INSEE ou LAU affiché par défaut.  
            Par défaut `"31555"`.

    Returns:
        dmc.TextInput: Champ de saisie Mantine configuré pour l’entrée du code INSEE/LAU.
    """
    return dmc.TextInput(
        id=f"{id_prefix}-lau-input",
        value=default_insee,
        label="Zone d’étude (INSEE)",
        placeholder="ex: 31555",
        w=250,
    )
