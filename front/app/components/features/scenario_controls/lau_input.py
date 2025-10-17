import dash_mantine_components as dmc

def LauInput(id_prefix: str, *, default_insee: str = "31555"):
    """
    Champ de saisie de la zone d’étude (code INSEE/LAU).
    Conserve l’ID existant.
    """
    return dmc.TextInput(
        id=f"{id_prefix}-lau-input",
        value=default_insee,
        label="Zone d’étude (INSEE)",
        placeholder="ex: 31555",
        w=250,
    )
