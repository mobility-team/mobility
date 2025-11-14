import dash_mantine_components as dmc

def RadiusControl(
    id_prefix: str,
    *,
    min_radius: int = 15,
    max_radius: int = 50,
    step: int = 1,
    default: int | float = 40,
):
    """Crée un contrôle de sélection du rayon d’analyse (en kilomètres).

    Ce composant combine un **slider** et un **champ numérique synchronisé** 
    pour ajuster le rayon d’un scénario (ex. rayon d’étude autour d’une commune).
    Les identifiants Dash sont conservés pour assurer la compatibilité avec
    les callbacks existants.

    - Le slider permet une sélection visuelle du rayon.
    - Le `NumberInput` permet une saisie précise de la valeur.
    - Les deux sont alignés horizontalement et liés via leur `id_prefix`.

    Args:
        id_prefix (str): Préfixe pour les identifiants Dash.  
            Les IDs générés sont :
            - `"{id_prefix}-radius-slider"`
            - `"{id_prefix}-radius-input"`
        min_radius (int, optional): Valeur minimale du rayon (en km).  
            Par défaut `15`.
        max_radius (int, optional): Valeur maximale du rayon (en km).  
            Par défaut `50`.
        step (int, optional): Pas d’incrémentation pour le slider et l’input.  
            Par défaut `1`.
        default (int | float, optional): Valeur initiale du rayon (en km).  
            Par défaut `40`.

    Returns:
        dmc.Group: Composant Mantine contenant le label, le slider et le champ numérique.
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
