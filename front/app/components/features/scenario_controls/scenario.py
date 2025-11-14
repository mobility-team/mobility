from .scenario_controls.panel import ScenarioControlsPanel

def ScenarioControls(
    id_prefix: str = "scenario",
    min_radius: int = 15,
    max_radius: int = 50,
    step: int = 1,
    default: int | float = 40,
    default_insee: str = "31555",
):
    """Construit et retourne le panneau de contrôle principal du scénario.

    Cette fonction agit comme un wrapper simple autour de `ScenarioControlsPanel`,
    qui gère l’interface utilisateur permettant de configurer les paramètres d’un
    scénario (zone géographique, rayon d’analyse, etc.).  
    Elle définit les valeurs par défaut et simplifie la création du composant.

    Args:
        id_prefix (str, optional): Préfixe utilisé pour les identifiants Dash afin
            d’éviter les collisions entre composants. Par défaut `"scenario"`.
        min_radius (int, optional): Rayon minimal autorisé dans le contrôle (en km).
            Par défaut `15`.
        max_radius (int, optional): Rayon maximal autorisé dans le contrôle (en km).
            Par défaut `50`.
        step (int, optional): Pas d’incrémentation du rayon (en km) pour le sélecteur.
            Par défaut `1`.
        default (int | float, optional): Valeur initiale du rayon affichée par défaut.
            Par défaut `40`.
        default_insee (str, optional): Code INSEE ou identifiant LAU de la commune
            sélectionnée par défaut. Par défaut `"31555"` (Toulouse).

    Returns:
        ScenarioControlsPanel: Instance configurée du panneau de contrôle du scénario.
    """
    return ScenarioControlsPanel(
        id_prefix=id_prefix,
        min_radius=min_radius,
        max_radius=max_radius,
        step=step,
        default=default,
        default_insee=default_insee,
    )
