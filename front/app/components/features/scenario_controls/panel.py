# app/components/features/.../panel.py
import dash_mantine_components as dmc
from .radius import RadiusControl
from .lau_input import LauInput
from .run_button import RunButton
from .transport_modes_inputs import TransportModesInputs


def ScenarioControlsPanel(
    id_prefix: str = "scenario",
    *,
    min_radius: int = 15,
    max_radius: int = 50,
    step: int = 1,
    default: int | float = 40,
    default_insee: str = "31555",
):
    """Assemble le panneau vertical de contrôle du scénario.

    Ce composant regroupe les principaux contrôles nécessaires à la
    configuration d’un scénario de mobilité ou d’analyse territoriale.
    Il est organisé verticalement (`dmc.Stack`) et inclut :
      - le contrôle du **rayon d’étude** (`RadiusControl`) ;
      - la **zone d’étude (INSEE/LAU)** (`LauInput`) ;
      - la section des **modes de transport** (`TransportModesInputs`) ;
      - le **bouton d’exécution** (`RunButton`).

    Ce panneau constitue la partie principale de l’interface utilisateur permettant
    de définir les paramètres du scénario avant de lancer une simulation.

    Args:
        id_prefix (str, optional): Préfixe utilisé pour générer les identifiants
            Dash des sous-composants. Par défaut `"scenario"`.
        min_radius (int, optional): Valeur minimale du rayon d’étude (en km).  
            Par défaut `15`.
        max_radius (int, optional): Valeur maximale du rayon d’étude (en km).  
            Par défaut `50`.
        step (int, optional): Pas d’incrémentation du rayon pour le slider et l’input.  
            Par défaut `1`.
        default (int | float, optional): Valeur initiale du rayon affichée.  
            Par défaut `40`.
        default_insee (str, optional): Code INSEE ou identifiant LAU par défaut de la
            zone sélectionnée (ex. `"31555"` pour Toulouse).  

    Returns:
        dmc.Stack: Composant vertical (`Stack`) regroupant tous les contrôles du panneau scénario.
    """
    return dmc.Stack(
        [
            RadiusControl(
                id_prefix,
                min_radius=min_radius,
                max_radius=max_radius,
                step=step,
                default=default,
            ),
            LauInput(id_prefix, default_insee=default_insee),
            TransportModesInputs(id_prefix="tm"),
            RunButton(id_prefix),
        ],
        gap="sm",
        style={"width": "fit-content", "padding": "8px"},
    )
