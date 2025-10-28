# app/components/features/…/panel.py
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
    """
    Panneau vertical de contrôles :
      - Rayon (slider + input)
      - Zone d’étude (INSEE)
      - Bouton 'Lancer la simulation'
      - Transport modes
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
