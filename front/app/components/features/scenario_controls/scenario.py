from .scenario_controls.panel import ScenarioControlsPanel

def ScenarioControls(
    id_prefix: str = "scenario",
    min_radius: int = 15,
    max_radius: int = 50,
    step: int = 1,
    default: int | float = 40,
    default_insee: str = "31555",
):
    return ScenarioControlsPanel(
        id_prefix=id_prefix,
        min_radius=min_radius,
        max_radius=max_radius,
        step=step,
        default=default,
        default_insee=default_insee,
    )
