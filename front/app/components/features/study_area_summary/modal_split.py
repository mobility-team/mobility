import dash_mantine_components as dmc
from .utils import fmt_pct

def ModalSplitList(share_car, share_bike, share_walk):
    return dmc.Stack(
        [
            dmc.Group([dmc.Text("Voiture :", size="sm"), dmc.Text(fmt_pct(share_car, 1), fw=600, size="sm")], gap="xs"),
            dmc.Group([dmc.Text("Vélo :",    size="sm"), dmc.Text(fmt_pct(share_bike, 1), fw=600, size="sm")], gap="xs"),
            dmc.Group([dmc.Text("À pied :",  size="sm"), dmc.Text(fmt_pct(share_walk, 1), fw=600, size="sm")], gap="xs"),
        ],
        gap="xs",
    )
