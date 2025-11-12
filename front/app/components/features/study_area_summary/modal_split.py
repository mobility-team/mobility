import dash_mantine_components as dmc
from .utils import fmt_pct

def _row(label: str, val) -> dmc.Group | None:
    if val is None: return None
    try:
        v = float(val)
    except Exception:
        return None
    if v <= 0:  # n'affiche pas les zéros
        return None
    return dmc.Group([dmc.Text(f"{label} :", size="sm"),
                      dmc.Text(fmt_pct(v, 1), fw=600, size="sm")], gap="xs")

def ModalSplitList(
    share_car=None, share_bike=None, share_walk=None, share_carpool=None,
    share_pt=None, share_pt_walk=None, share_pt_car=None, share_pt_bicycle=None
):
    rows = [
        _row("Voiture", share_car),
        _row("Vélo", share_bike),
        _row("À pied", share_walk),
        _row("Covoiturage", share_carpool),
    ]
    if (share_pt or 0) > 0:
        rows.append(dmc.Group([dmc.Text("Transports en commun", fw=700, size="sm"),
                               dmc.Text(fmt_pct(share_pt, 1), fw=700, size="sm")], gap="xs"))
        # sous-modes (indentés)
        sub = [
            _row("  À pied + TC",  share_pt_walk),
            _row("  Voiture + TC", share_pt_car),
            _row("  Vélo + TC",    share_pt_bicycle),
        ]
        rows.extend([r for r in sub if r is not None])

    rows = [r for r in rows if r is not None]
    return dmc.Stack(rows, gap="xs")
