import dash_mantine_components as dmc
from .utils import fmt_pct


def ModalSplitList(
    items=None,
    share_car=None,
    share_bike=None,
    share_walk=None,
    share_carpool=None,  # <-- nouveau param pour compat ascendante
):
    """
    Affiche la répartition modale uniquement pour les modes non nuls (> 0 %).

    Utilisation recommandée :
      - items : liste [(label:str, value:float 0..1), ...] déjà filtrée/renormalisée

    Compatibilité ascendante :
      - si items est None, on construit la liste à partir de share_* fournis.
    """
    rows = []

    # --- Compat ascendante : on construit items depuis les valeurs individuelles
    if items is None:
        items = []
        if share_car is not None:
            items.append(("Voiture", share_car))
        if share_bike is not None:
            items.append(("Vélo", share_bike))
        if share_walk is not None:
            items.append(("À pied", share_walk))
        if share_carpool is not None:
            items.append(("Covoiturage", share_carpool))

    # --- Filtrage : retirer les modes avec part <= 0
    filtered_items = [(label, val) for label, val in items if (val is not None and val > 1e-4)]

    if not filtered_items:
        # Rien à afficher (ex: tous les modes décochés)
        return dmc.Text("Aucun mode actif.", size="sm", c="dimmed")

    # --- Affichage
    for label, value in filtered_items:
        rows.append(
            dmc.Group(
                [
                    dmc.Text(f"{label} :", size="sm"),
                    dmc.Text(fmt_pct(value, 1), fw=600, size="sm"),
                ],
                gap="xs",
            )
        )

    return dmc.Stack(rows, gap="xs")
