"""
modal_split.py
==============

Composants d’affichage de la répartition modale (part des différents modes de transport).

Ce module fournit :
- une fonction interne `_row()` pour créer une ligne affichant un label et une part en pourcentage ;
- la fonction principale `ModalSplitList()` qui assemble ces lignes dans un composant vertical Mantine.

Utilisé dans le panneau de résumé global (`StudyAreaSummary`) pour présenter la
répartition modale agrégée d’une zone d’étude.
"""

import dash_mantine_components as dmc
from .utils import fmt_pct


def _row(label: str, val) -> dmc.Group | None:
    """Construit une ligne affichant le nom d’un mode et sa part en pourcentage.

    Ignore les valeurs nulles, invalides ou inférieures ou égales à zéro.

    Args:
        label (str): Nom du mode de transport à afficher.
        val (float | None): Part correspondante (entre 0 et 1).

    Returns:
        dmc.Group | None: Ligne contenant le label et la valeur formatée, ou `None`
        si la valeur n’est pas affichable.
    """
    if val is None:
        return None
    try:
        v = float(val)
    except Exception:
        return None
    if v <= 0:
        return None
    return dmc.Group(
        [
            dmc.Text(f"{label} :", size="sm"),
            dmc.Text(fmt_pct(v, 1), fw=600, size="sm"),
        ],
        gap="xs",
    )


def ModalSplitList(
    share_car=None,
    share_bike=None,
    share_walk=None,
    share_carpool=None,
    share_pt=None,
    share_pt_walk=None,
    share_pt_car=None,
    share_pt_bicycle=None,
):
    """Construit la liste affichant la répartition modale par type de transport.

    Crée un empilement vertical (`dmc.Stack`) de lignes représentant la part de
    chaque mode : voiture, vélo, marche, covoiturage, et transports en commun.
    Si les transports en commun sont présents, leurs sous-modes (TC + marche,
    TC + voiture, TC + vélo) sont affichés en indentation.

    Args:
        share_car (float, optional): Part de la voiture.
        share_bike (float, optional): Part du vélo.
        share_walk (float, optional): Part de la marche.
        share_carpool (float, optional): Part du covoiturage.
        share_pt (float, optional): Part totale des transports en commun.
        share_pt_walk (float, optional): Part du sous-mode "à pied + TC".
        share_pt_car (float, optional): Part du sous-mode "voiture + TC".
        share_pt_bicycle (float, optional): Part du sous-mode "vélo + TC".

    Returns:
        dmc.Stack: Composant vertical contenant les parts modales formatées.
    """
    rows = [
        _row("Voiture", share_car),
        _row("Vélo", share_bike),
        _row("À pied", share_walk),
        _row("Covoiturage", share_carpool),
    ]

    if (share_pt or 0) > 0:
        rows.append(
            dmc.Group(
                [
                    dmc.Text("Transports en commun", fw=700, size="sm"),
                    dmc.Text(fmt_pct(share_pt, 1), fw=700, size="sm"),
                ],
                gap="xs",
            )
        )
        # Sous-modes (indentés)
        sub = [
            _row("  À pied + TC", share_pt_walk),
            _row("  Voiture + TC", share_pt_car),
            _row("  Vélo + TC", share_pt_bicycle),
        ]
        rows.extend([r for r in sub if r is not None])

    rows = [r for r in rows if r is not None]
    return dmc.Stack(rows, gap="xs")
