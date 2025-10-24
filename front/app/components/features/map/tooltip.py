def default_tooltip() -> dict:
    return {
        "html": (
            "<div style='font-family:Arial, sans-serif;'>"
            "<b style='font-size:14px;'>Zone d’étude</b><br>"
            "<b>Unité INSEE :</b> {Unité INSEE}<br/>"
            "<b>Identifiant de zone :</b> {Identifiant de zone}<br/><br/>"
            "<b style='font-size:13px;'>Mobilité moyenne</b><br>"
            "Temps moyen de trajet : <b>{Temps moyen de trajet (minutes)}</b> min/jour<br>"
            "Distance totale parcourue : <b>{Distance totale parcourue (km/jour)}</b> km/jour<br>"
            "Niveau d’accessibilité : <b>{Niveau d’accessibilité}</b><br/><br/>"
            "<b style='font-size:13px;'>Répartition modale</b><br>"
            "Part des trajets en voiture : <b>{Part des trajets en voiture (%)}</b><br>"
            "Part des trajets à vélo : <b>{Part des trajets à vélo (%)}</b><br>"
            "Part des trajets à pied : <b>{Part des trajets à pied (%)}</b>"
            "</div>"
        ),
        "style": {
            "backgroundColor": "rgba(255,255,255,0.9)",
            "color": "#111",
            "fontSize": "12px",
            "padding": "8px",
            "borderRadius": "6px",
        },
    }
