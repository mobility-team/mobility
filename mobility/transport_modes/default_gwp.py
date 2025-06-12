from dataclasses import dataclass, field
from typing import Dict
import pandas as pd

@dataclass
class DefaultGWP:
    values: Dict[str, float] = field(default_factory=lambda: {
        "1.10": 0.0,       # Uniquement marche à pied
        "1.11": 0.0,       # Porté, transporté en poussette
        "1.12": 0.0,       # Rollers, trottinette
        "1.13": 0.0,       # Fauteuil roulant (y compris motorisé)
        "2.20": 0.00017,   # Bicyclette, tricycle (y compris à assistance électrique)
        "2.22": 0.0763,    # Cyclomoteur – Conducteur
        "2.23": 0.0763,    # Cyclomoteur – Passager
        "2.24": 0.191,     # Moto – Conducteur (y compris avec side-car)
        "2.25": 0.191,     # Moto – Passager (y compris avec side-car)
        "2.29": 0.191,     # Motocycles sans précision (y compris quads)
        "3.30": 0.218,     # Voiture, VUL, voiturette – Conducteur seul
        "3.31": 0.218,     # Voiture, VUL, voiturette – Conducteur avec passager
        "3.32": 0.218,     # Voiture, VUL, voiturette – Passager
        "3.33": 0.218,     # Voiture, VUL, voiturette – Tantôt conducteur tantôt passager
        "3.39": 0.218,     # Trois ou quatre roues sans précision
        "4.40": 0.218,     # Taxi (individuel, collectif)
        "4.41": 0.218,     # Transport spécialisé (handicapé)
        "4.42": 0.113,     # Ramassage organisé par l'employeur
        "4.43": 0.113,     # Ramassage scolaire
        "5.50": 0.113,     # Autobus urbain, trolleybus
        "5.51": 0.113,     # Navette fluviale
        "5.52": 0.0294,    # Autocar de ligne (sauf SNCF)
        "5.53": 0.0294,    # Autre autocar (affrètement, service spécialisé)
        "5.54": 0.0294,    # Autocar SNCF
        "5.55": 0.00428,   # Tramway
        "5.56": 0.00444,   # Métro, VAL, funiculaire
        "5.57": 0.00978,   # RER, SNCF banlieue
        "5.58": 0.0277,    # TER
        "5.59": 0.0294,    # Autres transports urbains & régionaux (sans précision)
        "6.60": 0.00293,   # TGV, 1ère classe
        "6.61": 0.00293,   # TGV, 2ème classe
        "6.62": 0.00898,   # Autre Train, 1ère classe
        "6.63": 0.00898,   # Autre Train, 2ème classe
        "6.69": 0.00898,   # Train sans précision
        "7.70": 0.152,     # Avion
        "8.80": 0.218,     # Bateau
        "9.90": 0.218      # Autre
    })
    
    def as_dataframe(self):
        return pd.DataFrame(self.values.items(), columns=["mode_id", "gwp"])