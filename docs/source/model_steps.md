# Model steps

Ongoing work, French only!

Source : https://github.com/mobility-team/mobility/issues/145#issuecomment-3228039287


Le fonctionnement actuel est le suivant :

Initialisation :
- Génération des séquences de motifs de déplacement dans chaque zone de transport, selon le profil de la population résidente (CSP, nombre de voitures du ménage, type de catégorie urbaine de la commune), et des besoins en heures d'activité pour chaque étape des séquences.
- Calcul des opportunités disponibles (=heures d'activités disponibles) par motif, pour chaque zone de transport.

Boucle :
- Calcul des coûts généralisés de transport pour chaque couple motif - origine - destination (sans congestion pour la première itération).
- Calcul des probabilités de choisir une destination en fonction du motif et de l'origine du déplacement ainsi que du lieu de résidence des personnes.
- Echantillonnage d'une séquence de destinations pour chaque séquence de motifs, zone de transport de résidence et CSP.
- Recherche des top k séquences de modes disponibles pour réaliser ces séquences de déplacements (k<=10)
- Calcul des flux résultants par OD et par mode, puis recalcul des coûts généralisés.
- Calcul d'une part de personnes qui vont changer d'assignation séquence de motifs + modes (en fonction de la saturation des opportunités à destination, de possibilités d'optimisation comparatives, et d'une part de changements aléatoires).
- Calcul des opportunités restantes à destination.
- Recommencement de la procédure avec cette part de personnes non assignées.
