# Mobility : Interface WEB

* Présentation Générale 
La bibliothèque Mobility a pour but de permettre l’étude de la mobilité de différentes personnes pour divers motifs à des échelles multiples, de l’individu à la population, du bâtiment au territoire.

Ce Projet de Développement Informatique consistait à proposer un premier prototype d'interface graphique de la librairie Mobility.

L'interface WEB permet de générer des graphes simulant les déplacements des individus sur un territoire choisi, et de mettre en évidence le mode de transport qu'un utilisateur répondant aux caractéristiques CSP sélectionnées privilégierait pour se rendre aux destinations colorisées.

## Comment utiliser l'interface ?
- Lancer le script app.py
- Démarrer votre navigateur WEB et aller à l'addresse locale suivante : http://127.0.0.1:8050/
- Sélectionner la simulation souhaitée (Pour l'instant seule la simulation part modale fonctionne)
- Sélectionner/Desélectionner au besoin les paramètres d'intérêt personnel (Pour l'instant n'ont aucun effet sur la simulation)
- Renseigner la zone d'étude selon le mode Rayon, Commune ou Département (Pour l'instant seul le rayon à été implanté)
- Lancer la simulation
- Télécharger les données utilisées au format CSV avec le bouton prévu à cet effet [non-implémenté]
- Télécharger le graphe simulé au format SVG avec le bouton prévu à cet effet [non-implémenté]

## Bon à savoir : 
>- Ne pas oublier de lire les infobulles, elles sont là pour mettre du contexte et vous guider dans votre simulation !
>- Si vous voulez installer spyder uniquement pour lancer ce projet : Démarrez miniforge, utilisez:  
>```
>mamba activate mobility
>pip install spyder
>```
>- Ne pas s'étonner de la durée du script surtout au premier lancement, la bibliotèque mobility prend du temps pour tout mettre en place
>- A chaque fois que la zone d'étude va être changée, un temps de chargement assez long va être présent (temps de télécharger les données de la zone)
>- Quand une simulation est effectuée, certains résultats sont sauvegardés, ce qui permet un chargement plus rapide lors de la relance de ceux-ci
>- Les simulations sont faites sur un echantillon de 1000 trajets, modifiable si vous souhaitez plus de valeurs au détriment du temps de calcul dans le fichier sim.py, dans la variable 'sample_size'
>- Pour des modifications plus précises des simulations, lire la [documentation de mobility]() 

© 2025 Mobility WebApp Prototype Interface Graphique - Développé par Anaïs Floch, Liam Longfier et Gabin Potel