## Rôles dans le projet

* Gestionnaire du projet :
   * Peut gérer l'équipe `mobility` sur GitHub
   * Peut gérer les données sur data.gouv.fr
   * Peut communiquer en externe sur le projet
   * A aussi les rôles revieweu·se et contributeur·ice

 Actuellement : @[remiBabut](https://github.com/remiBabut), @[Mind-the-Cap](https://github.com/Mind-the-Cap), @[FlxPo](https://github.com/FlxPo), @[louisegontier](https://github.com/louisegontier)

* Revieweur·se :
   * Peut reviewer les PR sur Github

* Contributeur·ice :
   * Peut proposer des PR (et les fusionner quand approuvées)
   * Peut ouvrir des issues et y contribuer

## Conventions de code
Les conventions Python permettent d'avoir une meilleure lisibilité du code, ce qui est important car on lit plus souvent le code qu'on ne l'écrit.
Un code standard peut être réutilisé facilement pour des projets, est plus facile à améliorer, et évite les doublons.
Nous suivons les conventions  [PEP8](https://peps.python.org/pep-0008/) et [PEP257](https://peps.python.org/pep-0257/) pour le projet.
Vous trouverez un court descriptif de la première [ici](https://python.doctor/page-pep-8-bonnes-pratiques-coder-python-apprendre), en voici les points essentiels :
* des lignes courtes (pas plus de 127 caractères[^1]),
* indentation avec 4 caractères,
* bon positionnement des espaces,
* commenter le code en anglais,
* utiliser les docstrings.

Cette utilisation a été décidée [ici](https://github.com/mobility-team/mobility/issues/20). Des tests automatiques vérifient que la convention est suivie (mais restent assez permissifs pour les erreurs mineures).

## Gestion des données externes
Nous cherchons à décharger les utilisateurs et utilisatrices de la gestion des données.

Idéalement, nous utilisons les API disponibles.

À défaut, nos parseurs vont récupérer directement les données massives en externe, les prétraitent, et les stockent en local.
Ils utilisent un lien permanent du fournisseur, ou, à défaut, les liens permanents mis en place par Mobility sur [data.gouv.fr](https://www.data.gouv.fr/fr/organizations/mobility/).

Pour les données plus légères (en dessous de 50 Mo), nous pouvons les stocker directement dans le package, en indiquant dans la documentation le moment où il faudra les mettre à jour.

Les données sont décrites dans [mobility/data](https://github.com/mobility-team/mobility/tree/main/mobility/data).


[^1]: Nous autorisons 127 caractères au lieu des 79 prévus à la base dans PEP8.
