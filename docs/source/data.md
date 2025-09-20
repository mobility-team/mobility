# Data
Data used by Mobility. Only available in French and for France for now!

## Données carbone
### Base Carbone
#### Description
La Base Carbone de l'ADEME recense les facteurs d'émissions en France, et est la base de données de référence. Au moment où cette documentation a été mise à jour (avril 2023), c'est la V22 de la Base Carbone qui est utilisée, et nous développons une requête automatique des données à l'aide de l'[API Base Carbone](https://api.gouv.fr/les-api/api_base_carbone).

#### Variables
Un fichier de mapping permet de faire le lien entre les modes utilisés dans les enquêtes de mobilité et ceux dont les facteurs d'émission sont répertoriés dans la base.

## Sondages (`surveys`)
### ENTP-2008
#### Description
L'enquête nationale transports déplacements a été réalisée en 2007-2008. [D'après l'INSEE](https://www.insee.fr/fr/metadonnees/source/serie/s1277), « son objectif est la connaissance des déplacements des ménages résidant en France métropolitaine et de leur usage des moyens de transport tant collectifs qu'individuels,  ainsi que la connaissance du parc des véhicules détenus par les ménages et de leur utilisation ».
### Utilisation
Ces données sont utilisées pour échantillonner sur une certaine durée des déplacement représentatifs de ménages, selon leur CSP, leur catégorie d'unité urbaine, leur nombre de voitures et le nombre de personnes au sein du ménage.
#### Variables
Les variables sont décrites dans [PROGEDO](https://data.progedo.fr/studies/doi/10.13144/lil-0634?tab=variables).
#### Conservation
L'équipe Mobility conserve les données détaillées de l'enquête sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-detaillees-de-lenquete-national-transports-et-deplacements-2008/).
C'est ce lien qui est utilisé par le code de Mobility pour récupérer automatiquement les données si elles ne sont pas déjà présentes localement.

Les données sont disponibles sous Licence Ouverte.

#### Traitement des données
Les étapes successives de traitement des données sont documentées dans le code.

### EMP-2019
L'[enquête de mobilité des personnes](https://www.statistiques.developpement-durable.gouv.fr/resultats-detailles-de-lenquete-mobilite-des-personnes-de-2019) a été réalisée en 2018 et 2019. Malgré son changement de nom, elle reprend la même méthodologie que l'ENTD 2008, et elle est traitée de manière similaire par `mobility`.
#### Variables
Contrairement à l'ENTD 2008, un [document d'accompagnement est disponible](https://www.statistiques.developpement-durable.gouv.fr/sites/default/files/2022-04/mise_a_disposition_tables_emp2019_public_V2.pdf).
#### Conservation
Les données sont également conservées sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-detaillees-de-lenquete-mobilite-des-personnes-2018-2019/).

Les données sont disponibles sous Licence Ouverte.

#### Licence
La base est [publiée par l'ADEME](https://www.data.gouv.fr/fr/datasets/base-carbone-r-1/) sous Licence Ouverte.

## Autres données INSEE
### ESANE
Données d'équipements issues de l'[Élaboration des statistiques annuelles d'entreprises (ÉSANE)](https://www.insee.fr/fr/metadonnees/source/serie/s1188).

### Base des unités urbaines 2020
Le fichier `cities_category.csv` reprend la [base des unités urbaines 2020](https://www.insee.fr/fr/information/4802589) permettant de faire le lien entre une commune et sa catégorie au sens INSEE. Ce fichier est stocké directement dans le répertoire et devrait être actualisé si une nouvelle base est publiée (a priori vers 2030). Pour reproduire les résultats de 2008 « dans leur contexte », il pourrait être utile d'utiliser une base plus ancienne (ce qui n'est pas le cas dans la code en avril 2023).

Le code R (rural) est devenu H (Hors unité urbaine) dans les dernières données, cela est pris en compte par le parseur dans la conversion de l'EMP 2019 au format de l'ENTD 2008.

![image](https://user-images.githubusercontent.com/105421514/233382976-7c4f9bb8-f773-4532-9942-01122c399586.png)
![image](https://user-images.githubusercontent.com/105421514/233383016-068a3f15-aad3-4408-a633-c05768ec04a1.png)
