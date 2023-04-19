# Données proposées par Mobility
## ENTP-2008
### Description
L'enquête nationale transports déplacements a été réalisée en 2007-2008. [D'après l'INSEE](https://www.insee.fr/fr/metadonnees/source/serie/s1277), « son objectif est la connaissance des déplacements des ménages résidant en France métropolitaine et de leur usage des moyens de transport tant collectifs qu'individuels,  ainsi que la connaissance du parc des véhicules détenus par les ménages et de leur utilisation ».

### Variables
Les variables sont décrites dans [PROGEDO](https://data.progedo.fr/studies/doi/10.13144/lil-0634?tab=variables).

### Conservation
L'équipe Mobility conserve les données détaillées de l'enquête sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-detaillees-de-lenquete-national-transports-et-deplacements-2008/).
C'est ce lien qui est utilisé par le code de Mobility pour récupérer automatiquement les données si elles ne sont pas déjà présentes localement.
Les données sont disponibles sous Licence Ouverte.

### Traitement des données
Les étapes successives de traitement des données sont documentées dans le code.

## EMP-2019
L'[enquête de mobilité des personnes](https://www.statistiques.developpement-durable.gouv.fr/resultats-detailles-de-lenquete-mobilite-des-personnes-de-2019) a été réalisée en 2018 et 2019. Malgré son changement de nom, elle reprend la même méthodologie que l'ENTD 2008, et elle est traitée de manière similaire par `mobility`.

### Variables
Contrairement à l'ENTD 2008, un [document d'accompagnement est disponible](https://www.statistiques.developpement-durable.gouv.fr/sites/default/files/2022-04/mise_a_disposition_tables_emp2019_public_V2.pdf).

### Conservation
Les données sont également conservées sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-detaillees-de-lenquete-mobilite-des-personnes-2018-2019/).
Les données sont disponibles sous Licence Ouverte.

## Base Carbone
### Description
La Base Carbone de l'ADEME recense les facteurs d'émissions en France, et est la base de données de référence. Au moment où cette documentation a été mise à jour (avril 2023), c'est la V22 de la Base Carbone qui est utilisée, et nous développons une requête automatique des données à l'aide de l'[API Base Carbone](https://api.gouv.fr/les-api/api_base_carbone). 

### Variables
Un fichier de mapping permet de faire le lien entre les modes utilisés dans les enquêtes de mobilité et ceux dont les facteurs d'émission sont répertoriés dans la base.

### Licence
La base est [publiée par l'ADEME](https://www.data.gouv.fr/fr/datasets/base-carbone-r-1/) sous Licence Ouverte.

## cities_category.csv
Ce fichier permet de faire le lien entre une commune et sa catégorie au sens INSEE. Documentation à compléter !
