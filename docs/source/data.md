# Data Sources

Mobility prepares many inputs from open data. The goal is to reduce the amount of manual data preparation needed by transport modellers.

## Mobility Surveys

For France, Mobility uses national mobility surveys such as:

- ENTD 2007-2008,
- EMP 2018-2019.

These surveys describe observed travel behaviour. Mobility uses them to build activity and trip patterns for synthetic people.

Survey transfer is a modelling assumption. National surveys provide detailed behavioural patterns, but they do not remove the need to compare the base case with local evidence when local evidence exists.

Some survey codes still appear in model inputs or outputs. The [survey codes page](survey_codes.md) lists the main French codes for socio-professional categories, motives, and trip modes.

### ENTD 2007-2008

The national transport and travel survey, `Enquete Nationale Transports Deplacements`, was run in 2007 and 2008. INSEE describes it as a survey about household trips, use of individual and collective transport modes, and the vehicle fleet owned by households.

Mobility uses ENTD data to sample representative mobility behaviour over a period, using variables such as socio-professional category, urban unit category, number of household cars, and household size.

Useful references:

- [INSEE source description](https://www.insee.fr/fr/metadonnees/source/serie/s1277)
- [PROGEDO variable catalogue](https://data.progedo.fr/studies/doi/10.13144/lil-0634?tab=variables)
- [Detailed ENTD data kept by the Mobility team on data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-detaillees-de-lenquete-national-transports-et-deplacements-2008/)

The detailed data is available under Licence Ouverte.

### EMP 2018-2019

The personal mobility survey, `Enquete Mobilite des Personnes`, was run in 2018 and 2019. It follows a method close to ENTD 2008, so Mobility can process it in a similar way.

Useful references:

- [Official EMP 2018-2019 results](https://www.statistiques.developpement-durable.gouv.fr/resultats-detailles-de-lenquete-mobilite-des-personnes-de-2019)
- [EMP public-use documentation PDF](https://www.statistiques.developpement-durable.gouv.fr/sites/default/files/2022-04/mise_a_disposition_tables_emp2019_public_V2.pdf)
- [Detailed EMP data kept by the Mobility team on data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-detaillees-de-lenquete-mobilite-des-personnes-2018-2019/)

The detailed data is available under Licence Ouverte.

## Administrative And Population Data

Mobility uses administrative and population data to build local study inputs, such as:

- local administrative units,
- census population,
- home-work flows,
- home-study flows,
- urban unit categories.

For French territories, these datasets usually come from INSEE or national open-data portals.

The synthetic population and result metrics use represented-person weights. When checking a project setup, compare the represented population by territory and segment with the population source used for the study.

### ESANE

Facility and business data can come from INSEE's annual business statistics, `Elaboration des statistiques annuelles d'entreprises`.

Reference:

- [INSEE ESANE source description](https://www.insee.fr/fr/metadonnees/source/serie/s1188)

### Urban Unit Categories

The file `cities_category.csv` maps French communes to their INSEE urban unit category. It is based on the [2020 urban unit database](https://www.insee.fr/fr/information/4802589).

This file is stored in the repository because the mapping is small and changes rarely. A future update will probably be needed when INSEE publishes a new urban unit database, likely around 2030.

One small compatibility detail matters for survey processing: the old rural code `R` became `H` in newer data. Mobility handles this when it converts EMP 2019 data to the ENTD 2008-style format used by the model.

## Activities And Opportunities

Activities need opportunity data. For example:

- jobs for work,
- schools or universities for study,
- shops for shopping,
- leisure facilities for leisure.

The exact source depends on the country and the activity.

These raw activity sources are converted into destination opportunity capacity inside the model. A report should keep both levels visible when they matter: the raw proxy used for the activity, and the resulting capacity or occupation indicators used by the simulation.

## Transport Networks

Road, cycling, and walking networks mainly come from OpenStreetMap.

Public transport uses GTFS feeds when available.

Network and service data should be treated as model inputs with versions and dates. This is especially important for public transport feeds, OSM extracts, and scenario-specific network changes.

## Emissions

Greenhouse gas emission factors are based on public emission-factor datasets, including ADEME data for French use cases.

For French studies, Mobility uses emission factors from ADEME Base Carbone.

Useful references:

- [ADEME Base Carbone API](https://api.gouv.fr/les-api/api_base_carbone)
- [Base Carbone on data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-carbone-r-1/)

Mobility keeps a mapping between survey transport modes and the emission-factor modes available in Base Carbone. This makes carbon indicators easier to compute from model results, but it is still a modelling assumption that should stay visible in a study report.

## Data Cache

Mobility stores prepared data in the folders configured with `mobility.set_params(...)`.

Keep project data folders for traceability. They help explain which inputs were used to produce a result.
