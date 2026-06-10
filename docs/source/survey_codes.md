# French Survey Codes

French survey data still appears in many model inputs and outputs. Use this page to decode common socio-professional categories, urban unit categories, journey motives, and trip modes without opening the raw survey documentation every time.

The labels below are reference labels used for interpretation. They do not replace the official survey documentation when you prepare a calibrated project model.

## Socio-Professional Categories

| Code | Label |
| --- | --- |
| `1` | Agriculteurs exploitants |
| `2` | Artisans, commercants, chefs d'entreprise |
| `3` | Cadres et professions intellectuelles superieures |
| `4` | Professions intermediaires |
| `5` | Employes |
| `6` | Ouvriers |
| `7` | Retraites |
| `8` | Inactifs |
| `no_csp` | Sans CSP |

Reference: [INSEE CSP definition](https://www.insee.fr/fr/metadonnees/definition/c1493).

## Urban Unit Categories

| Code | Label |
| --- | --- |
| `C` | Ville-centre de l'unite urbaine |
| `B` | Banlieue de l'unite urbaine |
| `I` | Ville isolee de l'unite urbaine |
| `R` | Espace rural, hors unite urbaine |

Reference: [INSEE urban unit definition](https://www.insee.fr/fr/metadonnees/definition/c1501).

## Journey Motives

| Code | Label |
| --- | --- |
| `1` to `8` | Motifs prives |
| `1.1` | Aller au domicile |
| `1.2` | Retour a la residence occasionnelle |
| `1.3` | Retour au domicile de parents hors menage ou d'amis |
| `1.11` | Etudier |
| `1.12` | Faire garder un enfant en bas age |
| `2` | Achats |
| `2.20` | Grande surface ou centre commercial |
| `2.21` | Commerce de proximite, boutique, service |
| `3` | Soins |
| `3.31` | Soins medicaux ou personnels |
| `4` | Demarches |
| `4.41` | Demarche administrative, recherche d'informations |
| `5` | Visites |
| `5.51` | Visite a des parents |
| `5.52` | Visite a des amis |
| `6` | Accompagner ou aller chercher |
| `6.61` | Accompagner quelqu'un a la gare, a l'aeroport, au metro, au bus ou au car |
| `6.62` | Accompagner quelqu'un a un autre endroit |
| `6.63` | Aller chercher quelqu'un a la gare, a l'aeroport, au metro, au bus ou au car |
| `6.64` | Aller chercher quelqu'un a un autre endroit |
| `7` | Loisirs |
| `7.71` | Activite associative, ceremonie religieuse, reunion |
| `7.72` | Centre de loisirs, parc d'attraction, foire |
| `7.73` | Manger ou boire hors domicile |
| `7.74` | Monument ou site historique |
| `7.75` | Spectacle culturel ou sportif |
| `7.76` | Sport |
| `7.77` | Promenade sans destination precise |
| `7.78` | Lieu de promenade |
| `8` | Vacances, changement de residence, autres motifs prives |
| `8.80` | Vacances hors residence secondaire |
| `8.81` | Residence secondaire |
| `8.82` | Residence occasionnelle |
| `8.89` | Autres motifs personnels |
| `9` | Motifs professionnels |
| `9.91` | Travail sur le lieu fixe et habituel |
| `9.92` | Travail hors lieu fixe et habituel, sauf tournee |
| `9.94` | Stage, conference, congres, formation, exposition |
| `9.95` | Tournee professionnelle ou visite de patients |
| `9.96` | Autres motifs professionnels |

## Trip Modes

| Code | Label |
| --- | --- |
| `1` | Pieton |
| `1.10` | Marche a pied |
| `1.11` | Porte ou transporte en poussette |
| `1.12` | Rollers, trottinette |
| `1.13` | Fauteuil roulant |
| `2` | Deux roues |
| `2.20` | Bicyclette, tricycle, including electric assistance |
| `2.22` | Cyclomoteur, driver |
| `2.23` | Cyclomoteur, passenger |
| `2.24` | Moto, driver |
| `2.25` | Moto, passenger |
| `2.29` | Motorcycles without detail, including quads |
| `3` | Automobile |
| `3.30` | Car or light utility vehicle, driver alone |
| `3.31` | Car or light utility vehicle, driver with passenger |
| `3.32` | Car or light utility vehicle, passenger |
| `3.33` | Sometimes driver, sometimes passenger |
| `3.39` | Three or four wheels without detail |
| `4` | Specialised transport, school transport, taxi |
| `4.40` | Taxi |
| `4.41` | Specialised transport for disabled people |
| `4.42` | Employer-organised transport |
| `4.43` | School transport |
| `5` | Urban or regional public transport |
| `5.50` | Urban bus or trolleybus |
| `5.51` | River shuttle |
| `5.52` | Line coach, excluding SNCF |
| `5.53` | Other coach |
| `5.54` | SNCF coach |
| `5.55` | Tramway |
| `5.56` | Metro, VAL, funicular |
| `5.57` | RER or suburban rail |
| `5.58` | TER |
| `5.59` | Other urban or regional public transport |
| `6` | Long-distance train or TGV |
| `6.60` | TGV, first class |
| `6.61` | TGV, second class |
| `6.62` | Other train, first class |
| `6.63` | Other train, second class |
| `6.69` | Train without detail |
| `7.70` | Plane |
| `8.80` | Boat |
| `9.90` | Other |
