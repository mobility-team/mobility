# Definitions
Some definitions and tables useful to understand Mobility data!

## France
France's statistical institute INSEE uses socioprofessional categories (Catégories socioprofessionnelles, or *CSP*) to describe the population.
It also uses categories for the urban setting of communes and journeys' motives and modes.
This documentation is only available in French.

### Catégories socioprofessionnelles
| Code CSP | CSP |
| - | - |
| 1 | Agriculteurs exploitants |
| 2 | Artisans, commerçants, chefs d'entreprise |
| 3 | Cadres et professions intellectuelles supérieures |
| 4 | Professions intermédiaires |
| 5 | Employés |
| 6 | Ouvriers |
| 7 | Retraités |
| 8 | Inactifs |
| no_csp | Sans CSP |

Définition INSEE : https://www.insee.fr/fr/metadonnees/definition/c1493

### Catégorie de l'unité urbaine
| Code | Catégorie de l'unité urbaine |
| - | - |
| C | Ville-centre de l'unité urbaine |
| B | Banlieue de l'unité urbaine |
| I | Ville isolé de l'unité urbaine |
| R | Espace rural, hors unité urbaine |

Définition INSEE : https://www.insee.fr/fr/metadonnees/definition/c1501

### Motif du déplacement
| Code | Motif |
|------|-------|
| 1 à 8 | MOTIFS PRIVÉS |
| 1.1 | Aller au domicile |
| 1.2 | Retour à la résidence occasionnelle |
| 1.3 | Retour au domicile de parents (hors ménage) ou d'amis |
| 1.11 | Etudier (école, lycée, université) |
| 1.12 | Faire garder un enfant en bas âge (nourrice, crèche, famille) |
| 2 | ACHATS |
| 2.20 | Se rendre dans une grande surface ou un centre commercial (y compris boutiques et services) |
| 2.21 | Se rendre dans un commerce de proximité, petit commerce, supérette, boutique, services (banque, cordonnier...) (hors centre commercial) |
| 3 | SOINS |
| 3.31 | Soins médicaux ou personnels (médecin, coiffeur...) |
| 4 | Démarches |
| 4.41 | Démarche administrative, recherche d'informations |
| 5 | VISITES |
| 5.51 | Visite à des parents |
| 5.52 | Visite à des amis |
| 6 | ACCOMPAGNER OU ALLER CHERCHER |
| 6.61 | Accompagner quelqu'un à la gare, à l'aéroport, à une station de métro, de bus, de car |
| 6.62 | Accompagner quelqu'un à un autre endroit |
| 6.63 | Aller chercher quelqu'un à la gare, à l'aéroport, à une station de métro, de bus, de car |
| 6.64 | Aller chercher quelqu'un à un autre endroit |
| 7 | LOISIRS |
| 7.71 | Activité associative, cérémonie religieuse, réunion |
| 7.72 | Aller dans un centre de loisirs, parc d'attraction, foire |
| 7.73 | Manger ou boire à l'extérieur du domicile |
| 7.74 | Visiter un monument ou un site historique |
| 7.75 | Voir un spectacle culturel ou sportif (cinéma, théâtre, concert, cirque, match) |
| 7.76 | Faire du sport |
| 7.77 | Se promener sans destination précise |
| 7.78 | Se rendre sur un lieu de promenade |
| 8 | VACANCES, CHANGER DE RÉSIDENCE ET " AUTRES MOTIFS PRIVÉS " |
| 8.80 | Vacances hors résidence secondaire |
| 8.81 | Se rendre dans une résidence secondaire |
| 8.82 | Se rendre dans une résidence occasionnelle |
| 8.89 | Autres motifs personnels |
| 9 | MOTIFS PROFESSIONNELS |
| 9.91 | Travailler dans son lieu fixe et habituel |
| 9.92 | Travailler en dehors d'un lieu fixe et habituel, sauf tournée (chantier, contacts professionnels, réunions, visite à des clients ou fournisseurs, repas d'affaires...) |
| 9.94 | Stage, conférence, congrès, formations, exposition |
| 9.95 | Tournées professionnelles (VRP) ou visites de patients |
| 9.96 | Autres motifs professionnels |

### Mode du déplacement
| Code | Label |
|------|-------|
| 1 | Piéton |
| 1.10 | Uniquement marche à pied |
| 1.11 | Porté, transporté en poussette |
| 1.12 | Rollers, trottinette |
| 1.13 | Fauteuil roulant (y compris motorisé) |
| 2 | Deux roues |
| 2.20 | Bicyclette, tricycle (y compris à assistance électrique) |
| 2.22 | Cyclomoteur (2 roues moins de 50 cm3) - Conducteur |
| 2.23 | Cyclomoteur (2 roues moins de 50 cm3) - Passager |
| 2.24 | Moto (plus de 50 cm3) - Conducteur (y compris avec side-car) |
| 2.25 | Moto (plus de 50 cm3) - Passager (y compris avec side-car) |
| 2.29 | Motocycles sans précision (y compris quads) |
| 3 | Automobile |
| 3.30 | Voiture, VUL, voiturette... - Conducteur seul |
| 3.31 | Voiture, VUL, voiturette... - Conducteur avec passager |
| 3.32 | Voiture, VUL, voiturette... - Passager |
| 3.33 | Voiture, VUL, voiturette... - Tantôt conducteur tantôt passager |
| 3.39 | Trois ou quatre roues sans précision |
| 4 | Transport spécialisé, scolaire, taxi |
| 4.40 | Taxi (individuel, collectif) |
| 4.41 | Transport spécialisé (handicapé) |
| 4.42 | Ramassage organisé par l'employeur |
| 4.43 | Ramassage scolaire |
| 5 | Transport en commun urbain ou régional |
| 5.50 | Autobus urbain, trolleybus |
| 5.51 | Navette fluviale |
| 5.52 | Autocar de ligne (sauf SNCF) |
| 5.53 | Autre autocar (affrètement, service spécialisé) |
| 5.54 | Autocar SNCF |
| 5.55 | Tramway |
| 5.56 | Métro, VAL, funiculaire |
| 5.57 | RER, SNCF banlieue |
| 5.58 | TER |
| 5.59 | Autres transports urbains & régionaux (sans précision) |
| 6 | Train grande ligne ou TGV |
| 6.60 | TGV, 1ère classe |
| 6.61 | TGV, 2ème classe |
| 6.62 | Autre Train, 1ère classe |
| 6.63 | Autre Train, 2ème classe |
| 6.69 | Train sans précision |
| 7.70 | Avion |
| 8.80 | Bateau |
| 9.90 | Autre |

## Switzerland

To do!
