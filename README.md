[![codecov](https://codecov.io/github/mobility-team/mobility/branch/main/graph/badge.svg?token=D31X32AZ43)](https://codecov.io/github/mobility-team/mobility)
[![Python package](https://github.com/mobility-team/mobility/actions/workflows/python-package.yml/badge.svg?branch=main)](https://github.com/mobility-team/mobility/actions/workflows/python-package.yml)
[![Code style: black][black-badge]][black-link]
[![Documentation Status][rtd-badge]][rtd-link]

# Mobility, an open-source library for mobility modelisation
Mobility is an open-source solution to compute the carbon emissions due to the mobility of a local population.

It is developed mainly by [AREP](https://arep.fr) and [Elioth](https://elioth.com/) with [ADEME](https://wiki.resilience-territoire.ademe.fr/wiki/Mobility) support, but anyone can join us!
For now, it is mainly focused on French territories.

[Documentation on mobility.readthedocs.io](https://mobility.readthedocs.io/en/latest/)

Find more infos (in French) on [Mobility website](https://mobility-team.github.io/)

# Mobility, une librairie open source pour la modélisation de la mobilité
Mobility est une solution open source servant à calculer l'empreinte carbone liée à la mobilité d'une population locale.


L'outil est principalement développé par [AREP](https://arep.fr) et [Elioth](https://elioth.com/) avec le soutien de l'[ADEME](https://wiki.resilience-territoire.ademe.fr/wiki/Mobility), mais toute personne peut nous rejoindre !
Pour l'instant, la solution est centrée sur les territoires et les données françaises.

[Documentation sur mobility.readthedocs.io](https://mobility.readthedocs.io/en/latest/)

Plus d'infos sur [le site web](https://mobility-team.github.io/) !

# Contributeur·ices
| Entreprise/école  | Participant·es |
| :------------- | :------------- |
| AREP  | Capucine-Marin Dubroca-Voisin <br> Antoine Gauchot <br> Félix Pouchain |
| Elioth  | Louise Gontier <br> Arthur Haulon  |
| École Centrale de Lyon | Anas Lahmar <br> Ayoub Foundou <br> Charles Pequignot <br> Lyes Kaya  <br> Zakariaa El Mhassani |

# Utilisations
| Utilisateur  | Date | Projet |
| :------------- | :------------- | :------------- |
| AREP  | 2020-2022 | [Luxembourg in Transition]([url](https://www.arep.fr/nos-projets/luxembourg-in-transition-paysage-capital/)) |
| AREP | En cours (2022) | Étude pour le [Grand Annecy]([url](https://www.arep.fr/nos-projets/grand-annecy/)) |

# Comment utiliser Mobility ?
_En cours de rédaction_

# Comment contribuer ?
* Vous pouvez regarder nos [issues](https://github.com/mobility-team/mobility/issues), particulièrement celles marquées comme [good-first-issue](https://github.com/mobility-team/mobility/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22), et proposer d'y contribuer.
* Tester l'outil et nous indiquer là où la documentation peut être améliorée est très utile ! Que ce soit pour une suggestion ou une issue, n'hésitez pas à [ouvrir une issue](https://github.com/mobility-team/mobility/issues/new).
* Nous espérons que vous pourrez utiliser Mobility pour vos travaux de recherche et de conseil ! Nous comptons sur vous pour partager le code que vous avez utilisé.
* Nous suivons PEP8 pour notre code Python. Pour d'autres bonnes pratiques, [suivez le guide](https://github.com/mobility-team/mobility/tree/main/mobility) !

[rtd-badge]: https://readthedocs.org/projects/mobility/badge/?version=latest
[rtd-link]: https://mobility.readthedocs.io/en/latest/?badge=latest
[black-badge]: https://img.shields.io/badge/code%20style-black-000000.svg
[black-link]: https://github.com/ambv/black

## Setting up a development environment with Docker
* Make sure you have [Docker installed](https://docs.docker.com/get-docker/) on your machine.
* Clone the repository to your local machine using Git. For example, you can run the following command in your terminal: 
`git clone https://github.com/mobility-team/mobility.git`
* Navigate to the project directory:
`cd mobility`
* Create a Docker container:
* For Windows:
`CMD: docker run -it --name mobility-dev -w /opt -v %cd%:/opt python:3.9 bash`
`PoweShell: docker run -it --name mobility-dev -w /opt -v ${PWD}:/opt python:3.9 bash`
* For Linux:
`docker run -it --name mobility-dev -w /opt -v $(PWD):/opt python:3.9 bash`
* Install the required dependencies:
`pip install -r requirements.txt && pip install -e .`
* Install the pytest package:
`pip install pytest`
* Run the unit tests to make sure everything is working properly:
`pytest`
* Starts a Docker container 
`docker container start -i mobility-dev`
