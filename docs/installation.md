# Installation
## Installer les outils de développement
- Installer mamba avec [miniforge](https://github.com/conda-forge/miniforge), pour créer un environnement dédié pour mobility et gérer ses dépendances Python et R.
- Installer [git](https://git-scm.com/), pour pouvoir récupérer le code de mobility.
- Installer un environnement de développement, pour pouvoir utiliser la librairie dans des scripts. Vous pouvez par exemple utiliser [spyder](https://www.spyder-ide.org/) (idéalement dans un environnement mamba, voir les [instructions d'installation de spyder](https://docs.spyder-ide.org/current/installation.html#conda-environment)) ou [vs code](https://code.visualstudio.com/).

## Récuperer le code avec git
- Ouvrir une invite de commande, puis allez dans le dossier dans lequel vous souhaitez stocker le code de Mobility : `cd path/to/mobility-repo`
- Récupérer le code avec la commande : `git clone https://github.com/mobility-team/mobility.git`
- Passer sur la branche carpool : `git checkout carpool`

## Créer un environnement dédié et installer les dépendances de mobility
- Créer un environnement pour mobility à partir du fichier environment.yml : `mamba env create -n mobility -f environment.yml`
- Activer l'environnement mobility : `mamba activate mobility`
- Installer mobility avec pip : `pip install -e .`
- Si vous utilisez spyder, installez la librairie spyder-kernels : `pip install spyder-kernels`.
- Installer les dépendances R de mobility en lançant Python dans l'invite de commande, avec la commande `python`, puis :
```python
import mobility
mobility.set_params()
```

> Si votre code renvoie une erreur de type R indiquant qu'un package est manquant (généralement pak), il est possible que le téléchargement soit bloqué par votre proxy d'entreprise. Voici la procédure dans ce cas :
> * Dans une invite de commande, après avoir exécuté `mamba activate mobility`, utiliser la commande `R` pour entrer dans le terminal R.
> * Utiliser la commande `install.packages(c("dodgr","gtfsrouter", "sf", "geodist", "dplyr", "sfheaders", "nngeo", "data.table", "reshape2", "arrow", "stringr", "pbapply", "hms", "lubridate", "readxl", "pbapply"), method='wininet')` pour installer les packages R
> * Sur Windows, utiliser la commande `install.packages(file.choose(), repos=NULL)` et aller sélectionner le fichier ZIP `osmdata_0.2.5.005.zip` dans `mobility/mobility/resources/`, cela permet d'installer une version d'osmdata plus rapide (modifiée par nos soins).
> * Utiliser la commande `q()` pour quitter le terminal R.

> Si Spyder ne reconnaît pas l'environnement que vous avez créé même en lui spécifiant la bonne version de python dans Outils > Préférences > Interpreteur Python, vous pouvez :
> * Utiliser la commande `mamba install spyder` dans l'environnement `mobility`
> * Accepter les changements proposés
> * Lancer la commande `spyder` depuis l'environnement