# Installation
## Installer les outils de développement
- Installer mamba avec [miniforge](https://github.com/conda-forge/miniforge), pour créer un environnement dédié pour mobility et gérer ses dépendances Python et R.
- Installer [git](https://git-scm.com/), pour pouvoir récupérer le code de mobility.
- Installer un environnement de développement, pour pouvoir utiliser la librairie dans des scripts. Vous pouvez par exemple utiliser [spyder](https://www.spyder-ide.org/) (idéalement dans un environnement mamba, voir les [instructions d'installation de spyder](https://docs.spyder-ide.org/current/installation.html#conda-environment)) ou [vs code](https://code.visualstudio.com/).

## Récupérer le code avec git
Vous pouvez soit utiliser git directement soit GitHub Desktop.

### Avec git
- Ouvrir une invite de commande, puis allez dans le dossier dans lequel vous souhaitez stocker le code de Mobility : `cd path/to/mobility-repo`. Sous Windows, choisissez un dossier standard (par exemple "C:\Users\username\Documents\GitHub\mobility"), l'utilisation de dossiers système peut bloquer l'installation.
- Récupérer le code avec la commande : `git clone https://github.com/mobility-team/mobility.git`
- Si nécessaire, se placer sur la branche main : `git checkout main`

### Avec GitHub Desktop
- Installer et ouvrir GitHub Desktop
- File > Clone repository > URL > `mobility-team/mobility`. Sous Windows, choisissez un dossier standard (par exemple `C:\Users\username\Documents\GitHub\mobility`), l'utilisation de dossiers système peut bloquer l'installation.
- Rester sur la branche main

## Créer un environnement dédié et installer les dépendances de mobility
- Ouvrir Miniforge Prompt et se placer dans le dossier où a été installé mobility ( `cd path/to/mobility-repo`)
- Créer un environnement pour mobility à partir du fichier environment.yml : `mamba env create -n mobility -f environment.yml`
- Activer l'environnement mobility : `mamba activate mobility`
- Installer mobility avec pip : `pip install -e .`
- Si vous utilisez spyder, installez la librairie spyder-kernels : `pip install spyder-kernels`
- Installer les dépendances R de mobility en lançant Python dans l'invite de commande, avec la commande `python`, puis :
```python
import mobility
mobility.set_params(debug=True)
```
- Vous pouvez sortir du terminal Python avec `exit()`

> Si votre code renvoie une erreur de type R indiquant qu'un package est manquant (généralement pak), il est possible que le téléchargement soit bloqué par votre proxy d'entreprise. Voici la procédure dans ce cas si vous êtes sur Windows :

> Relancer la commande d'installation en changeant de méthode de téléchargement `mobility.set_params(debug=True, r_packages_download_method="wininet")`. Pak devrait pouvoir s'installer correctement, puis installer les autres packages.
> Si cela ne fonctionne toujours pas, essayer d'installer les packages manuellement :
> * Dans une invite de commande, après avoir exécuté `mamba activate mobility`, utiliser la commande `R` pour entrer dans le terminal R.
> * Dans un premier temps, utiliser la commande `install.packages('pak')`, puis `install.packages('pak', method='wininet')` si la première n'a pas marché.
> * Utiliser la commande `q()` pour quitter le terminal R. Retenter l'utilisation de `mobility.set_params(debug=True)`.
> * Si cela ne suffit pas, utiliser (à nouveau dans R) la commande `install.packages(c('remotes', 'dodgr', 'sf', 'geodist', 'dplyr', 'sfheaders', 'nngeo', 'data.table', 'reshape2', 'arrow', 'stringr', 'hms', 'lubridate', 'codetools', 'future', 'future.apply', 'ggplot2', 'svglite', 'cppRouting', 'duckdb', 'jsonlite', 'gtfsrouter', 'geos', 'FNN', 'cluster', 'dbscan'), method='wininet')` pour installer les packages R.
> * Sur Windows, utiliser la commande `install.packages(file.choose(), repos=NULL)` et aller sélectionner le fichier ZIP `osmdata_0.2.5.005.zip` dans `mobility/mobility/resources/`, cela permet d'installer une version d'osmdata plus rapide (modifiée par nos soins).
> * Utiliser la commande `q()` pour quitter le terminal R.

Pour faire fonctionner Mobility, vous pouvez ensuite utiliser un IDE comme Spyder ou VS Code.

### Spyder
Lancer Spyder et bien spécifier comme interpréteur celui que vous avez créé. Sur Windows, il peut se trouver dans le dossier `C:\Users\dubrocac\AppData\Local\miniforge3\envs\mobility`.
> Si Spyder ne reconnaît pas l'environnement que vous avez créé même en lui spécifiant la bonne version de python dans Outils > Préférences > Interpreteur Python, vous pouvez :
> * Utiliser la commande `mamba install spyder` dans l'environnement `mobility`
> * Accepter les changements proposés
> * Lancer la commande `spyder` depuis l'environnement

### VS Code
Lancer VS Code et bien spécifier comme interpréteur celui que vous avez créé. Sur Windows, il peut se trouver dans le dossier `C:\Users\dubrocac\AppData\Local\miniforge3\envs\mobility`.

## Erreurs communes
### L'étape 'Parsing OSM data' prend trop de temps
Sur Windows, assurez-vous de bien utiliser le package `osmdata_0.2.5.005.zip` fourni dans Mobility.
- Pour obtenir une liste des packages existants, utilisez la commande :
```ip = as.data.frame(installed.packages()[,c(1,3:4)])
ip = ip[is.na(ip$Priority),1:2,drop=FALSE]
ip
```
- Si la version d'osmdata est déjà 0.2.5.005, votre configuration est correcte et le problème est ailleurs !
- Sinon, utiliser la commande `install.packages(file.choose(), repos=NULL)` et aller sélectionner le fichier ZIP `osmdata_0.2.5.005.zip` dans `mobility/mobility/resources/`.
- Utiliser la commande `q()` pour quitter le terminal R.

### Autres problèmes
Les problèmes actuellement rencontrés sont [listés comme bugs](https://github.com/mobility-team/mobility/issues?q=is%3Aissue%20state%3Aopen%20label%3Abug). N'hésitez pas à les regarder si besoin et à [soumettre une nouvelle issue](https://github.com/mobility-team/mobility/issues/new) si nécessaire.
