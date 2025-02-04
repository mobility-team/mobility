# Installation
- Installer mamba avec [miniforge](https://github.com/conda-forge/miniforge).
- Aller dans le dossier qui contient le code du repo : `cd path/to/mobility-repo`.
- Créer un environnement pour mobility à partir du fichier environment.yml : `mamba env create -n mobility -f environment.yml`.
- Activer l'environnement mobility : `mamba activate mobility`.
- Installer mobility avec pip : `pip install -e .`.
- Vous pouvez utiliser Spyder en indiquant bien l'environnement que vous avez créé comme environnement à utiliser
- Importer mobility dans votre code avec `import mobility` (script d'exemple [ici](https://github.com/mobility-team/mobility/blob/main/examples/trip_localizer_detailed_steps/trip_localizer_detailed_steps.py)).
- Il faut appeler `mobility.setup` avant de pouvoir utiliser mobility : la fonction va fixer plusieurs variables d'environnement qui peuvent être nécessaires (où stocker les fichiers temporaires, info sur le proxy pour les requêtes http...) et installer si besoin les packages R.
> Si votre code renvoie une erreur de type R indiquant qu'un package est manquant (généralement pak), il est possible que le téléchargement soit bloqué par votre proxy d'entreprise. C'est notamment le cas chez AREP, voici la procédure dans ce cas :
> * Dans le terminal Miniforge Prompt, après avoir exécuté `mamba activate mobility`, utiliser la commande  `R` pour entrer dans le terminal R
> * Utiliser la commande `install.packages(c("dodgr","gtfsrouter", "sf", "geodist", "dplyr", "sfheaders", "nngeo", "data.table", "reshape2", "arrow", "stringr", "pbapply", "hms", "lubridate", "readxl", "pbapply"), method='wininet')` pour installer les packages R
> * Sur Windows, utiliser la commande `install.packages(file.choose(), repos=NULL)` et aller sélectionner le fichier ZIP `osmdata_0.2.5.005.zip` dans `mobility/mobility/resources/`, cela permet d'installer une version d'osmdata plus rapide (modifiée par nos soins).
> * Utiliser la commande `q()` pour quitter le terminal R.

> Si Spyder ne reconnaît pas l'environnement que vous avee créé, vous pouvez :
> * Utiliser la commande `mamba install spyder` dans l'environnement `mobility`
> * Accepter les changements proposés
> * Lancer la commande `spyder` depuis l'environnement