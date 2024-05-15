# Processus d'installation


## Installer GitDesktop (ou GitHub Web)

 - Cloner le dossier mobility sur l'ordinateur avec l'URL : https://github.com/mobility-team/mobility.git
 - Fetch la branche school-model
 
 
## Installer Mamba en suivant les instruciton ci-dessous:

- Remplacement du script setup.py par un fichier [pyproject.toml](https://github.com/mobility-team/mobility/blob/stage-2-model/pyproject.toml) à l'emplacement d'enregistrement du dossier mobility, pour mettre à jour le process de création de package : https://packaging.python.org/en/latest/tutorials/packaging-projects/
- Création d'un fichier [environment.yml](https://github.com/mobility-team/mobility/blob/stage-2-model/environment.yml) auquel vous rajouterez les dépendances suivantes: geojson, openpyxl, pyarrow, py7zr, python-dotenv et spyder afin de gérer l'installation des dépendances non installables avec pip : R, osmium, geopandas...
- Pour le moment la dernière version de Miniforge n'est pas sur pypi, donc le process complet est le suivant :
	- Installer mamba avec Miniforge (https://github.com/conda-forge/miniforge):
		- Télécharger Miniforge (https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Windows-x86_64.exe)
		- Cocher "Create start menu shortcuts" and "Add Miniforge3 to my PATH environment variable". Attention, le deuxième n'est pas sélectionné d'office.
 
  	- Ouvrir Miniforge et aller dans le dossier qui contient le code du repo : cd path/to/mobility-repo.
        - Créer un environnement pour mobility à partir du fichier environment.yml : mamba env create -n mobility -f environment.yml.
        - Activer l'environnement mobility : mamba activate mobility.
        - Installer mobility avec pip : pip install -e .
  	 
  	       
  # Vérification de l'instalation et lancement du programme
  
  - Ouvrir Spyder dans le dossier mobility via Miniforge3.
  - Ouvrir le fichier C:\Users\Formation\Documents\GitHub\mobility\examples\Millau\pa.py pour tester l'installation en sélectionnant:
  	- Une variable *Age* en utilisant les valeurs 1, 2 et 3 pour accéder respectivement aux données d'écoles maternelles et primaires, des collèges ou des lycées
	- Une variable *model* où on pourra choisir les modèles de radiation (radiation), proximité (proximity) ou de carte scolaire (school_map)
  	- Une liste *dep* contenant le ou les départements sur lesquels vous voulez observer les déplacements scolaires.

Si vous obtenez une erreur sur l'import Mobility, vérifiez que votre fichier comporte les lignes suivantes: 
```python
import sys
sys.path.insert(0,"../..")
```
Si l'erreur ne vient pas de cela, relancez l'ordinateur ou bien si ce n'est pas suffisant, recommencez l'installation.
