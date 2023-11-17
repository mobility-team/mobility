# Premiers pas
## Installer Mobility
Mobility est disponible sur pypi. Pour installer le package dans votre environnement de travail :
```shell
pip install mobility-tools
```

## Générer un échantillon de déplacements pour un usager
Mobility permet de générer des échantillons de déplacements en fonction de plusieurs informations sur les usagers :
- Catégorie socioprofessionnelle.
- Catégorie professionnelle du ménage.
- Catégorie d'unité urbaine de son lieu de résidence principale.
- Taille du ménage.
- Nombre de voitures du ménage.

Pour générer un an de déplacements d'un retraité vivant seul, dans une ville centre, sans voiture, il est nécessaire d'initialiser un `TripSampler`, puis d'utiliser sa méthode `get_trips`, pour récuperer une dataframe pandas contenant les déplacements échantillonés.

```python
import mobility

trip_sampler = mobility.TripSampler()

trips = trip_sampler.get_trips(
  csp="7",
  csp_household="7",
  urban_unit_category="C",
  n_pers=1,
  n_cars="0",
  n_years=1
)
```