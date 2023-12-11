# Premiers pas
## Installer Mobility
Utilisez pypi pour installer le package dans votre environnement de travail :
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

Pour générer un an de déplacements d'un usager, il est nécessaire auparavant de créer un `TripSampler`, puis d'utiliser sa méthode `get_trips`, pour récuperer une dataframe pandas contenant les déplacements échantillonnés. Par exemple pour un retraité vivant seul, dans une ville centre, sans voiture :

```python
import mobility

trip_sampler = mobility.TripSampler()

retiree_trips = trip_sampler.get_trips(
  csp="7",
  csp_household="7",
  urban_unit_category="C",
  n_pers="1",
  n_cars="0",
  n_years=1
)
```

Pour un ouvrier vivant avec une employée en banlieue, avec deux voitures :

```python
worker_trips = trip_sampler.get_trips(
  csp="6",
  csp_household="5",
  urban_unit_category="B",
  n_pers="3",
  n_cars="2",
  n_years=1
)
```

## Générer un échantillon de déplacements pour un groupe d'usagers
Étant donné que Mobility procède par échantillonnage, les déplacements de chaque profil vont varier à chaque exécution du code. Il convient donc de générer un nombre suffisant de profils avant de pour comparer deux profils ou deux populations, en vérifiant la convergence des indicateurs étudiés : nombre total de déplacements, distance totale parcourue, détail par mode, par motif...

Reprenons nos deux profils précédents, en échantillonnant cette fois (de manière arbitraire) 100 personnes de chaque profil :

```python
def sample_n_persons(n, csp, csp_household, urban_unit_category, n_pers, n_cars, n_years):

    all_trips = []
    
    for i in range(n):
        
        trips = trip_sampler.get_trips(
          csp,
          csp_household,
          urban_unit_category,
          n_pers,
          n_cars
        )
        
        trips["individual_id"] = i
        
        all_trips.append(trips)

    all_trips = pd.concat(all_trips)
    
    return all_trips

group_A_trips = sample_n_persons(
    n=100,
    csp="7",
    csp_household="7",
    urban_unit_category="C",
    n_pers="1",
    n_cars="0",
    n_years=1
)

group_B_trips = sample_n_persons(
    n=100,
    csp="6",
    csp_household="5",
    urban_unit_category="B",
    n_pers="3",
    n_cars="2",
    n_years=1
)

group_A_trips["group"] = "A"
group_B_trips["group"] = "B"

trips = pd.concat([group_A_trips, group_B_trips])
```

On peut maintenant comparer les comportements de mobilité de nos deux profils. 

```python
# Compute and plot the total distance travelled, for each individual in each group
total_distance = trips.groupby(["group", "individual_id"], as_index=False)["distance"].sum()
sns.catplot(data=total_distance, x="group", y="distance", kind="box")

# Group modes and motives by broad category
trips["mode_group"] = trips["mode_id"].str[0:1]
trips["motive_group"] = trips["motive"].str[0:1]

# Compute and plot the total distance travelled by mode, for each individual in each group
total_distance_by_mode = trips.groupby(["group", "mode_group", "individual_id"], as_index=False)["distance"].sum()
sns.catplot(data=total_distance_by_mode, x="mode_group", y="distance", hue="group", kind="box")

# Compute and plot the total distance travelled by motive, for each individual in each group
total_distance_by_motive = trips.groupby(["group", "motive_group", "individual_id"], as_index=False)["distance"].sum()
sns.catplot(data=total_distance_by_motive, x="motive_group", y="distance", hue="group", kind="box")
```

Dans notre premier profil de retraité, la plupart des personnes parcourraient environ 6000 km/an, contre 17 000 km/an pour notre second profil de 17 000 km/an. Certains retraités se déplaceraient cependant plus de 20 000 km/an, plus que la plupart des ouvriers !

La plus grosse différence vient de l'utilisation de la voiture (mode 3) : 2000 km/an pour le premier profil, 13 000 km/an pour le second. Et sans surprise ce sont les déplacements liés au travail (motif 9 "professionnels") qui font la différence : 8 000 km/an pour les ouvriers.

| Distance totale par groupe | Distance totale par groupe et par mode | Distance totale par groupe et par motif |
|:----------------------:|:----------------------:|:----------------------:|
| ![](quickstart/total_distance.png) | ![](quickstart/total_distance_by_mode.png) | ![](quickstart/total_distance_by_motive.png) |

