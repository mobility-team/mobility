import mobility
import pandas as pd
import seaborn as sns
sns.set_theme()

trip_sampler = mobility.TripSampler()

# Generate one year of trips for a retired person,
# living alone in a city center, without a car 
retiree_trips = trip_sampler.get_trips(
  csp="7",
  csp_household="7",
  urban_unit_category="C",
  n_pers="1",
  n_cars="0",
  n_years=1
)

# Generate one year of trips for a worker living with an employee,
# in the suburbs, with two cars in the household
worker_trips = trip_sampler.get_trips(
  csp="6",
  csp_household="5",
  urban_unit_category="B",
  n_pers="3",
  n_cars="2",
  n_years=1
)


# Generate one year of trips for a group of persons with a given profile
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