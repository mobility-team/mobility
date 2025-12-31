import os
import dotenv
import mobility
import polars as pl

dotenv.load_dotenv()


mobility.set_params(
    # package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    # project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
        package_data_folder_path="D:/mobility-data",
        project_data_folder_path="D:/test-09",
        debug=True
)

transport_zones = mobility.TransportZones("fr-09122", radius = 40, level_of_detail=1)
tz = transport_zones.get()
emp = mobility.EMPMobilitySurvey()
pop = mobility.Population(transport_zones, sample_size = 10000)
modes = [mobility.CarMode(transport_zones), mobility.WalkMode(transport_zones), mobility.BicycleMode(transport_zones)]
surveys = [emp]
motives = [mobility.HomeMotive(), mobility.WorkMotive(), mobility.OtherMotive(population=pop)]

list_seeds = [0, 42, 64, 1000, 8888, 9999, 67654, 3333333333, 56563563208387, 7345678900000743]
print(len(list_seeds))
list_pop = []

pop_trips_base = mobility.PopulationTrips(
    pop,
    modes,
    motives,
    surveys,
    parameters=mobility.PopulationTripsParameters(n_iterations=6, seed=0, k_mode_sequences=6)
    )
labels=pop_trips_base.get_prominent_cities()


for seed in list_seeds:
    print("\n SEED:", seed, "\n")
    pop_trips = mobility.PopulationTrips(
        pop,
        modes,
        motives,
        surveys,
        parameters=mobility.PopulationTripsParameters(n_iterations=6, seed=seed, k_mode_sequences=6)
        )
    cost_per_person = pop_trips.evaluate("cost_per_person", plot_delta=False, compare_with=pop_trips_base, labels=labels)
    #dist_per_person = pop_trips.evaluate("distance_per_person", plot_delta=True, compare_with=pop_trips_base)
    #time_per_person = pop_trips.evaluate("time_per_person", plot_delta=True, compare_with=pop_trips_base)
    #ghg_per_person = pop_trips.evaluate("ghg_per_person", plot_delta=True, compare_with=pop_trips_base)
    list_pop.append(pop_trips)
    
    
cost_all = list_pop[0].evaluate("cost_per_person").select("transport_zone_id", "cost", "n_persons").group_by("transport_zone_id").agg(pl.col("cost").sum(), pl.col("n_persons").sum())
i = 1
for pop in list_pop[1:10]:
    cost_add = pop.evaluate("cost_per_person").select("transport_zone_id", "cost", "n_persons").group_by("transport_zone_id").agg(pl.col("cost").sum(), pl.col("n_persons").sum())
    suffix = "_" + str(i)
    cost_all = cost_all.join(cost_add, on="transport_zone_id", suffix=suffix)
    i += 1
    
cost_all = cost_all.sort(by="cost")

pandas_cost=cost_all.select(pl.col("^cost.*$")).to_pandas()
pandas_cost.plot()


distance_all = list_pop[0].evaluate("distance_per_person").select("transport_zone_id", "distance", "n_persons").group_by("transport_zone_id").agg(pl.col("distance").sum(), pl.col("n_persons").sum())
i = 1
for pop in list_pop[1:10]:
    distance_add = pop.evaluate("distance_per_person").select("transport_zone_id", "distance", "n_persons").group_by("transport_zone_id").agg(pl.col("distance").sum(), pl.col("n_persons").sum())
    suffix = "_" + str(i)
    distance_all = distance_all.join(distance_add, on="transport_zone_id", suffix=suffix)
    i += 1
    
distance_all = distance_all.sort(by="distance")

pandas_distance=distance_all.select(pl.col("^distance.*$")).to_pandas()
pandas_distance.plot()

time_all = list_pop[0].evaluate("time_per_person").select("transport_zone_id", "time", "n_persons").group_by("transport_zone_id").agg(pl.col("time").sum(), pl.col("n_persons").sum())
i = 1
for pop in list_pop[1:10]:
    time_add = pop.evaluate("time_per_person").select("transport_zone_id", "time", "n_persons").group_by("transport_zone_id").agg(pl.col("time").sum(), pl.col("n_persons").sum())
    suffix = "_" + str(i)
    time_all = time_all.join(time_add, on="transport_zone_id", suffix=suffix)
    i += 1
    
time_all = time_all.sort(by="time")

pandas_time=time_all.select(pl.col("^time.*$")).to_pandas()
pandas_time.plot()

for pop in list_pop:
    pop.plot_od_flows()