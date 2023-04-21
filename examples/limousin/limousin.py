import datetime
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mobility.get_insee_data import get_insee_data
import mobility.radiation_model as rm
from r5py import TravelTimeMatrixComputer, TransportMode
from r5py import TransportNetwork

# Load Limousin cities from IGN admin express data
# available at https://data.cquest.org/ign/adminexpress/
cities = gpd.read_file("D:/data/ign/admin_express/ADMIN-EXPRESS-COG_3-0__SHP__FRA_L93_2021-05-19/ADMIN-EXPRESS-COG_3-0__SHP__FRA_2021-05-19/ADMIN-EXPRESS-COG/1_DONNEES_LIVRAISON_2021-05-19\ADECOG_3-0_SHP_LAMB93_FR/COMMUNE.shp")
cities = cities[cities["INSEE_DEP"].isin(["19", "23", "87"])].copy()
cities["id"] = cities["INSEE_COM"]
cities_poly = cities.copy()
cities["geometry"] = cities.centroid
cities.to_crs("4326", inplace=True)
cities_poly.to_crs("4326", inplace=True)

# Load the number of workers and jobs fo each city
insee_data = get_insee_data()
workers = insee_data["active_population"]
jobs = insee_data["jobs"]

# Load the transport network and the GTFS data
# OSM data from http://download.geofabrik.de/europe/france.html
# GTFS from https://transport.data.gouv.fr/datasets/arrets-horaires-et-parcours-theoriques-des-reseaux-naq-lim-nva-m-1
transport_network = TransportNetwork(
    "D:/data/osm/limousin-latest.osm.pbf",
    [
        "D:/data/gtfs/ca_limoges_metropole-aggregated-gtfs.zip"
    ]
)

# Compute the travel times between all cities
# (chunk the request to 50-to-all cities, the request might be too large otherwise)
travel_times = []

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

for i, ids in enumerate(chunks(range(cities.shape[0]), 50)):
    
    print("Computing travel times for chunk : ", i)
    
    origins = cities.iloc[ids].copy()
    destinations = cities.copy()
    
    travel_time_matrix_computer = TravelTimeMatrixComputer(
        transport_network,
        origins=origins,
        destinations=destinations,
        departure=datetime.datetime(2023,4,25,8,0),
        transport_modes=[TransportMode.CAR],
        max_time=datetime.timedelta(hours=10)
    )
    
    tt = travel_time_matrix_computer.compute_travel_times()
    
    travel_times.append(tt)

travel_times = pd.concat(travel_times)


# Approximation : for same city trips, make the travel time half of the
# minimum travel time to other cities
travel_times_other_cities = travel_times[travel_times["from_id"] != travel_times["to_id"]]
min_travel_times = travel_times_other_cities.groupby("from_id", as_index=False)["travel_time"].min()

travel_times = pd.merge(travel_times, min_travel_times, on="from_id", suffixes=["", "_min"])

travel_times["travel_time"] = np.where(
    travel_times["from_id"] == travel_times["to_id"],
    travel_times["travel_time_min"]/2,
    travel_times["travel_time"]
)

travel_times.drop("travel_time_min", axis=1, inplace=True)

# Visualize travel times from Tulle to the other cities
city = cities.loc[cities["id"] == "19272"][["id", "geometry"]]

travel_times_from_city = pd.merge(
    cities_poly[["id", "geometry"]],
    travel_times[travel_times["from_id"] == city["id"].iloc[0]],
    left_on="id", right_on="to_id"
)

ax = travel_times_from_city.plot(column="travel_time", legend=True)
city.plot(ax=ax, color='red')
plt.show()


# Format data for the oportunity model : sources, sinks and transport costs
sources = pd.merge(
    cities,
    workers.reset_index(),
    left_on="id",
    right_on="CODGEO"
)

sources = sources[["id", "active_pop_CS6"]].copy()
sources.columns = ["transport_zone_id", "source_volume"]
sources.set_index("transport_zone_id", inplace=True)

sinks = pd.merge(
    cities,
    jobs.reset_index(),
    left_on="id",
    right_on="CODGEO"
)

sinks = sinks[["id", "n_jobs_CS3"]].copy()
sinks.columns = ["transport_zone_id", "sink_volume"]
sinks.set_index("transport_zone_id", inplace=True)

costs = travel_times.copy()
costs["cost"] = 10*costs["travel_time"]/60
costs = costs[["from_id", "to_id", "cost"]]
costs.columns = ["from", "to", "cost"]


# Run the radiation model
flows, _, _ = rm.iter_radiation_model(sources, sinks, costs)
flows = flows[flows > 0.0]
flows = flows.reset_index()


# Plot the resulting flows
coordinates = cities.copy()
coordinates.to_crs("2154", inplace=True)
coordinates["x"] = coordinates.geometry.x
coordinates["y"] = coordinates.geometry.y
coordinates = coordinates[["INSEE_COM", "NOM", "x", "y"]].copy()
coordinates.columns = ["CODGEO", "NOM_COM", "x", "y"]
coordinates.set_index("CODGEO", inplace=True)

rm.plot_flow(flows, coordinates, n_flows=1000, n_locations=20)


# Plot the distribution of travel times
dist = pd.merge(flows, travel_times, left_on=["from" ,"to"], right_on=["from_id", "to_id"])
dist["travel_time"].sample(n=100000, weights=dist["flow_volume"], replace=True).hist()
dist["travel_time"].sample(n=10000, weights=dist["flow_volume"], replace=True).describe()


# Make Limoges so attractive that people are willing to take 30 more min to get there
# (with time cost of 10 €/h, equivalent to removing 5 € to the total cost)
# and create 5000 jobs in the city
mod_costs = costs.copy()
mod_costs.loc[costs["to"] == "19272", "cost"] = mod_costs.loc[costs["to"] == "19272", "cost"] - 5
mod_sinks = sinks.copy()
mod_sinks.loc["19272", "sink_volume"] += 5000
flows, _, _ = rm.iter_radiation_model(sources, mod_sinks, mod_costs)
flows = flows[flows > 0.0]
flows = flows.reset_index()

rm.plot_flow(flows, coordinates, n_flows=1000, n_locations=20)

dist = pd.merge(flows, travel_times, left_on=["from" ,"to"], right_on=["from_id", "to_id"])
dist["travel_time"].sample(n=10000, weights=dist["flow_volume"], replace=True).describe()


# Equilibrate perfectly the active population and the number of jobs
# (= everybody works where they live)
mod_sinks = sources.copy()
mod_sinks.columns = ["sink_volume"]
flows, _, _ = rm.iter_radiation_model(sources, mod_sinks, costs)
flows = flows[flows > 0.0]
flows = flows.reset_index()

rm.plot_flow(flows, coordinates, n_flows=1000, n_locations=20)

dist = pd.merge(flows, travel_times, left_on=["from" ,"to"], right_on=["from_id", "to_id"])
dist["travel_time"].sample(n=10000, weights=dist["flow_volume"], replace=True).describe()
