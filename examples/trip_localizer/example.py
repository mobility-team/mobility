import os
import dotenv
import mobility
import pandas as pd

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path="D:/data/mobility/projects/lyon"
)

# Generate localized and non-localized trips for individuals sampled 
# from the population of each transport zone around Lyon 
# (takes ~ 30 min for now, reduce the radius and the sample size if you want faster results)
transport_zones = mobility.TransportZones("69383", method="radius", radius=30.0)
population = mobility.Population(transport_zones, sample_size=10000)
trips = mobility.Trips(population)
loc_trips = mobility.LocalizedTrips(trips)

# Load the dataframes in memory
transport_zones_df = transport_zones.get()
population_df = population.get()
trips_df = trips.get()
loc_trips_df = loc_trips.get()

# Compute the localized and non-localized total travelled distance by each individual
trips_df = trips_df.groupby("individual_id", as_index=False)["distance"].sum()
loc_trips_df = loc_trips_df.groupby("individual_id", as_index=False)["distance"].sum()

# Compare the two total distances
comparison = pd.merge(trips_df, loc_trips_df, on="individual_id", suffixes=["", "_localized"])
comparison["variation"] = comparison["distance_localized"]/comparison["distance"] - 1.0
comparison["variation"].describe()
comparison["variation"].hist(bins=30)

# Plot the average distance by transport zone
trips_df = pd.merge(trips_df, population_df, on="individual_id")
distance_by_tz = trips_df.groupby("transport_zone_id", as_index=False)["distance"].mean()

loc_trips_df = pd.merge(loc_trips_df, population_df, on="individual_id")
loc_distance_by_tz = loc_trips_df.groupby("transport_zone_id", as_index=False)["distance"].mean()

distance_map = pd.merge(transport_zones_df, distance_by_tz, on="transport_zone_id")
distance_map.plot("distance", legend=True)

loc_distance_map = pd.merge(transport_zones_df, loc_distance_by_tz, on="transport_zone_id")
loc_distance_map.plot("distance", legend=True)

comparison = pd.merge(comparison, population_df, on="individual_id")
comparison = comparison.groupby("transport_zone_id")["distance_localized"].sum()/comparison.groupby("transport_zone_id")["distance"].sum()
comparison = comparison.reset_index()
comparison_map = pd.merge(transport_zones_df, comparison, on="transport_zone_id")
comparison_map.plot(0, legend=True)
