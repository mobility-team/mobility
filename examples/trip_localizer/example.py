import os
import dotenv
import mobility
import pandas as pd
from pathlib import Path
from mobility.parsers import download_work_home_flows
from mobility.radiation_departments import compute_similarity_index
import numpy as np

dotenv.load_dotenv()

mobility.set_params()

# Generate localized and non-localized trips for individuals sampled 
# from the population of each transport zone around Lyon 
# (takes ~ 30 min for now, reduce the radius and the sample size if you want faster results)



def find_best_alpha_beta_pair(insee_code, radius=25):
    """
    

    Parameters
    ----------
    insee_code : ste
        INSEE code of the central commune

    Returns
    -------
    best alpha, beta pair found for this commune and radius
    
    In the case of Bidart with a radius of 25, it's [0.8, 0.1]
    
    Beware, this function can take more than one hour to run
    """
    best_score = 0
    best_pair = [np.nan, np.nan]
    list_results = [[]]
    print("Finding the best α,β pair")
    for alpha in np.arange(0, 1.1, 0.1):
        for beta in np.arange(0, 1.1, 0.1):
            if alpha + beta < 1.05:
                print(f"\n\nα = {alpha:.1f}, β ={beta:.1f}")
                print("Testing radius", radius)
                
                transport_zones = mobility.TransportZones(insee_code, method="radius", radius=radius)
                population = mobility.Population(transport_zones, sample_size=1000)
                trips = mobility.Trips(population)
                loc_trips = mobility.LocalizedTrips(trips, alpha=alpha, beta=beta)
                
                # Load the dataframes in memory
                transport_zones_df = transport_zones.get()
                population_df = population.get()
                trips_df = trips.get()
                loc_trips_df = loc_trips.get()
                car_inputs = loc_trips.inputs["car_travel_costs"].get()
                walk_inputs = loc_trips.inputs["walk_travel_costs"].get()
                
                # Cap trying
                #print(loc_trips_df)
                loc_trips_df.to_clipboard()
                #trips_by_od_and_mode = loc_trips_df.groupby(["from_transport_zone_id","to_transport_zone_id","mode_id"])["distance"].count()
                #loc_trips_df["from_transport_zone_id"] = loc_trips_df["from_transport_zone_id"].astype("int", errors="ignore")
                #loc_trips_df["to_transport_zone_id"] = loc_trips_df["to_transport_zone_id"].astype("int", errors="ignore")
             
                # Similarity
                
                ## Model
                predicted_flow = loc_trips_df.groupby(["from_transport_zone_id","to_transport_zone_id"])["distance"].count()
                predicted_flow = predicted_flow.reset_index()
                predicted_flow["from_transport_zone_id"] = predicted_flow["from_transport_zone_id"].astype("int")
                predicted_flow["to_transport_zone_id"] = predicted_flow["to_transport_zone_id"].astype("int")
                predicted_flow = predicted_flow.merge(transport_zones_df[["transport_zone_id", "admin_id", "name"]], left_on="from_transport_zone_id",
                                                      right_on="transport_zone_id") 
                predicted_flow = predicted_flow.merge(transport_zones_df[["transport_zone_id", "admin_id", "name"]], left_on="to_transport_zone_id",
                                                      right_on="transport_zone_id", suffixes=["_from","_to"])
                predicted_flow = predicted_flow.rename(columns={"distance": "flow_volume"})
                predicted_flow = predicted_flow.set_index(["admin_id_from","admin_id_to"])
                
    
                ## INSEE
                lst_departments = ["64", "40"]
                work_home_fluxes_csv = download_work_home_flows()    
                data_folder_path = Path(os.path.dirname(__file__)).joinpath("data").joinpath("insee").joinpath("territories")
            
                # Import the INSEE data on the work-home mobility on Millau
                file_path = os.path.join(data_folder_path, work_home_fluxes_csv)
                raw_flowDT = pd.read_csv(
                    file_path,
                    sep=";",
                    usecols=["COMMUNE", "DCLT", "IPONDI", "TRANS"],
                    dtype={"COMMUNE": str, "DCLT": str, "IPONDI": float, "TRANS": int},
                )
            
                # Only keep the flows in the given departments
            
                raw_flowDT["DEP"] = raw_flowDT["COMMUNE"].str.slice(0, 2)
                raw_flowDT["DEP2"] = raw_flowDT["DCLT"].str.slice(0, 2)
                mask = raw_flowDT["DEP"].apply(lambda x: x in lst_departments)
                mask2 = raw_flowDT["DEP2"].apply(lambda x: x in lst_departments)
                raw_flowDT = raw_flowDT.loc[mask]
                raw_flowDT = raw_flowDT.loc[mask2]
                
                empirical_flow = raw_flowDT.rename(
                    columns={"IPONDI": "flow_volume", "COMMUNE": "from", "DCLT": "to"}
                )
                empirical_flow = empirical_flow.groupby(["from", "to"])["flow_volume"].sum()
                empirical_flow = pd.DataFrame(empirical_flow)
                
                ## Compare
                ssi = compute_similarity_index(predicted_flow["flow_volume"], empirical_flow, threshold=100)
                list_results += [[[alpha, beta],ssi]]
                if ssi > best_score:
                    best_score = ssi
                    best_pair = [alpha, beta]
    print("Best α,β pair found is", best_pair)
    
    return best_pair


transport_zones = mobility.TransportZones("64125", method="radius", radius=25)
population = mobility.Population(transport_zones, sample_size=10000)
trips = mobility.Trips(population)
loc_trips = mobility.LocalizedTrips(trips, alpha=0.8, beta=0.1)

# Load the dataframes in memory
transport_zones_df = transport_zones.get()
population_df = population.get()
trips_df = trips.get()
loc_trips_df = loc_trips.get()

# car_inputs = loc_trips.inputs["car_travel_costs"].get()
# walk_inputs = loc_trips.inputs["walk_travel_costs"].get()
             





# predicted_flow = loc_trips_df.groupby(["from_transport_zone_id","to_transport_zone_id"])["distance"].count()
# predicted_flow = predicted_flow.reset_index()
# predicted_flow["from_transport_zone_id"] = predicted_flow["from_transport_zone_id"].astype("int")
# predicted_flow["to_transport_zone_id"] = predicted_flow["to_transport_zone_id"].astype("int")
# predicted_flow = predicted_flow.merge(transport_zones_df[["transport_zone_id", "admin_id", "name"]], left_on="from_transport_zone_id",
#                                       right_on="transport_zone_id") 
# predicted_flow = predicted_flow.merge(transport_zones_df[["transport_zone_id", "admin_id", "name"]], left_on="to_transport_zone_id",
#                                       right_on="transport_zone_id", suffixes=["_from","_to"])
# predicted_flow = predicted_flow.rename(columns={"distance": "flow_volume"})
# predicted_flow = predicted_flow.set_index(["admin_id_from","admin_id_to"])

    

# # Modal choice by distance
# loc_trips_df["int_distance"] = loc_trips_df["distance"].astype("int")
# mode_by_distance = loc_trips_df.groupby(["int_distance","mode_id"])["distance"].count()
# print(mode_by_distance)
# mode_by_distance.to_clipboard()


# trips_from_center_city = loc_trips_df.groupby(["from_transport_zone_id","mode_id"],as_index=False)["distance"].count()
# trips_from_center_city["from_transport_zone_id"] = trips_from_center_city["from_transport_zone_id"].astype('int').astype('str')
# print(trips_from_center_city)
# trips_from_center_city = trips_from_center_city.set_index(["from_transport_zone_id","mode_id"]).loc["0"]
# print(trips_from_center_city)


# mode_sum = trips_from_center_city.loc["bicycle"] + trips_from_center_city.loc["bus+bus"] + trips_from_center_city.loc["car"] + trips_from_center_city.loc["walk"]

# print(int(mode_sum))

# results = [[]]
# res_list=[]
# for mode in ["bicycle","bus+bus", "car", "walk"]:    
#     modal_share = float(trips_from_center_city.loc[mode] / mode_sum)
#     res_list+=[modal_share]
#     print (mode, "modal share:", modal_share)

# results += [res_list]
    


# print(results)


# trips_by_mode = loc_trips_df.groupby("mode_id")["distance"].count()
# print(trips_by_mode)

# # Compute the localized and non-localized total travelled distance by each individual
# trips_df = trips_df.groupby("individual_id", as_index=False)["distance"].sum()
# loc_trips_df = loc_trips_df.groupby("individual_id", as_index=False)["distance"].sum()



# # Compare the two total distances
# comparison = pd.merge(trips_df, loc_trips_df, on="individual_id", suffixes=["", "_localized"])
# comparison["variation"] = comparison["distance_localized"]/comparison["distance"] - 1.0
# comparison["variation"].describe()
# comparison["variation"].hist(bins=30)

# # Plot the average distance by transport zone
# trips_df = pd.merge(trips_df, population_df, on="individual_id")
# distance_by_tz = trips_df.groupby("transport_zone_id", as_index=False)["distance"].mean()

# loc_trips_df = pd.merge(loc_trips_df, population_df, on="individual_id")
# loc_distance_by_tz = loc_trips_df.groupby("transport_zone_id", as_index=False)["distance"].mean()

# distance_map = pd.merge(transport_zones_df, distance_by_tz, on="transport_zone_id")
# distance_map.plot("distance", legend=True)

# loc_distance_map = pd.merge(transport_zones_df, loc_distance_by_tz, on="transport_zone_id")
# loc_distance_map.plot("distance", legend=True)

# comparison = pd.merge(comparison, population_df, on="individual_id")
# comparison = comparison.groupby("transport_zone_id")["distance_localized"].sum()/comparison.groupby("transport_zone_id")["distance"].sum()
# comparison = comparison.reset_index()
# comparison_map = pd.merge(transport_zones_df, comparison, on="transport_zone_id")
# comparison_map.plot(0, legend=True)



# Compute the modal share of each OD
modal_share = loc_trips_df.groupby(["from_transport_zone_id", "to_transport_zone_id", "mode_id"])["trip_id"].count()
modal_share = modal_share/modal_share.groupby(["from_transport_zone_id", "to_transport_zone_id"]).sum()
modal_share.name = "modal_share"

modal_share_biarritz = pd.merge(
    transport_zones_df,
    modal_share.xs(0).xs("walk", level=1).reset_index(),
    left_on="transport_zone_id",
    right_on="to_transport_zone_id"
)

modal_share_biarritz.plot("modal_share", legend=True)
