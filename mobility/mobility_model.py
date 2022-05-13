import os
import time
from pathlib import Path
import logging

import pandas as pd
import numpy as np

class MobilityModel:
    
    def __init__(self, sources, sinks, locations, trip_cost, default_trip_cost, max_distance, alpha, beta):
        self.sources = sources
        self.sinks = sinks
        self.locations = locations
        self.trip_cost = trip_cost
        self.default_trip_cost = default_trip_cost
        self.max_distance = max_distance
        self.alpha = alpha
        self.beta = beta
        
    def compute_distance_to_sinks(self, location, sources, sinks, max_distance):
        # Compute the distance between the origin and all possible destinations
        # if locations are cities, we approximate them as discs, based on their area :
        # trip length = 
        #   expected trip length from a random point to the centroid of the origin +  
        #   distance between the origin and the destination +
        #   expected trip length from the centroid to a random point of the destination
        
        if sources.index.isin([(location)]).any(): #LG
            x, y, r = sources.loc[location][["x", "y", "r"]].values.tolist()
        else:
            x, y, r = self.locations.loc[location][["x", "y", "r"]].values.tolist() #LG
        dist = np.sqrt(np.power(sinks["x"] - x, 2) + np.power(sinks["y"] - y, 2))
        dist = r/3 + dist + sinks["r"]/3

        # Apply a correction factor
        # from the article "From crow-fly distances to real distances, or the origin of detours, Heran"
        dist = dist*(1.1+0.3*np.exp(-dist/20))
        
        # If origin = destination
        # the trip length is the expected distance between two points on the city's disc
        if sources.index.isin([(location)]).any(): #LG
            d = sources.at[location, "d_internal"]
        else:
            d = self.locations.at[location, "d_internal"]  #LG
        d = d*(1.1+0.3*np.exp(-d/20))
        dist.at[location] = d
    
        dist.name = "d"
        
        dist = dist[dist < max_distance]
        return dist
    
        
    def _compute_sink_probabilities(self, origin, sources, sinks, max_distance):
        
        # Extract the number of employees at the source location
        if sources.index.isin([(origin)]).any(): #LG
            m_i = sources.loc[origin]["m_i"]
        else:
            print("Location does not contains this particular case")
            m_i=0
        
        # Keep only cities within a certain radius
        dist = self.compute_distance_to_sinks(origin, sources, sinks, max_distance)
        
        # Find the volume of the sinks at the selected destinations
        n_sinks_within_radius = pd.merge(sinks, dist, on="location_id")[["m_j"]]
        
        # Merge the sinks volumes with corresponding trip unit costs
        df = pd.merge(
            n_sinks_within_radius,
            self.trip_cost,
            left_index=True,
            right_index=True,
            how="left"
        )
        
        df["to"] = n_sinks_within_radius.index.values
        
        # All missing links are imputed with the cost of car travel
        df.fillna(self.default_trip_cost, inplace=True) 
        
        # Merge with the trip length to compute the trip costs
        df = pd.merge(df, dist, left_on="to", right_on="location_id")   
        df["cost"] = df["d"]*df["cost_per_km"]/1000
        
        # Compute the number of jobs as a function of the distance to the city
        df["cost_bin"] = (np.round(df["cost"]*100)).astype(int)
        n_sinks_at_cost = df.groupby("cost_bin", sort=True, as_index=False)["m_j"].sum()
        n_sinks_at_cost["s_ij"] = np.cumsum(n_sinks_at_cost["m_j"])
        
        # Compute the number of "intervening opportunities"
        # = total volume of sinks in locations which are less costly than the location at hand
        df = pd.merge(df, n_sinks_at_cost[["cost_bin", "s_ij"]], on = "cost_bin")
        df["s_ij"] = np.maximum(df["s_ij"] - df["m_j"], 0)

        # Compute the probabilities with the UO model
        df["p_ij"] = (m_i + self.alpha*df["s_ij"])*df["m_j"]/(m_i + (self.alpha+self.beta)*df["s_ij"])/(m_i + (self.alpha+self.beta)*df["s_ij"] + df["m_j"])
        df["p_ij"] = df["p_ij"]/df["p_ij"].sum()
        
        df = df[df["p_ij"] > 0]
        
        # Keep only th first 95% of the distribution
        df.sort_values("p_ij", inplace=True, ascending=False)
        df = df[df["p_ij"].cumsum() < 0.95].copy()
        df["p_ij"] = df["p_ij"]/df["p_ij"].sum()
        
        df.rename({"to": "location_id"}, axis=1, inplace=True)
        df.set_index("location_id", inplace=True)
        
        return df["p_ij"]
    
    def _compute_source_probabilities(self, destination, sources, sinks, max_distance):
        
        # Keep only cities within a certain radius
        dist = self.compute_distance_to_sinks(destination, sources, sinks, max_distance)
        sources_index = list(sources.index[np.isin(sources.index, dist.index)])
        
        flows = []
        
        for source_index in sources_index:
            p_ij = self._compute_sink_probabilities(source_index, sources, sinks, max_distance)
            if destination in p_ij.index.tolist():
                flows.append({"location_id": source_index, "volume": p_ij.loc[destination]*sources.loc[source_index]["m_i"]})
                         
        flows = pd.DataFrame(flows)
        flows.set_index("location_id", inplace=True)
        
        flows["p_ij"] = flows["volume"]/flows["volume"].sum()
        
        return flows["p_ij"]
        

class WorkMobilityModel(MobilityModel):
    
    def __init__(self, origin):
        
        data_folder_path = Path(os.path.dirname(__file__)).parent / "data"

        locations = pd.read_csv(
            data_folder_path / "input/mobility/work_home/locations.csv",
            dtype={"location_id": str}
        )
        locations.loc[locations["location_id"]=="13201", "location_id"] ="13055" # PB MARSEILLE
        
        sources = pd.read_csv(
            data_folder_path / "input/mobility/work_home/sources.csv",
            dtype={"location_id": str, "NA5": str, "CS1": str},
        )
        
        sinks = pd.read_csv(
            data_folder_path / "input/mobility/work_home/destinations.csv",
            dtype={"location_id": str, "NA5": str, "CS1": str}
        )
        
        sources = pd.merge(sources, locations, on="location_id")
        sinks = pd.merge(sinks, locations, on="location_id")
        
        sources.set_index(["NA5", "CS1", "location_id"], inplace=True)
        sinks.set_index(["NA5", "CS1", "location_id"], inplace=True)
        locations.set_index(["location_id"], inplace=True)
        
        trip_cost = pd.read_csv(data_folder_path / "input/mobility/costs/trips_average_unit_cost.csv", dtype={"from": str, "to": str}, index_col=["from", "to"])
        mode_cost = pd.read_csv(data_folder_path / "input/mobility/costs/unit_costs.csv", index_col="TRANS")
        
        # if origin in [str(13200 + i) for i in range(1, 16)]:
        #     loca = "13055" # Data available only at city scale
        # else:
        #     loca = origin
        # trip_cost = trip_cost.xs(loca)
        trip_cost = trip_cost.xs(origin)
        trip_cost.index.rename("location_id", inplace=True)
        
        super().__init__(
            sources=sources,
            sinks=sinks,
            locations=locations,
            trip_cost=trip_cost,
            default_trip_cost=mode_cost.loc[4].values[0],
            max_distance=40e3,
            alpha=0.3,
            beta=0.7
        )
        
    def compute_sink_probabilities(self, origin, na5, cs1):
        # Subset the sources and sinks 
        sources = self.sources.xs(na5).xs(cs1)
        sinks = self.sinks.xs(na5).xs(cs1)
        
        if len(sources)==0:
            print("work sources is None so try any type of work")
            sources = self.sources
            sinks = self.sinks
        df = super()._compute_sink_probabilities(origin, sources, sinks, self.max_distance)
        return df
    
    def compute_source_probabilities(self, destination, na5, cs1):
        # Subset the sources and sinks 
        sources = self.sources.xs(na5).xs(cs1)
        sinks = self.sinks.xs(na5).xs(cs1)
        df = super()._compute_source_probabilities(destination, sources, sinks, self.max_distance)
        return df
    
    def add_to_source(self, location, na5, cs1, volume):
        if location in [str(13200 + i) for i in range(1, 16)]:
            location = "13055"

        if self.sources.index.isin([(na5, cs1, location)]).any():
            self.sources.at[(na5, cs1, location), "m_i"] += volume
        else:
            print("source not in index")
            self.sources.loc[(na5, cs1, location), :] = [volume]+self.locations.loc[location].tolist()
        
    def add_to_sink(self, location, na5, cs1, volume):

        if location in [str(13200 + i) for i in range(1, 16)]:
            location = "13055"

        if self.sinks.index.isin([(na5, cs1, location)]).any():
            self.sinks.at[(na5, cs1, location), "m_j"] += volume
        else:
            print("sink not in index")
            self.sinks.loc[(na5, cs1, location), :] = [volume]+self.locations.loc[location].tolist()        

class ShopsMobilityModel(MobilityModel):
    
    def __init__(self, origin):
        
        data_folder_path = Path(os.path.dirname(__file__)).parent / "data"

        locations = pd.read_csv(
            data_folder_path / "input/mobility/work_home/locations.csv",
            dtype={"location_id": str}
        )
        locations.loc[locations["location_id"]=="13201", "location_id"] ="13055" # PB MARSEILLE
        
        sources = pd.read_csv(
            data_folder_path / "input/mobility/shops/sources.csv",
            dtype={"location_id": str},
        )
        sources.loc[sources["location_id"]=="13201", "location_id"] ="13055" # PB MARSEILLE
        
        sinks = pd.read_csv(
            data_folder_path / "input/mobility/shops/sinks.csv",
            dtype={"location_id": str}
        )
        sinks.loc[sinks["location_id"]=="13201", "location_id"] ="13055" # PB MARSEILLE
        
        sources = pd.merge(sources, locations, on="location_id")
        sinks = pd.merge(sinks, locations, on="location_id")
        
        sources.set_index(["location_id"], inplace=True)
        sinks.set_index(["location_id"], inplace=True)
        locations.set_index(["location_id"], inplace=True)
        # print(sources)
        # print(sinks)
        # print(locations)
        
        trip_cost = pd.read_csv(data_folder_path / "input/mobility/costs/trips_average_unit_cost.csv", dtype={"from": str, "to": str}, index_col=["from", "to"])
        mode_cost = pd.read_csv(data_folder_path / "input/mobility/costs/unit_costs.csv", index_col="TRANS")
        
        # if origin in [str(13200 + i) for i in range(1, 16)]:
        #     loca = "13055" # Data available only at city scale
        # else:
        #     loca = origin
        # trip_cost = trip_cost.xs(loca)
        trip_cost = trip_cost.xs(origin)
        trip_cost.index.rename("location_id", inplace=True)
        
        super().__init__(
            sources=sources,
            sinks=sinks,
            locations=locations,
            trip_cost=trip_cost,
            default_trip_cost=mode_cost.loc[4].values[0],
            max_distance=40e3,
            alpha=0.0,
            beta=1.0
        )
        
    def compute_sink_probabilities(self, origin):
        # print("shop test sink")
        # print(origin)
        # print(self.sources)
        # print(self.sinks)
        # print(self.max_distance)
        df = super()._compute_sink_probabilities(origin, self.sources, self.sinks, self.max_distance) 
        return df
    
    def compute_source_probabilities(self, destination):
        # print("shop test source")
        # print(destination)
        # print(self.sources)
        # print(self.sinks)
        # print(self.max_distance)
        df = super()._compute_source_probabilities(destination, self.sources, self.sinks, self.max_distance)
        return df
    
    def add_to_source(self, location, volume):
        if location in [str(13200 + i) for i in range(1, 16)]:
            location = "13055"

        if self.sources.index.isin([location]).any():
            self.sources.at[(location), "m_i"] += volume
        else:
            print("shops : source not in index")
            self.sources.loc[(location), :] = [volume]
        
    def add_to_sink(self, location, volume):
        if location in [str(13200 + i) for i in range(1, 16)]:
            location = "13055"

        if self.sinks.index.isin([location]).any():
            self.sinks.at[location, "m_j"] += volume
        else:
            print("shops : sink not in index")
            self.sinks.loc[(location), :] = [volume]        
    
    