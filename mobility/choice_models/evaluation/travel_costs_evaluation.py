import re
import itertools

import polars as pl
import geopandas as gpd
import plotly.express as px

from urllib.parse import unquote
from typing import Dict, List

class TravelCostsEvaluation:
    """
    Evaluate and compare modeled travel costs (time, distance) against reference data 
    such as Google Maps results.
    """
    
    def __init__(self, results):
        """
        Initialize the evaluator with model results.
        
        Parameters
        ----------
        results : object
            Object containing model outputs, including weekday and weekend costs 
            and transport zones.
        """
        self.results = results
    
    
    def get(
            self,
            ref_costs: List,
            variable: str = "time",
            weekday: bool = True,
            plot: bool = True
        ):
        """
        Compares the travel costs (time, distance) of the model with reference 
        data (from google maps for example). 
        
        Parameters
        ----------
        ref_costs : list
            List of dicts with the following structure : 
                {
                     "url": "https://www.google.com/maps/dir/46.1987752,6.14416/46.0672248,6.3121397/@46.1336438,6.1540326,12z/data=!4m9!4m8!1m0!1m1!4e1!2m3!6e0!7e2!8j1761033600!3e0?entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D",
                     "hour": 8.0,
                     "travel_costs": [
                         {
                             "mode": "car",
                             "time": (24.0+40.0)/2.0/60.0,
                             "distance": 27.4
                         },
                         {
                             "mode": "walk/public_transport/walk",
                             "time": 64.0/60.0,
                             "distance": None
                         },
                         {
                             "mode": "bicycle",
                             "time": 105.0/60.0,
                             "distance": 26.5
                         }
                     ]
                }
                 
                
        variable: str
            Controls wether the comparison is made on "time" or "distance".
        
        weekday:
            Controls wether the comparison is made on weekday or weekend results.
            
        plot: bool
            Should a scatter plot of the results be displayed.
        
        Returns
        -------
        pl.DataFrame
            Input ref_costs dataframe with a distance_model and a time_model column.
        """
        
        costs = self.results.weekday_costs if weekday else self.results.weekend_costs
        transport_zones = self.results.transport_zones.get()
        
        ref_costs = self.convert_to_dataframe(ref_costs)
        ref_costs = self.join_transport_zone_ids(ref_costs, transport_zones)
        
        ref_costs = (
            ref_costs
            .join(costs.collect(), on=["from", "to", "mode"], suffix="_model")
        )
        
        if plot:
            fig = px.scatter(
                ref_costs,
                x=variable,
                y=variable + "_model",
                color="mode",
                hover_data={
                    "origin": True,
                    "destination": True,
                    "mode": True,
                    "time": True,
                    "time_model": True,
                    "distance": True,
                    "distance_model": True
                }
            )
            
            fig.show("browser")
        
        return ref_costs
    
    
    def convert_to_dataframe(self, ref_costs):
        """
        Convert structured reference cost data into a Polars DataFrame.
        
        Parameters
        ----------
        ref_costs : list of dict
            List of structured reference cost entries.
        
        Returns
        -------
        pl.DataFrame
            Flattened DataFrame with one row per OD-mode combination and an index column.
        """
        
        ref_costs = [
            self.flatten_travel_costs(**c)
            for c in ref_costs
        ]
        ref_costs = list(itertools.chain.from_iterable(ref_costs))
        ref_costs = pl.DataFrame(ref_costs)
        
        ref_costs = ( 
            ref_costs
            .with_row_index("ref_index")
        )
        
        return ref_costs
    

    def flatten_travel_costs(self, origin: Dict, destination: Dict, departure_hour: float, travel_costs: Dict):
        """
        Convert a single reference route definition into row entries.
        
        Parameters
        ----------
        origin : dict
            name, lon, and lat of the origin.
        destination : dict
            name, lon, and lat of the destination.
        url : str
            Google Maps directions URL.
        departure_hour : float
            Departure hour in decimal format.
        travel_costs : dict
            Dictionary of mode/time/distance entries.
        
        Returns
        -------
        list of dict
            Each dict contains travel attributes and coordinates for a specific mode.
        """
        
        travel_costs = [
            {
                "origin": origin["name"],
                "destination": destination["name"],
                "from_lon": origin["lon"],
                "from_lat": origin["lat"],
                "to_lon":  destination["lon"],
                "to_lat": destination["lat"],
                "departure_hour": departure_hour,
                "time": tc["time"],
                "distance": tc["distance"],  
                "mode": tc["mode"]
            } 
            for tc in travel_costs
        ]
        
        return travel_costs
    

    def join_transport_zone_ids(self, ref_costs, transport_zones):
        """
        Assign origin and destination transport zone IDs to each reference trip.
        
        Parameters
        ----------
        ref_costs : pl.DataFrame
            Reference travel cost DataFrame with coordinates.
        transport_zones : gpd.GeoDataFrame
            Transport zone polygons with an ID column.
        
        Returns
        -------
        pl.DataFrame
            Input DataFrame with added 'from' and 'to' zone identifiers.
        """
        
        origins = gpd.GeoDataFrame(
            ref_costs.to_pandas()[["ref_index"]],
            geometry=gpd.points_from_xy(
                ref_costs["from_lon"],
                ref_costs["from_lat"]
            ),
            crs="EPSG:4326"
        )
        
        origins = origins.to_crs("EPSG:3035")
        
        destinations = gpd.GeoDataFrame(
            ref_costs.to_pandas()[["ref_index"]],
            geometry=gpd.points_from_xy(
                ref_costs["to_lon"],
                ref_costs["to_lat"]
            ),
            crs="EPSG:4326"
        )
        
        destinations = destinations.to_crs("EPSG:3035")
        
        origins = gpd.sjoin(origins, transport_zones[["transport_zone_id", "geometry"]])
        destinations = gpd.sjoin(destinations, transport_zones[["transport_zone_id", "geometry"]])
        
        origins = ( 
            pl.DataFrame(origins.drop(["geometry", "index_right"], axis=1))
            .rename({"transport_zone_id": "from"})
        )
        
        destinations = ( 
            pl.DataFrame(destinations.drop(["geometry", "index_right"], axis=1))
            .rename({"transport_zone_id": "to"})
        )
        
        ref_costs = (
            ref_costs
            .join(origins, on="ref_index")
            .join(destinations, on="ref_index")
        )
        
        return ref_costs