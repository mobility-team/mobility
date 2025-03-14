import os
import pathlib
import logging
import shortuuid
import pandas as pd
import numpy as np
import geopandas as gpd

from rich.progress import Progress

from mobility.file_asset import FileAsset
from mobility.parsers import CityLegalPopulation
from mobility.parsers import CensusLocalizedIndividuals
from mobility.parsers.admin_boundaries import get_french_regions_boundaries, get_french_cities_boundaries

class Population(FileAsset):
    
    def __init__(self, transport_zones: gpd.GeoDataFrame, sample_size: int):
        
        inputs = {"transport_zones": transport_zones, "sample_size": sample_size}

        file_name = "population.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name    

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Population already prepared. Reusing the file : " + str(self.cache_path))
        individuals = pd.read_parquet(self.cache_path)

        return individuals
    
    def create_and_get_asset(self) -> pd.DataFrame:

        transport_zones = self.inputs["transport_zones"].get()
        sample_size = self.inputs["sample_size"]
        
        sample_sizes = self.get_sample_sizes(transport_zones, sample_size)
        census_data = self.get_census_data(transport_zones)
        individuals = self.get_individuals(sample_sizes, census_data)
    
        individuals.to_parquet(self.cache_path)

        return individuals
    
    
    def get_sample_sizes(self, transport_zones: gpd.GeoDataFrame, sample_size: int):
        
        logging.info("Computing the number of individuals in each transport zone given the global sample size...")
        
        legal_pop_by_city = CityLegalPopulation().get()
        
        population = pd.merge(
            transport_zones,
            legal_pop_by_city,
            on="local_admin_unit_id",
            how="left"
        )
        
        if population["legal_population"].isnull().any():
            logging.info(
                """
                    Could not associate legal populations to some of the 
                    transport zones (maybe because of differences between
                    cities ids in the sources for the transport zones and 
                    the legal populations ?). The population count of these 
                    transport zones will be set to zero.
                """
            )
            population["legal_population"].fillna(0.0, inplace=True)
        
        population["n_persons"] = sample_size*population["legal_population"].pow(0.5)/population["legal_population"].pow(0.5).sum()
        population["n_persons"] = np.ceil(population["n_persons"])
        population["n_persons"] = population["n_persons"].astype(int)
        population["n_persons"] = np.maximum(population["n_persons"], 1)
        
        sampling_rate = population["n_persons"].sum()/population["legal_population"].sum()
        
        logging.info("Global sampling rate : " + str(round(10000*sampling_rate)/10000) + " %.")
        
        return population
    
    
    def get_census_data(self, transport_zones: gpd.GeoDataFrame):
        
        logging.info("Loading census data...")
        
        regions = get_french_regions_boundaries()
        
        transport_zones = gpd.sjoin(transport_zones, regions[["INSEE_REG", "geometry"]], predicate="intersects") 
        transport_zones_regions = transport_zones["INSEE_REG"].drop_duplicates().tolist()
        
        census_data = [CensusLocalizedIndividuals(transport_zones).get() for tz_region in transport_zones_regions]
        census_data = pd.concat(census_data)
        
        census_data.set_index(["CANTVILLE"], inplace=True)
        
        return census_data
    
    
    def get_individuals(self, sample_sizes: pd.DataFrame, census_data: pd.DataFrame):
        
        cantons = get_french_cities_boundaries()
        cantons = cantons[["INSEE_COM", "INSEE_CAN"]]
        cantons.columns = ["local_admin_unit_id", "CANTVILLE"]
        
        sample_sizes = pd.merge(sample_sizes, cantons, on="local_admin_unit_id")

        logging.info("Sampling census data in each transport zone...")
        
        cities = sample_sizes.to_dict(orient="records")
        
        individuals = []

        with Progress() as progress:
            
            task = progress.add_task("[green]Sampling individuals...", total=len(cities))
        
            for city in cities:
                
                indiv = census_data.loc[city["CANTVILLE"]].sample(city["n_persons"], weights="weight")
                indiv = indiv.reset_index()
                indiv = indiv[["age", "socio_pro_category", "ref_pers_socio_pro_category", "n_pers_household", "n_cars"]]
                indiv["transport_zone_id"] = city["transport_zone_id"]
                individuals.append(indiv)
                
                progress.update(task, advance=1)
        
            
        individuals = pd.concat(individuals)
        
        individuals["individual_id"] = [shortuuid.uuid() for _ in range(individuals.shape[0])]
        
        return individuals
    

    