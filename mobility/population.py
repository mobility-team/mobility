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
    """
    Sample a synthetic population for the area.
    
    Will create a population of sample_size. Use the .get() method to retrieve the data frame with this population.

    Parameters inhabitants within the transport zones.
    
    For Switzerland, you need to supply private census data using swiss_census_data_path, as it is not publically available.
    
    Parameters
    ----------
    transport_zones : mobility.TransportZones
        Transports zones previously defined.
    sample_size : int
        Number of inhabitants to sample within the given transport zones. Higher number enable the representation of rare situations while lower numbers enable faster calculations.
    swiss_census_data_path : str | pathlib.Path, optional
        DESCRIPTION. The default is None.

    Methods
    -------
    get():
        Provides a data frame with the population.

    """

    def __init__(
            self,
            transport_zones,
            sample_size: int,
            swiss_census_data_path: str | pathlib.Path = None
        ):

        
        inputs = {"transport_zones": transport_zones, "sample_size": sample_size}
        
        self.swiss_census_data_path = swiss_census_data_path

        file_name = "population.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name    

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:
        """
        Retrieve cached sampled population for the given inputs.

        Returns
        -------
        individuals : pandas.DataFrame
            Sample of individuals with their age, socio-professional category, socio-professional category of the household person reference,
            number of persons in the household, number of cars in the household, transport zone and individual id.
        """
        logging.info("Population already prepared. Reusing the file : " + str(self.cache_path))
        individuals = pd.read_parquet(self.cache_path)

        return individuals
    
    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Create and retrieve sampled population for the given inputs.
        
        Returns
        -------
        individuals : pandas.DataFrame
            Sample of individuals with their age, socio-professional category, socio-professional category of the household person reference,
            number of persons in the household, number of cars in the household, transport zone and individual id.

        """
        transport_zones = self.inputs["transport_zones"].get()
        sample_size = self.inputs["sample_size"]
        
        sample_sizes = self.get_sample_sizes(transport_zones, sample_size)
        
        country_codes = transport_zones["local_admin_unit_id"].str[:2].unique().tolist()

        if "ch" in country_codes:
            swiss_census_data = self.get_swiss_census_data(transport_zones)
            swiss_individuals = self.get_swiss_individuals(sample_sizes, swiss_census_data)
            individuals = swiss_individuals
        
        if "fr" in country_codes:
            french_census_data = self.get_french_census_data(transport_zones)
            french_individuals = self.get_french_individuals(sample_sizes, french_census_data)
            individuals = french_individuals

        if "ch" in country_codes and "fr" in country_codes:
            individuals = pd.concat([french_individuals, swiss_individuals])
        
        individuals.to_parquet(self.cache_path)

        return individuals
    
    
    def get_sample_sizes(self, transport_zones: gpd.GeoDataFrame, sample_size: int):
        """Compute the number of individuals in each transport zone given the global sample size."""
        logging.info("Computing the number of individuals in each transport zone given the global sample size...")
        
        legal_pop_by_city = CityLegalPopulation().get()
        
        population = pd.merge(
            transport_zones,
            legal_pop_by_city,
            on="local_admin_unit_id",
            how="left"
        )
        
        population["legal_population"] = population["legal_population"]*population["weight"]
        
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
    
    
    def get_french_census_data(self, transport_zones: gpd.GeoDataFrame):
        """Get French census data from INSEE."""
        logging.info("Loading french census data...")
        
        regions = get_french_regions_boundaries()
        
        transport_zones = transport_zones[transport_zones["local_admin_unit_id"].str.contains("fr-")]
        transport_zones = gpd.sjoin(transport_zones, regions[["INSEE_REG", "geometry"]], predicate="intersects") 
        transport_zones_regions = transport_zones["INSEE_REG"].drop_duplicates().tolist()
        
        census_data = [CensusLocalizedIndividuals(tz_region).get() for tz_region in transport_zones_regions]
        census_data = pd.concat(census_data)
        
        census_data.set_index(["CANTVILLE"], inplace=True)
        
        return census_data
    
    
    def get_french_individuals(self, sample_sizes: pd.DataFrame, census_data: pd.DataFrame):
        """Sample French individuals using the data per canton (smallest admin unit available)"""
        cantons = get_french_cities_boundaries()
        cantons = cantons[["INSEE_COM", "INSEE_CAN"]]
        cantons.columns = ["local_admin_unit_id", "CANTVILLE"]
        
        sample_sizes = pd.merge(sample_sizes, cantons, on="local_admin_unit_id")

        logging.info("Sampling census data in each french transport zone...")
        
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
        individuals["country"] = "fr"
        
        return individuals
    
    
    
    def get_swiss_census_data(self, transport_zones: gpd.GeoDataFrame):
        """Check that a swiss census has been provided and use its data."""
        if transport_zones["local_admin_unit_id"].str.contains("ch-").sum() > 0:
            
            if self.swiss_census_data_path is None:
                raise ValueError(
                    ("Some transport zones are in Switzerland and no path to "
                     "a preprocessed swiss census dataset (which is not openly "
                     "available at the moment).")
                )
                
            if self.swiss_census_data_path.exists() is False:
                raise ValueError(
                    "The preprocessed census dataset provided does not exist at the location " + str(self.swiss_census_data_path)
                )
                
            census_data = pd.read_parquet(self.swiss_census_data_path)
        
        else:
            
            census_data = pd.DataFrame(
                columns=[
                    "age", "socio_pro_category", "ref_pers_socio_pro_category",
                    "n_pers_household", "n_cars", "local_admin_unit_id"
                ]
            )
            
        census_data = census_data.set_index("local_admin_unit_id")
        
        return census_data
    

    def get_swiss_individuals(self, sample_sizes: pd.DataFrame, census_data: pd.DataFrame):
        """Sample Swiss individuals."""
        sample_sizes = sample_sizes[sample_sizes["local_admin_unit_id"].str.contains("ch-")]

        logging.info("Sampling census data in each swiss transport zone...")
        
        cities = sample_sizes[["local_admin_unit_id", "n_persons", "transport_zone_id"]].to_dict(orient="records")
        
        individuals = []

        with Progress() as progress:
            
            task = progress.add_task("[green]Sampling individuals...", total=len(cities))
        
            for city in cities:
                
                indiv = census_data.loc[city["local_admin_unit_id"]].sample(city["n_persons"], weights="weight")
                indiv = indiv.reset_index()
                indiv = indiv[["age", "socio_pro_category", "ref_pers_socio_pro_category", "n_pers_household"]]
                indiv["transport_zone_id"] = city["transport_zone_id"]
                individuals.append(indiv)
                
                progress.update(task, advance=1)
        
            
        individuals = pd.concat(individuals)
        
        individuals["individual_id"] = [shortuuid.uuid() for _ in range(individuals.shape[0])]
        individuals["country"] = "ch"
        
        return individuals