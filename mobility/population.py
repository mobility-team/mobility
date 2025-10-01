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
            switzerland_census: CensusLocalizedIndividuals = None
        ):

        inputs = {
            "transport_zones": transport_zones,
            "sample_size": sample_size,
            "switzerland_census": switzerland_census
        }

        cache_path = {
            "individuals": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "individuals.parquet",
            "population_groups": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "population_groups.parquet"
        }

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
        logging.info("Population already prepared. Reusing the files : " + str(self.cache_path))
        return self.cache_path
    
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
        country_codes = transport_zones["local_admin_unit_id"].str[:2].unique().tolist()
        legal_pop_by_city = CityLegalPopulation().get()
        
        lau_to_tz_coeff = ( 
            transport_zones
            [["transport_zone_id", "local_admin_unit_id", "weight"]]
            .rename({"weight": "lau_to_tz_coeff"}, axis=1)
        )
        
        # Estimate the population groups in each transport zone id
        # Population groups = groups of persons with the same socio-economic 
        # caracteristics (household size, age...)
        getters = { 
            "ch": self.get_swiss_pop_groups,
            "fr": self.get_french_pop_groups
        }
        
        pop_groups = [getters[c](transport_zones, legal_pop_by_city, lau_to_tz_coeff) for c in country_codes]
        pop_groups = pd.concat(pop_groups)
        

        # Sample the population groups to get a representative sample of individuals
        sample_sizes = self.get_sample_sizes(lau_to_tz_coeff, self.inputs["sample_size"])
        sample_sizes = sample_sizes.set_index("transport_zone_id")["n_persons"].to_dict()
        
        individuals = ( 
            pop_groups
            .groupby("transport_zone_id", as_index=False)
            .apply(lambda g: g.sample(n=sample_sizes[g.name], weights="weight"))
        )
        
        individuals["individual_id"] = [shortuuid.uuid() for _ in range(individuals.shape[0])]
        
        individuals = individuals[
            [
                "individual_id", "transport_zone_id", "age", "socio_pro_category",
                "ref_pers_socio_pro_category", "n_pers_household", "country", "n_cars"
            ]
        ]
        
        individuals.to_parquet(self.cache_path["individuals"])
        pop_groups.to_parquet(self.cache_path["population_groups"])

        return self.cache_path

    
    def get_french_pop_groups(
            self,
            transport_zones: gpd.GeoDataFrame,
            legal_pop_by_city: pd.DataFrame,
            lau_to_tz_coeff: pd.DataFrame
        ):
        
        regions = get_french_regions_boundaries()
        transport_zones = transport_zones[transport_zones["local_admin_unit_id"].str.contains("fr-")]
        transport_zones = gpd.sjoin(transport_zones, regions[["INSEE_REG", "geometry"]], predicate="intersects") 
        transport_zones_regions = transport_zones["INSEE_REG"].drop_duplicates().tolist()
        
        cantons = get_french_cities_boundaries()
        cantons = cantons[["INSEE_COM", "INSEE_CAN"]]
        cantons.columns = ["local_admin_unit_id", "CANTVILLE"]
        
        # Load the french census data
        census_data = [CensusLocalizedIndividuals(tz_region).get() for tz_region in transport_zones_regions]
        census_data = pd.concat(census_data)
        census_data.set_index(["CANTVILLE"], inplace=True)
        census_data["pop_group_share"] = census_data["weight"]/census_data.groupby("CANTVILLE")["weight"].transform("sum")
        census_data = census_data.reset_index()
        census_data = census_data.drop(["weight"], axis=1)
        
        pop_groups = pd.merge(transport_zones, lau_to_tz_coeff, on=["transport_zone_id", "local_admin_unit_id"])
        pop_groups = pd.merge(pop_groups, cantons, on="local_admin_unit_id")
        pop_groups = pd.merge(pop_groups, census_data, on="CANTVILLE")
        pop_groups = pd.merge(pop_groups, legal_pop_by_city, on="local_admin_unit_id")
        pop_groups["weight"] = ( 
            pop_groups["legal_population"]
            * pop_groups["lau_to_tz_coeff"]
            * pop_groups["pop_group_share"]
        )
        
        pop_groups = pop_groups[
            [
                "transport_zone_id", "local_admin_unit_id",
                "age", "socio_pro_category", 
                "ref_pers_socio_pro_category", "n_pers_household", "n_cars",
                "weight"
            ]
        ]
        
        pop_groups["country"] = "fr"

        return pop_groups
    

    def get_swiss_pop_groups(
            self,
            transport_zones: gpd.GeoDataFrame,
            legal_pop_by_city: pd.DataFrame,
            lau_to_tz_coeff: pd.DataFrame
        ):
        
        # Load the swiss census data
        if self.switzerland_census is None:
            raise ValueError(
                ("Some transport zones are in Switzerland and no parser for "
                 "the swiss census dataset was provided (which is not openly "
                 "available at the moment).")
            )
            
        transport_zones = transport_zones[transport_zones["local_admin_unit_id"].str.contains("ch-")]
            
        census_data = self.inputs["switzerland_census"].get()
        census_data = census_data.set_index("local_admin_unit_id")
        census_data = census_data.loc[transport_zones["local_admin_unit_id"]]
        census_data["pop_group_share"] = census_data["weight"]/census_data.groupby("local_admin_unit_id")["weight"].transform("sum")
        census_data = census_data.reset_index()
        census_data = census_data.drop(["individual_id", "weight"], axis=1)
        
        pop_groups = pd.merge(transport_zones, lau_to_tz_coeff, on=["transport_zone_id", "local_admin_unit_id"])
        pop_groups = pd.merge(pop_groups, census_data, on="local_admin_unit_id")
        pop_groups = pd.merge(pop_groups, legal_pop_by_city, on="local_admin_unit_id")
        pop_groups["weight"] = ( 
            pop_groups["legal_population"]
            * pop_groups["lau_to_tz_coeff"]
            * pop_groups["pop_group_share"]
        )
        
        pop_groups = pop_groups[
            [
                "transport_zone_id", "local_admin_unit_id",
                "age", "socio_pro_category",
                "ref_pers_socio_pro_category", "n_pers_household", "n_cars",
                "weight"
            ]
        ]
        
        pop_groups["country"] = "ch"
         
        return pop_groups
    
        
    def get_sample_sizes(self, lau_to_tz_coeff: pd.DataFrame, sample_size: int):
        """Compute the number of individuals in each transport zone given the global sample size."""
        logging.info("Computing the number of individuals in each transport zone given the global sample size...")
        
        legal_pop_by_city = CityLegalPopulation().get()
        
        population = pd.merge(
            lau_to_tz_coeff,
            legal_pop_by_city,
            on="local_admin_unit_id",
            how="left"
        )
        
        population["legal_population"] = population["legal_population"]*population["lau_to_tz_coeff"]
        
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