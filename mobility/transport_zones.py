import os
import logging
import pandas as pd
import geopandas as gpd
import shapely
import pathlib

from mobility.parsers.admin_boundaries import get_french_cities_boundaries, get_french_epci_boundaries
from mobility.parsers.urban_units import get_french_urban_units
from mobility.asset import Asset


class TransportZones(Asset):
    """
    A class for managing transport zones, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving transport
    zones based on specified criteria such as city ID, method, and radius.

    Attributes:
        insee_city_id (str): The INSEE code of the city.
        method (str): The method to define transport zones ('epci_rings' or 'radius').
        radius (int): The radius around the city to define transport zones, applicable if method is 'radius'.

    Methods:
        get_cached_asset: Retrieve a cached transport zones GeoDataFrame.
        create_and_get_asset: Create and retrieve transport zones based on the current inputs.
        filter_cities_epci_rings: Filter cities based on EPCI rings.
        filter_cities_within_radius: Filter cities within a specified radius.
        prepare_transport_zones_df: Prepare the transport zones GeoDataFrame.
    """

    def __init__(self, insee_city_id: str, method: str = "epci_rings", radius: int = 40):
        """
        Initializes a TransportZones object with the given INSEE city ID, method, and radius.

        Args:
            insee_city_id (str): The INSEE code of the city.
            method (str, optional): Method to define transport zones. Defaults to 'epci_rings'.
            radius (int, optional): Radius in kilometers if method is 'radius'. Defaults to 40.
        """

        inputs = {"insee_city_id": insee_city_id, "method": method, "radius": radius}

        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        """
        Retrieves the transport zones from the cache.

        Returns:
            gpd.GeoDataFrame: The cached transport zones.
        """

        logging.info("Transport zones already created. Reusing the file " + str(self.cache_path))
        transport_zones = gpd.read_file(self.cache_path)

        return transport_zones

    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        """
        Creates transport zones based on the current inputs and retrieves them.

        Returns:
            gpd.GeoDataFrame: The newly created transport zones.
        """

        logging.info("Creating transport zones...")

        cities = get_french_cities_boundaries()

        if self.inputs["method"] == "epci_rings":
            filtered_cities = self.filter_cities_epci_rings(cities, self.inputs["insee_city_id"])

        elif self.inputs["method"] == "radius":
            filtered_cities = self.filter_cities_within_radius(cities, self.inputs["insee_city_id"], self.inputs["radius"])

        else:
            raise ValueError("Method should be one of : epci_rings, radius.")

        transport_zones = self.prepare_transport_zones_df(filtered_cities)
        transport_zones.to_file(self.cache_path)

        return transport_zones

    def filter_cities_epci_rings(self, cities: gpd.GeoDataFrame, insee_city_id: str):
        """
        Filters cities based on the EPCI rings method. It selects cities belonging to the first and
        second ring of EPCIs around the EPCI of the specified city.

        Args:
            cities (gpd.GeoDataFrame): The GeoDataFrame containing city data.
            insee_city_id (str): The INSEE code of the target city.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame of cities filtered based on EPCI rings.
        """

        # Find the EPCI of the city
        epci_id = cities.loc[cities["INSEE_COM"] == insee_city_id, "SIREN_EPCI"].values

        if len(epci_id) == 0:
            raise ValueError("No city with id '" + insee_city_id + "' was found in the admin-express 2023 database.")
        else:
            epci_id = epci_id[0]

        # Load the geometries of all nearby EPCIs
        epcis = get_french_epci_boundaries()

        # Select first / second ring of EPCIs around the EPCI of the city
        city_epci = epcis.loc[epcis["CODE_SIREN"] == epci_id].iloc[0]

        first_ring_epcis = epcis[epcis.touches(city_epci.geometry)]
        first_ring_epcis_union = shapely.ops.unary_union(first_ring_epcis.geometry)

        second_ring_epcis = epcis[epcis.touches(first_ring_epcis_union)]

        selected_epcis = pd.concat([first_ring_epcis, second_ring_epcis])

        # Get the geometries of the cities in the selected EPCIs
        cities = cities.loc[cities["SIREN_EPCI"].isin(selected_epcis["CODE_SIREN"].values)]

        return cities

    def filter_cities_within_radius(self, cities: gpd.GeoDataFrame, insee_city_id: str, radius: int) -> gpd.GeoDataFrame:
        """
        Filters cities within a specified radius from a given city. It selects cities within the
        specified radius from the centroid of the target city.

        Args:
            cities (gpd.GeoDataFrame): The GeoDataFrame containing city data.
            insee_city_id (str): The INSEE code of the target city.
            radius (int): The radius in kilometers around the target city.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame of cities filtered within the specified radius.
        """

        city = cities[cities["INSEE_COM"] == insee_city_id]
        if city.empty:
            raise ValueError(f"No city with INSEE code '{insee_city_id}' found.")
        buffer = city.centroid.buffer(radius * 1000).iloc[0]
        cities = cities[cities.within(buffer)]

        return cities

    def prepare_transport_zones_df(self, filtered_cities: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Prepares and formats the transport zones data frame from the filtered cities. It includes
        merging with urban unit categories and assigning transport zone IDs.

        Args:
            filtered_cities (gpd.GeoDataFrame): The GeoDataFrame of filtered cities.

        Returns:
            gpd.GeoDataFrame: A formatted GeoDataFrame representing transport zones.
        """

        # Add the urban unit category info
        urban_units = get_french_urban_units()
        filtered_cities = pd.merge(filtered_cities, urban_units, on="INSEE_COM", how="left")

        # Prepare and format the transport zones data frame
        transport_zones = filtered_cities[["INSEE_COM", "NOM", "urban_unit_category", "geometry"]].copy()
        transport_zones.columns = ["admin_id", "name", "urban_unit_category", "geometry"]
        transport_zones["admin_level"] = "city"
        transport_zones["transport_zone_id"] = [i for i in range(transport_zones.shape[0])]
        transport_zones = transport_zones[
            ["transport_zone_id", "admin_id", "name", "admin_level", "urban_unit_category", "geometry"]
        ]

        return transport_zones
