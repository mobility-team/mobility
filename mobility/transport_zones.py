import os
import pathlib
import pandas as pd
import geopandas as gpd
import shapely
import numpy as np

from mobility.parsers.ign import prepare_ign
from mobility.parsers.urban_units import prepare_urban_units
from mobility.parsers.get_osm import get_osm

    
def get_transport_zones(insee_city_id, method="epci_rings", radius=40):
    """
    Returns a geodataframe of transport zones around a city of interest.

    Args:
        insee_city_id (str): the INSEE id of the city (ref 2023).
        method (str): method used to select cities around the city of interest.
            Can be one of the two methods :
            - epci_rings : all cities of the EPCIs that  are adjacent to the EPCI
            of the city (first hop) or adjacent to the neighbor EPCIs (second hop).
            - radius (all cities within a X km 
            radius around the centroid of the city)
        n_hops (int): number 
        radius (float): radius in km around the city to select cities, for the 
            "radius" selection method.
        
    Returns:
        geopandas.geodataframe: a geopandas geodataframe containing the following columns :
            transport_zone_id (int): integer id of the transport zone.
            admin_id (str): administrative id of the transport zone (INSEE city id for example). 
            name (str): name of the transport zone.
            admin_level (str): administrative level of the transport zone (city for example).
            geometry (shapely.Polygon): polygon geometry of the transport zone.

    """
    
    data_folder_path = (
        pathlib.Path(os.path.dirname(__file__)).parents[0] / "mobility/data/"
    )
    
    # Check if the parquet files already exist, if not writes them calling the corresponding funtion
    check_files = (data_folder_path / "ign/admin-express/EPCI.parquet").exists()
    check_files = (data_folder_path / "ign/admin-express/COMMUNE.parquet").exists()
    
    if not (check_files):
        prepare_ign()

    
    # Load IGN city admin borders (admin express)
    cities = gpd.read_parquet(
        data_folder_path / "ign/admin-express/COMMUNE.parquet",
        columns=["INSEE_COM", "SIREN_EPCI", "NOM", "geometry"]
    )
    
    if method == "epci_rings":
    
        # Fix the Grand Paris EPCI ids
        cities.loc[cities["SIREN_EPCI"].str[0:9] == "200054781", "SIREN_EPCI"] = "200054781"
        
        # Find the EPCI of the city
        epci_id = cities.loc[cities["INSEE_COM"] == insee_city_id, "SIREN_EPCI"].values
        
        if len(epci_id) == 0:
            raise ValueError("No city with id '" + insee_city_id + "' was found in the admin-express 2023 database.")
        else:
            epci_id = epci_id[0]
            
        # Load the geometries of all nearby EPCIs
        epcis = gpd.read_parquet(data_folder_path / "ign/admin-express/EPCI.parquet")
        
        # Select first / second ring of EPCIs around the EPCI of the city
        city_epci = epcis.loc[epcis["CODE_SIREN"] == epci_id].iloc[0]
        
        first_ring_epcis = epcis[epcis.touches(city_epci.geometry)]
        first_ring_epcis_union = shapely.ops.unary_union(first_ring_epcis.geometry)
        
        second_ring_epcis = epcis[epcis.touches(first_ring_epcis_union)]
        
        selected_epcis = pd.concat([
            first_ring_epcis,
            second_ring_epcis
        ])
        
        # Get the geometries of the cities in the selected EPCIs
        cities = cities.loc[cities["SIREN_EPCI"].isin(selected_epcis["CODE_SIREN"].values)]
        
    elif method == "radius":
        
        city = cities[cities["INSEE_COM"] == insee_city_id]
        buffer = city.centroid.buffer(radius*1000).iloc[0]
        cities = cities[cities.within(buffer)]
        
    else:
        raise ValueError("""
                Method should be one of : epci_rings, radius.
            """
        )

    # Add the urban unit category info
    check_files = (data_folder_path / "insee/territories/UU2020_au_01-01-2023.xlsx").exists()
    
    if not (check_files):
        prepare_urban_units()
        
    urban_units = pd.read_excel(
        data_folder_path / "insee/territories/UU2020_au_01-01-2023.xlsx",
        sheet_name="Composition_communale",
        skiprows=5
    )
    
    urban_units = urban_units.iloc[:, [0, 5]]
    urban_units.columns = ["INSEE_COM", "urban_unit_category"]
    urban_units["urban_unit_category"] = np.where(
        urban_units["urban_unit_category"] != "H",
        urban_units["urban_unit_category"],
        "R"
    )
    
    cities = pd.merge(cities, urban_units, on="INSEE_COM", how="left")
    
    # Prepare and format the transport zones data frame
    transport_zones = cities[["INSEE_COM", "NOM", "urban_unit_category", "geometry"]].copy()
    transport_zones.columns = ["admin_id", "name", "urban_unit_category", "geometry"]
    transport_zones["admin_level"] = "city"
    transport_zones["transport_zone_id"] = [i for i in range(transport_zones.shape[0])]
    transport_zones = transport_zones[[
        "transport_zone_id", "admin_id",
        "name", "admin_level", "urban_unit_category", "geometry"
    ]]
    
    return transport_zones
    

# Monkey patch openpyxl to avoid an error when opening the urban units INSEE file
# Source : https://stackoverflow.com/questions/71733414/copying-from-a-range-of-cells-with-openpyxl-error-colors-must-be-argb-hex-valu
from openpyxl.styles.colors import WHITE, RGB
__old_rgb_set__ = RGB.__set__
def __rgb_set_fixed__(self, instance, value):
    try:
        __old_rgb_set__(self, instance, value)
    except ValueError as e:
        if e.args[0] == 'Colors must be aRGB hex values':
            __old_rgb_set__(self, instance, WHITE)
RGB.__set__ = __rgb_set_fixed__


# Small test
z = get_transport_zones("75056", method="radius", radius=40)
transport_zones_boundary = z.to_crs(4326).unary_union
osm_file_path = get_osm(transport_zones_boundary, verify="C:/Users/pouchaif/Documents/dev/forcepoint.pem")
