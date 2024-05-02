import pathlib
import os
import logging
import pandas as pd
import geopandas as gpd
import shapely

from mobility.parsers.admin_boundaries import get_french_cities_boundaries, get_french_epci_boundaries
from mobility.parsers.urban_units import get_french_urban_units
from mobility.caching import is_update_needed
    
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
    
    tzs_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones.gpkg"
    
    inputs = {
        "insee_city_id": insee_city_id,
        "method": method,
        "radius": radius
    }
    
    update_needed, inputs_hash = is_update_needed(inputs, tzs_file_path)
    
    if update_needed is True:
        
        logging.info("Creating transport zones...")
    
        # Load IGN city admin borders (admin express)
        cities = get_french_cities_boundaries()
        
        if method == "epci_rings":
            
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
        urban_units = get_french_urban_units()
    
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
        
        # Save to file
        transport_zones.to_file(tzs_file_path)
        
    else:
        
        logging.info("Transport zones already created. Reusing the file " + str(tzs_file_path))
        
        transport_zones = gpd.read_file(tzs_file_path)
        
        
    transport_zones.inputs_hash = inputs_hash
    
    return transport_zones

