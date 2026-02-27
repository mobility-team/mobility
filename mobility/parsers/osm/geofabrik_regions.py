import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

from mobility.file_asset import FileAsset

class GeofabrikRegions(FileAsset):
    """
    Manage data for the GeoFabrik regions in France and Switzerland.
    
    Parameters
    ----------
    extract_date : str, default="250101"
        Date of export of the OSM data, in format YYMMDD.
    """
        
    def __init__(self, extract_date: str = "250101"): 
        inputs = {"extract_date": extract_date}
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "geofabrik_regions.gpkg"
        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> gpd.GeoDataFrame:
        """
        Get the already stored data about GeoFabrik regions in France and Switzerland with the given extract_date.

        Returns
        -------
        regions : geopandas.geodataframe.GeoDataFrame
            Returns the data already stored:
            geometries of the boundaries of all the GeoFabrik regions in France and Switzerland, and URLs to download them with the data at the given extract_date.


        """
        logging.info("Geofabrik regions already prepared. Reusing the file : " + str(self.cache_path))
        regions = gpd.read_file(self.cache_path)

        return regions
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Create the data about GeoFabrik regions in France and Switzerland with the given extract_date.

        Returns
        -------
        regions : geopandas.geodataframe.GeoDataFrame
            Geometries of the boundaries of all the GeoFabrik regions in France and Switzerland, and URLs to download them with the data at the given extract_date.

        """        
        logging.info("Preparing Geofabrik regions boundaries.")
        
        regions_urls = [
            "/europe/france/alsace",
            "/europe/france/aquitaine",
            "/europe/france/auvergne",
            "/europe/france/basse-normandie",
            "/europe/france/bourgogne",
            "/europe/france/bretagne",
            "/europe/france/centre",
            "/europe/france/champagne-ardenne",
            "/europe/france/corse",
            "/europe/france/franche-comte",
            "/europe/france/haute-normandie",
            "/europe/france/ile-de-france",
            "/europe/france/languedoc-roussillon",
            "/europe/france/limousin",
            "/europe/france/lorraine",
            "/europe/france/midi-pyrenees",
            "/europe/france/nord-pas-de-calais",
            "/europe/france/pays-de-la-loire",
            "/europe/france/picardie",
            "/europe/france/poitou-charentes",
            "/europe/france/provence-alpes-cote-d-azur",
            "/europe/france/rhone-alpes",
            "/europe/switzerland",
        ]
        
        regions_urls = ["https://download.geofabrik.de" + url for url in regions_urls]
        regions_urls = [url + ".poly" for url in regions_urls]
        
        regions = []
        
        for region_url in regions_urls:
            
            logging.info("Fetching region boundary : " + region_url)
        
            #Grabs 
            region = pd.read_table(region_url, skiprows=1, skipfooter=2, engine="python")
            
            region = pd.DataFrame({
                "x": region.iloc[:, 0].str.split("   ").str[1],
                "y": region.iloc[:, 0].str.split("   ").str[2]
            })
            
            region = Polygon(zip(region["x"], region["y"]))
            region = gpd.GeoDataFrame(geometry=[region], crs=4326)
            
            region["url"] = region_url.replace(".poly", "-" + self.inputs["extract_date"] + ".osm.pbf")
            
            regions.append(region)
            
        regions = pd.concat(regions)
            
        regions.to_file(self.cache_path)

        return regions
