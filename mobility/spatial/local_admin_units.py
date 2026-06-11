import logging
import os
import pathlib

import geopandas as gpd
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.spatial.admin_units import FrenchAdminUnits, SwissAdminUnits
from mobility.spatial.local_admin_units_categories import LocalAdminUnitsCategories

class LocalAdminUnits(FileAsset):
    """FileAsset class preparing local admin units in France and Switzerland.
    
    Use .get() method to get its content (under Parquet format).
    
    In France, uses adminexpress base from IGN, stored on https://cartes.gouv.fr/. For Paris, Lyon and Marseille, each 'arrondissement' is considered a distinct admin unit.
    
    In Switzerland, uses swisstopo data stored on geo.admin.ch
    
    Data from both countries is merged and stored in Parquet format under coordinates system EPSG:3035.
    """
    
    def __init__(self):
        
        inputs = {
            "french_local_admin_units": FrenchAdminUnits(level="commune"),
            "swiss_local_admin_units": SwissAdminUnits(level="municipality"),
            "categories": LocalAdminUnitsCategories()
        }
        
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "local_admin_units.parquet"
        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Local administrative units already prepared. Reusing the file : " + str(self.cache_path))
        local_admin_units = gpd.read_parquet(self.cache_path)

        return local_admin_units
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Preparing local administrative units.")
        
        local_admin_units = pd.concat(
            [
                self.format_local_admin_units(self.inputs["french_local_admin_units"].get()),
                self.format_local_admin_units(self.inputs["swiss_local_admin_units"].get()),
            ]
        )
        
        local_admin_units = pd.merge(
            local_admin_units,
            self.inputs["categories"].get(),
            on="local_admin_unit_id"
        )
    
        local_admin_units.to_parquet(self.cache_path)

        return local_admin_units
    
    
    @staticmethod
    def format_local_admin_units(admin_units):
        """Return admin units with the historical LocalAdminUnits columns."""
        admin_units = admin_units[["admin_id", "admin_name", "country", "geometry"]].copy()
        admin_units.columns = [
            "local_admin_unit_id",
            "local_admin_unit_name",
            "country",
            "geometry",
        ]
        return admin_units
