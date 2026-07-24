import logging
import os
import pathlib

import numpy as np
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file
from mobility.runtime.io.patch_openpyxl import patch_openpyxl
from mobility.spatial.selected_rows import read_selected_rows


class GermanLocalAdminUnitsCategories(FileAsset):
    """German urban unit categories used by the shared local-admin-units file."""

    def __init__(self):
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bkg"
            / "local_admin_units_categories_de.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("German local administrative unit categories already prepared. Reusing %s.", self.cache_path)
        return pd.read_parquet(self.cache_path)

    def get_by_ids(self, local_admin_unit_ids: list[str]) -> pd.DataFrame:
        """Return categories for the requested German local admin units."""
        return read_selected_rows(
            self,
            "local_admin_unit_id",
            local_admin_unit_ids,
            ["local_admin_unit_id", "urban_unit_category"],
        )
    

    def create_and_get_asset(self) -> pd.DataFrame:
        mapping_dictionary = {
            11: "C",
            12: "I",
            21: "B",
            22: "B",
            30: "B",
            40: "R",
            50: "R"
        }
        path = "data/germany/raumgliederungen-referenzen-2024.xlsx"
        categories = pd.read_excel(
            path,
            sheet_name="Gemeindereferenz (inkl.Kreise)",
            skiprows=2,
        )
        categories = categories.iloc[:, [1, 52]]
        categories.columns = ["local_admin_unit_id", "urban_unit_category"]
        categories["local_admin_unit_id"] = "de-" + categories["local_admin_unit_id"].astype(str)
        categories["urban_unit_category"] = categories["urban_unit_category"].map(mapping_dictionary)
        categories.to_parquet(self.cache_path)
        return categories
