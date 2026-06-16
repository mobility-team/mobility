import logging
import os
import pathlib
import zipfile

import numpy as np
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file
from mobility.runtime.io.patch_openpyxl import patch_openpyxl
from mobility.spatial.selected_rows import read_selected_rows


class FrenchLocalAdminUnitsCategories(FileAsset):
    """French urban unit categories used by the shared local-admin-units file."""

    def __init__(self):
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "local_admin_units_categories_fr.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("French local administrative unit categories already prepared. Reusing %s.", self.cache_path)
        return pd.read_parquet(self.cache_path)

    def get_by_ids(self, local_admin_unit_ids: list[str]) -> pd.DataFrame:
        """Return categories for the requested French local admin units."""
        return read_selected_rows(
            self,
            "local_admin_unit_id",
            local_admin_unit_ids,
            ["local_admin_unit_id", "urban_unit_category"],
        )

    def create_and_get_asset(self) -> pd.DataFrame:
        url = "https://www.data.gouv.fr/fr/datasets/r/c59f74bb-8095-4e41-9627-5fecca95668d"
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "UU2020_au_01-01-2023.zip"
        download_file(url, path)

        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(path.parent)

        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "UU2020_au_01-01-2023.xlsx"
        patch_openpyxl()

        categories = pd.read_excel(
            path,
            sheet_name="Composition_communale",
            skiprows=5,
        )
        categories = categories.iloc[:, [0, 5]]
        categories.columns = ["local_admin_unit_id", "urban_unit_category"]

        arr_categories = pd.concat(
            [
                pd.DataFrame(
                    {
                        "local_admin_unit_id": np.arange(75101, 75121, 1).astype(str),
                        "urban_unit_category": "C",
                    }
                ),
                pd.DataFrame(
                    {
                        "local_admin_unit_id": np.arange(69381, 69390, 1).astype(str),
                        "urban_unit_category": "C",
                    }
                ),
                pd.DataFrame(
                    {
                        "local_admin_unit_id": np.arange(13201, 13218, 1).astype(str),
                        "urban_unit_category": "C",
                    }
                ),
            ]
        )

        categories = categories[~categories["local_admin_unit_id"].isin(["69123", "75056", "13055"])]
        categories = pd.concat([categories, arr_categories])
        categories["local_admin_unit_id"] = "fr-" + categories["local_admin_unit_id"].astype(str)
        categories["urban_unit_category"] = np.where(
            categories["urban_unit_category"] != "H",
            categories["urban_unit_category"],
            "R",
        )

        categories.to_parquet(self.cache_path)
        return categories
