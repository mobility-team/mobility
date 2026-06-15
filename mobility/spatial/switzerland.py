import logging
import os
import pathlib

import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file
from mobility.spatial.selected_rows import read_selected_rows


class SwissLocalAdminUnitsCategories(FileAsset):
    """Swiss municipality categories used by the shared local-admin-units file."""

    def __init__(self):
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bfs"
            / "local_admin_units_categories_ch.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Swiss local administrative unit categories already prepared. Reusing %s.", self.cache_path)
        return pd.read_parquet(self.cache_path)

    def get_by_ids(self, local_admin_unit_ids: list[str]) -> pd.DataFrame:
        """Return categories for the requested Swiss local admin units."""
        return read_selected_rows(
            self,
            "local_admin_unit_id",
            local_admin_unit_ids,
            ["local_admin_unit_id", "urban_unit_category"],
        )

    def create_and_get_asset(self) -> pd.DataFrame:
        url = "https://www.data.gouv.fr/fr/datasets/r/c776c9fe-5405-4568-b456-65209387035b"
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs"
        file_path = data_folder / "BFS - Typologie des communes 2020 en 9 catégories.xlsx"
        file_path = download_file(url, file_path)

        categories = pd.read_excel(file_path, skiprows=4, skipfooter=11)
        categories = categories.iloc[:, [0, 2]]
        categories.columns = ["local_admin_unit_id", "urban_unit_category"]
        categories["local_admin_unit_id"] = "ch-" + categories["local_admin_unit_id"].astype(str)
        categories["urban_unit_category"] = categories["urban_unit_category"].map(
            {
                "Commune urbaine d’une grande agglomération (11)": "C",
                "Commune urbaine d'une agglomération moyenne (12)": "C",
                "Commune urbaine d’une petite ou hors agglomération (13)": "I",
                "Commune périurbaine de forte densité (21)": "B",
                "Commune périurbaine de moyenne densité (22)": "B",
                "Commune périurbaine de faible densité (23)": "B",
                "Commune d’un centre rural (31)": "R",
                "Commune rurale en situation centrale (32)": "R",
                "Commune rurale périphérique (33)": "R",
            }
        )

        categories.to_parquet(self.cache_path)
        return categories
