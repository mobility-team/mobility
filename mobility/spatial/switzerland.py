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
        inputs = {"cache_version": 2}
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

        # The stable part of the BFS typology is the numeric code in parentheses.
        # The French wording can change slightly between source files.
        type_codes = categories["urban_unit_category"].astype(str).str.extract(r"\((\d{2})\)", expand=False)
        categories["urban_unit_category"] = type_codes.map(
            {
                "11": "C",
                "12": "C",
                "13": "I",
                "21": "B",
                "22": "B",
                "23": "B",
                "31": "R",
                "32": "R",
                "33": "R",
            }
        )
        unknown_categories = categories.loc[
            categories["urban_unit_category"].isna(),
            "local_admin_unit_id",
        ].tolist()
        if unknown_categories:
            raise ValueError(
                "No Swiss urban unit category could be read for local admin units: "
                f"{sorted(unknown_categories)}."
            )

        categories.to_parquet(self.cache_path)
        return categories
