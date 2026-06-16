import os
import pathlib

import pandas as pd
import pyarrow.parquet as pq

from mobility.activities.shopping.countries.french_swiss_turnover import (
    FrenchSwissShoppingTurnover,
)
from mobility.runtime.io.download_file import download_file


class FrenchShoppingOpportunities:
    """French shopping opportunities."""

    def filter_by_local_admin_unit_id(
        self,
        local_admin_unit_ids: list[str],
    ) -> pd.DataFrame:
        insee_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        turnover = FrenchSwissShoppingTurnover()
        shops_turnover_ratio = turnover.prepare_shops_turnover_ratio()

        url = "https://www.data.gouv.fr/api/1/datasets/r/b532ef31-edd9-4017-adda-b077aa0d39e3"
        parquet_path = insee_data_folder / "BPE24.parquet"
        download_file(url, parquet_path)

        french_shops = pq.read_table(parquet_path).to_pandas()
        french_shops = french_shops.dropna(subset=["LONGITUDE"])
        french_shops = french_shops[french_shops["TYPEQU"].str.startswith("B")]

        shops_turnover = pd.merge(
            french_shops,
            shops_turnover_ratio,
            left_on="TYPEQU",
            right_on="code_equipement",
            how="left",
        )
        shops_turnover = shops_turnover[
            ["DEPCOM", "naf_id", "LONGITUDE", "LATITUDE", "turnover_by_equipment"]
        ]
        shops_turnover.columns = ["local_admin_unit_id", "naf_id", "lon", "lat", "turnover"]
        shops_turnover["local_admin_unit_id"] = "fr-" + shops_turnover["local_admin_unit_id"]
        if local_admin_unit_ids:
            shops_turnover = shops_turnover[
                shops_turnover["local_admin_unit_id"].isin(local_admin_unit_ids)
            ].copy()

        os.unlink(parquet_path)

        return shops_turnover


class FrenchShopping:
    """French shopping inputs."""

    country = "fr"

    @property
    def opportunities(self):
        return FrenchShoppingOpportunities()
