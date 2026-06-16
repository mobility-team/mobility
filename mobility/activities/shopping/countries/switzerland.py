import os
import pathlib
import zipfile

import geopandas as gpd
import pandas as pd

from mobility.activities.shopping.countries.french_swiss_turnover import (
    FrenchSwissShoppingTurnover,
)
from mobility.runtime.io.download_file import download_file
from mobility.spatial.local_admin_units import LocalAdminUnits


class SwissShoppingOpportunities:
    """Swiss shopping opportunities."""

    def filter_by_local_admin_unit_id(
        self,
        local_admin_unit_ids: list[str],
    ) -> pd.DataFrame:
        bfs_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs"
        turnover = FrenchSwissShoppingTurnover()
        shops_turnover_ratio = turnover.prepare_shops_turnover_ratio()
        insee_to_naf = turnover.prepare_insee_to_naf()
        noga_to_naf = turnover.prepare_noga_to_naf()

        url_statent = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32258837/master"
        statent_zip_path = bfs_data_folder / "ag-b-00.03-22-STATENT2022.zip"
        download_file(url_statent, statent_zip_path)

        with zipfile.ZipFile(statent_zip_path, "r") as zip_ref:
            zip_ref.extractall(bfs_data_folder)

        statent_path = bfs_data_folder / "ag-b-00.03-22-STATENT2022" / "STATENT_2022.csv"
        swiss_employees_colnames = pd.read_csv(
            statent_path,
            sep=";",
            index_col=0,
            nrows=0,
        ).columns.tolist()
        selected_columns = [swiss_employees_colnames[i] for i in [1, 2, 3] + list(range(226, 311))]

        swiss_employees = pd.read_csv(
            statent_path,
            sep=";",
            usecols=selected_columns,
        )
        swiss_employees = swiss_employees.melt(id_vars=["E_KOORD", "N_KOORD", "RELI"])
        swiss_employees = swiss_employees[swiss_employees["value"] != 0]
        swiss_employees["NOGA"] = swiss_employees["variable"].str.extract(r"B08(\d+)VZA")

        swiss_employees = pd.merge(swiss_employees, noga_to_naf, on="NOGA")
        swiss_employees["naf_id"] = swiss_employees["NAF"].str.replace(".", "", regex=False).str[:3]

        insee_to_naf = insee_to_naf.copy()
        insee_to_naf["naf_id"] = insee_to_naf["code_naf"].str.replace(".", "", regex=False).str[:3]
        swiss_employees = pd.merge(swiss_employees, insee_to_naf, on="naf_id", how="left")
        swiss_employees = swiss_employees.dropna(subset=["code_equipement"])

        swiss_shop_employees = swiss_employees[swiss_employees["code_equipement"].str.startswith("B")]
        shops_turnover = pd.merge(
            swiss_shop_employees,
            shops_turnover_ratio,
            on=["code_equipement", "naf_id"],
            how="left",
        )
        shops_turnover = shops_turnover.groupby(
            ["E_KOORD", "N_KOORD", "RELI", "Description NOGA", "naf_id"],
        ).agg(
            {
                "value": "mean",
                "turnover_by_employee": "mean",
                "turnover_by_equipment": "mean",
            }
        ).reset_index()
        shops_turnover["turnover"] = shops_turnover["turnover_by_employee"] * shops_turnover["value"]

        grid_resolution = 100
        shops_turnover["E_KOORD_center"] = shops_turnover["E_KOORD"] + grid_resolution / 2
        shops_turnover["N_KOORD_center"] = shops_turnover["N_KOORD"] + grid_resolution / 2
        shops_turnover = gpd.GeoDataFrame(
            shops_turnover,
            geometry=gpd.points_from_xy(
                shops_turnover["E_KOORD_center"],
                shops_turnover["N_KOORD_center"],
            ),
            crs="EPSG:2056",
        )
        shops_turnover = shops_turnover.to_crs(epsg=3035)

        if local_admin_unit_ids:
            local_admin_units = LocalAdminUnits(local_admin_unit_ids=local_admin_unit_ids).get()
        else:
            local_admin_units = LocalAdminUnits(countries=["ch"]).get()
        shops_turnover = gpd.sjoin(shops_turnover, local_admin_units, how="left", predicate="within")
        shops_turnover = shops_turnover.dropna(subset=["local_admin_unit_id"])
        shops_turnover = shops_turnover.to_crs(epsg=4326)

        shops_turnover["lon"] = shops_turnover.geometry.x
        shops_turnover["lat"] = shops_turnover.geometry.y
        shops_turnover = pd.DataFrame(shops_turnover.drop(columns="geometry"))
        shops_turnover = shops_turnover[["local_admin_unit_id", "naf_id", "lon", "lat", "turnover"]]

        os.unlink(statent_zip_path)
        os.unlink(statent_path)

        return shops_turnover


class SwissShopping:
    """Swiss shopping inputs."""

    country = "ch"

    @property
    def opportunities(self):
        return SwissShoppingOpportunities()
