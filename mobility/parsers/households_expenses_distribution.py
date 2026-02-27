import os
import pathlib
import logging
import zipfile
import pandas as pd
import py7zr

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file

class HouseholdsExpensesDistribution(FileAsset):
    """
    This class processes and retrieves the spatial distribution of household expenses
    based on INSEE and BFS data for France and Switzerland.
    It stores expenses separately for shops, hobbies, and studies.
    """

    def __init__(self):
        inputs = {"v": 4}
        cache_path = {
            "shops_expenses": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "shops_expenses.parquet",
            "hobbies_expenses": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "hobbies_expenses.parquet",
            "studies_expenses": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "studies_expenses.parquet"
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> dict:
        """Retrieve cached household expenses data."""
        logging.info("Using cached household expenses data.")
        return {
            "shops": pd.read_parquet(self.cache_path["shops_expenses"]),
            "hobbies": pd.read_parquet(self.cache_path["hobbies_expenses"]),
            "studies": pd.read_parquet(self.cache_path["studies_expenses"])
        }

    def create_and_get_asset(self) -> dict:
        """Create and retrieve the household expenses data if not cached."""
        hh_expenses_fr = self.prepare_french_households_expenses()
        hh_expenses_ch = self.prepare_swiss_households_expenses()
        print("CH:")
        print(hh_expenses_ch)
        
        hh_expenses = pd.concat([hh_expenses_fr, hh_expenses_ch])
        print(hh_expenses)
        
        for category in ["shops", "hobbies", "studies"]:
            df_category = hh_expenses[hh_expenses["expenses_category"] == category]
            df_category.to_parquet(self.cache_path[f"{category}_expenses"])
        
        return self.get_cached_asset()

    def prepare_french_households_expenses(self) -> pd.DataFrame:
        """Prepare household expenses data for France."""
        logging.info("Preparing household expenses data for France.")
        insee_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        url_hh_expenses = 'https://www.insee.fr/fr/statistiques/fichier/4648319/TM106.csv'
        hh_expenses_path = insee_data_folder / "tm106.csv"
        download_file(url_hh_expenses, hh_expenses_path)
        
        hh_expenses_fr = pd.read_csv(hh_expenses_path, sep=";")
        hh_expenses_fr = hh_expenses_fr[hh_expenses_fr["NOMENCLATURE"].str.len() == 2]

        categories = {
            "shops": ["01", "02", "03", "05", "11", "12"],
            "hobbies": ["09"],
            "studies": ["10"]
        }
        category_map = {code: category for category, codes in categories.items() for code in codes}
        hh_expenses_fr["expenses_category"] = hh_expenses_fr["NOMENCLATURE"].replace(category_map)
        hh_expenses_fr = hh_expenses_fr.groupby(["expenses_category", "DECUC"]).agg({"CONSO": "sum"}).reset_index()
        
        hh_expenses_fr = self.spatialize_french_expenses(hh_expenses_fr, insee_data_folder)
        return hh_expenses_fr

    def spatialize_french_expenses(self, hh_expenses_fr: pd.DataFrame, insee_data_folder: pathlib.Path) -> pd.DataFrame:
        """Spatialize household expenses data for France."""
        url_hh_fr = "https://www.insee.fr/fr/statistiques/fichier/6215138/Filosofi2017_carreaux_200m_csv.zip"
        hh_fr_zip_path = insee_data_folder / "Filosofi2017_carreaux_200m_csv.zip"
        download_file(url_hh_fr, hh_fr_zip_path)
        
        with zipfile.ZipFile(hh_fr_zip_path, "r") as zip_ref:
            zip_ref.extractall(insee_data_folder)
        
        with py7zr.SevenZipFile(insee_data_folder / "Filosofi2017_carreaux_200m_csv.7z", mode="r") as archive:
            archive.extractall(path=insee_data_folder)
            
        hh_fr = pd.read_csv(
            insee_data_folder /"Filosofi2017_carreaux_200m_met.csv" 
        )
        
        hh_fr[["coord_Y", "coord_X"]] = hh_fr["Idcar_200m"].str.extract(r"N(\d+)E(\d+)")
        hh_fr = hh_fr[['lcog_geo', 'coord_Y', 'coord_X', 'Men', 'Ind_snv']]
        hh_fr["lcog_geo"] = hh_fr["lcog_geo"].astype(str).str.zfill(5).str[:5]
        
        hh_fr['Ind_snv_decile'] = pd.qcut(hh_fr['Ind_snv'], q=10, labels=range(1, 11), duplicates='drop')
        
        hh_fr["Ind_snv_decile"] = pd.to_numeric(hh_fr["Ind_snv_decile"], errors="coerce")
        hh_expenses_fr["DECUC"] = pd.to_numeric(hh_expenses_fr["DECUC"], errors="coerce")

        hh_expenses_fr = pd.merge(
            hh_fr, 
            hh_expenses_fr, 
            left_on = "Ind_snv_decile", 
            right_on = "DECUC"
            )

        hh_expenses_fr["CONSO"] = hh_expenses_fr["CONSO"] * hh_expenses_fr["Men"] 

        hh_expenses_fr = hh_expenses_fr.groupby(['lcog_geo', 'coord_Y', 'coord_X', 'expenses_category']).agg({
            "CONSO": "sum"
        }).reset_index()
        hh_expenses_fr.columns = ['local_admin_unit_id', 'coord_Y', 'coord_X', 'expenses_category', 'expenses']
        hh_expenses_fr["local_admin_unit_id"] = "fr-" + hh_expenses_fr["local_admin_unit_id"]

        hh_fr_conso_city =  hh_expenses_fr.groupby(['local_admin_unit_id', 'expenses_category']).agg({
            "expenses": "sum"
        }).reset_index()

        os.unlink(hh_fr_zip_path)
        os.unlink(insee_data_folder / "Filosofi2017_carreaux_200m_csv.7z")
        os.unlink(insee_data_folder /"Filosofi2017_carreaux_200m_met.csv")
          
        return hh_fr_conso_city
    
    def prepare_swiss_households_expenses(self) -> pd.DataFrame:
        """Prepare household expenses data for Switzerland."""
        logging.info("Preparing household expenses data for Switzerland.")
        bfs_data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs"
        url_hh_ch_expenses = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32667039/master"
        hh_expenses_ch_path = bfs_data_folder / "je-f-20.02.01.02.01.xlsx"
        download_file(url_hh_ch_expenses, hh_expenses_ch_path)
        
        hh_expenses_ch = pd.read_excel(hh_expenses_ch_path, skiprows=12, usecols=[1, 6])
        hh_expenses_ch.columns = ['category', 'expenses']
        hh_expenses_ch = hh_expenses_ch.dropna(subset=["category", "expenses"])
        hh_expenses_ch["category_id"] = hh_expenses_ch["category"].str[:2]
        
        
        categories_ch = {
            "shops": ["51", "52", "56", "58", "68"],
            "hobbies": ["68"],
            "studies": ["67"]
        }
        category_map_ch = {code: category for category, codes in categories_ch.items() for code in codes}
        hh_expenses_ch["expenses_category"] = hh_expenses_ch["category_id"].apply(lambda x: category_map_ch.get(x, "others"))
        print('CH before grouping')
        print(hh_expenses_ch)
        
        hh_expenses_ch = hh_expenses_ch.groupby(["expenses_category"]).agg({"expenses": "sum"}).reset_index()
        hh_expenses_ch["expenses"] = hh_expenses_ch["expenses"] * 12 / 0.99  # Convert to yearly expenses in euros
        print('CH after grouping')
        print(hh_expenses_ch)
        
        hh_expenses_ch = self.spatialize_swiss_expenses(hh_expenses_ch, bfs_data_folder)
        print('CH spatialized')
        print(hh_expenses_ch)
        return hh_expenses_ch

    def spatialize_swiss_expenses(self, hh_expenses_ch: pd.DataFrame, bfs_data_folder: pathlib.Path) -> pd.DataFrame:
        """Spatialize household expenses data for Switzerland."""
        url_hh_ch = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32408791/master"
        hh_ch_path = bfs_data_folder / "cc-f-01.02.02.01.xlsx"
        download_file(url_hh_ch, hh_ch_path)
        
        hh_ch = pd.read_excel(hh_ch_path, skiprows = 2, usecols=range(0, 2))
        hh_ch.columns = ["city", "tot"]
        hh_ch = hh_ch.iloc[1:].reset_index(drop=True)
        hh_ch["city_id"] = hh_ch["city"].str[:4]
        
        hh_expenses_ch = hh_ch.merge(hh_expenses_ch, how="cross")
        hh_expenses_ch["expenses"] = hh_expenses_ch["expenses"] * hh_expenses_ch["tot"]
        hh_expenses_ch = hh_expenses_ch.dropna(subset=["expenses"])

        
        hh_expenses_ch = hh_expenses_ch[["city_id", "expenses_category", "expenses"]]
        hh_expenses_ch.columns = ['local_admin_unit_id', 'expenses_category', 'expenses']
        hh_expenses_ch["local_admin_unit_id"] = "ch-" + hh_expenses_ch["local_admin_unit_id"]
        
        return hh_expenses_ch
    
