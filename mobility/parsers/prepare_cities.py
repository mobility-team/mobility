import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def prepare_cities(proxies={}):
    """
    Downloads and processes Insee data related to french Communal Division.
    Concatenates the cities and neighborhoods data frames into a single dataset.
    Args:
        proxies (dict, optional): A dictionary of proxy settings if needed for the download. Defaults to None.

    Returns:
        None

    Downloads the data from the Insee website, unzips it, reads Excel sheets, 
    processes the data, and saves it as a compressed CSV file.
    """
    # Define the data folder path where the downloaded and processed data will be stored.
    data_folder_path = (
        Path(os.path.dirname(__file__)).parents[0] / "data/insee/cities"
    )

    # Create the data folder if it doesn't exist.
    if not data_folder_path.exists():
        os.makedirs(data_folder_path)

    # Define the path to the ZIP file containing the Insee data.
    path = data_folder_path / "table-appartenance-geo-communes-22.zip"

    # Download the raw survey data from the Insee Communal Division.
    if not path.exists():
        # Download the ZIP file from the Insee website.
        r = requests.get(
            url="https://www.insee.fr/fr/statistiques/fichier/2028028/table-appartenance-geo-communes-22.zip",
            proxies=proxies,
        )
        with open(path, "wb") as file:
            file.write(r.content)

        # Unzip the content of the downloaded ZIP file.
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)
            
        # Read data from the Excel sheet "COM".
        com = pd.read_excel(
            data_folder_path / "table-appartenance-geo-communes-22.xlsx",
            sheet_name="COM",
            skiprows=5
        )
        
        # Filter out specific "CODGEO" values.
        com = com.loc[~com["CODGEO"].isin(["75056", "69123", "13201"])]
        
        # Read data from the Excel sheet "ARM" and skip the first 5 rows.
        arm = pd.read_excel(
            data_folder_path / "table-appartenance-geo-communes-22.xlsx",
            sheet_name="ARM",
            skiprows=5
        )
        
        # Concatenate the "COM" and "ARM" data frames.
        com = pd.concat([com, arm])
        
        # Select specific columns for the final dataset.
        com = com[["CODGEO", "LIBGEO", "DEP", "REG", "CV"]]
        
        # Save the processed data as a compressed CSV file in the data folder.
        com.to_csv(data_folder_path / "cities.csv.gz", compression="gzip", 
                   encoding="utf-8", index=False)

   