import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def prepare_dwellings(reg, proxies={}):
    """
    Download, process, and save Insee dwellings data for a specific region.

    Args:
        reg (str): Region code. Valid values are "11", "24", "27", "28", "32", "44", "52", "53", "75", "76".
        proxies (dict, optional): Proxy configuration for downloading data.

    Returns:
        None
    """

    # Define the data folder path where the downloaded and processed data will be stored.
    data_folder_path = (
        Path(os.path.dirname(__file__)).parents[0] / "data/insee/dwellings"
    )

    # Create the data folder if it doesn't exist.
    if not data_folder_path.exists():
        os.makedirs(data_folder_path)
    
    if reg=="11":
        zone = "A"
        url = "https://www.data.gouv.fr/fr/datasets/r/30f90960-2839-4bc6-8f20-d5679832793d"
    elif reg in ["24", "27", "28", "32"]:
        zone = "B"
        url = "https://www.data.gouv.fr/fr/datasets/r/e6e042cc-9780-458d-b732-888c21433496"
    elif reg in ["44", "52", "53"]:
        zone = "C"
        url = "https://www.data.gouv.fr/fr/datasets/r/41797024-a56d-4f50-a0dc-07d5e92bcb53"
    elif reg in ["75", "76"]:
        zone = "D"
        url = "https://www.data.gouv.fr/fr/datasets/r/8a325743-2ad5-4ebc-8574-529e3b9cfd5e"
    else:
        zone = "E"
        url = "https://www.data.gouv.fr/fr/datasets/r/8b6cb882-5f1b-4f0d-890c-d78de3b4efa0"


    # Define the path to the ZIP file containing the Insee data.
    path = data_folder_path / f"RP2019_INDCVIZ{zone}_csv.zip"

    # Download the raw survey data from the Insee population census.
    if not path.exists():
        # Download the ZIP file from the Insee website.
        r = requests.get(
            url=url,
            proxies=proxies,
        )
        with open(path, "wb") as file:
            file.write(r.content)

        # Unzip the content of the downloaded ZIP file.
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)
            
        path_csv = data_folder_path / f"FD_INDCVIZ{zone}_2019.csv"
            
        # Read data from the csv.
        dwellings = pd.read_csv(
            path_csv, 
            sep = ";", 
            usecols=[
                "CANTVILLE", 
                "LPRM", 
                "NPERR", 
                "NBPI", 
                "AGEREV", 
                "SFM", 
                "STOCD", 
                "SURF", 
                "IPONDI", 
                "CS1", 
                "TACT", 
                "AGEREVQ", 
                "NA5", 
                "NUMMI", 
                "ARM", 
                "GARL", 
                "VOIT"
            ],
            dtype={"CANTVILLE": str},
        )
                
        dwellings = dwellings.astype(str)

        dwellings.to_parquet(data_folder_path / f"FD_INDCVIZ{zone}_2019.parquet", compression='gzip')
                

   