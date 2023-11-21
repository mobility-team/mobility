import os
import pathlib
import requests
import zipfile

def prepare_urban_units(proxies={}):
    
    # The directory path where dataFrames/ Parquets are stored
    data_folder_path = (
        pathlib.Path(os.path.dirname(__file__)).parents[0] / "data/insee/territories"
    )

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the IGN data from data.gouv.fr if needed
    path = data_folder_path / "UU2020_au_01-01-2023.zip"
    
    if path.exists() is False:
        
        print("Downloading urban units data (INSEE)")
        
        # Download the zip file
        r = requests.get(
            url="https://www.data.gouv.fr/fr/datasets/r/c59f74bb-8095-4e41-9627-5fecca95668d",
            proxies=proxies
        )
        with open(path, "wb") as file:
            file.write(r.content)

        # Unzip the content
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)


    