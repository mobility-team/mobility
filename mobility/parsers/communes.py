import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def communes_data(proxies={}):
    """
    Downloads (if needed) the INSEE census mobility flows data
    (hosted on data.gouv.fr by the mobility project at
    https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25
    """

    data_folder_path = (
        Path(os.path.dirname(__file__)).parents[0] / "data/insee/commune_data"
    )

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)
    

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "donneesCommunes.csv"

    if path.exists() is False:
        # Download the zip file
        r = requests.get(
            url="https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25",
            proxies=proxies,
        )
        with open(path, "wb") as file:
            file.write(r.content)

        # Unzip the content
        '''with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)'''

    return str(data_folder_path / "donneesCommunes.csv")