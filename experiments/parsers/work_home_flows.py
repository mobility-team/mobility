import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def download_work_home_flows(proxies={}):
    """
    Downloads (if needed) the INSEE census mobility flows data
    (hosted on data.gouv.fr by the mobility project at
    https://www.data.gouv.fr/fr/datasets/r/f3f22487-22d0-45f4-b250-af36fc56ccd0)
    """

    data_folder_path = (
        Path(os.path.dirname(__file__)).parents[0] / "data/insee/work_home_flows"
    )

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "rp2019-mobpro-csv.zip"

    if path.exists() is False:
        # Download the zip file
        r = requests.get(
            url="https://www.data.gouv.fr/fr/datasets/r/f3f22487-22d0-45f4-b250-af36fc56ccd0",
            proxies=proxies,
        )
        with open(path, "wb") as file:
            file.write(r.content)

        # Unzip the content
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)

    return str(data_folder_path / "FD_MOBPRO_2019.csv")
