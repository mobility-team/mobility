import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def prepare_school_VT(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data
    (hosted on data.gouv.fr by the mobility project at
    https://www.data.gouv.fr/fr/datasets/emploi-population-active-en-2019/),
    then creates one dataframe for the number of job per city
    and one for the active population per city
    and writes these into parquet files
    """

    # data_folder_path = Path("D:/PDI01/mobility-stage-2-model/mobility/parsers/data/insee/school")
    data_folder_path =Path("C:/Users/bapti/OneDrive/Documents/GitHub/mobility/mobility/data/insee/schools")    

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "RP2020_MOBSCO_csv.zip"

    if not test:
        if path.exists() is False:
            # Download the zip file
            print("Downloading from insee.fr, it can take several minutes...")
            r = requests.get(
                url="https://www.insee.fr/fr/statistiques/fichier/7637665/RP2020_mobsco_csv.zip",
                proxies=proxies,
            )
            with open(path, "wb") as file:
                file.write(r.content)

            # Unzip the content
            with zipfile.ZipFile(path, "r") as zip_ref:
                zip_ref.extractall(data_folder_path)
        path_csv = data_folder_path / "FD_MOBSCO_2020.csv"
    else:
        print("Using a restricted dataset for test")
        path_csv = data_folder_path / "FD_MOBSCO_2020.csv"

    # Informations about school and student population for each city
    db_student_flow = pd.read_csv(
        path_csv,
        sep=";",
        usecols=[
            "COMMUNE",
            "DCETUF",
            "AGEREV10",
            "IPONDI"
        ],
        dtype={"COMMUNE": str, "DCETUF": str,},
    )

    db_student_flow.rename(
        columns={
            "COMMUNE": "Commune_Residence",
            "DCETUF": "Commune_Scolarite",
            "AGEREV10": "Tranche_Age",
            "IPONDI": "Poids_Eleve",
            
        },
        inplace=True,
    )


    # ------------------------------------------
    # Write datasets to parquet files
    db_student_flow.to_parquet(data_folder_path / "student_flow.parquet")

    return db_student_flow