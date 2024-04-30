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
    
    data_folder_path = (
        Path(os.path.dirname(__file__)).parents[0] / "data/insee/schools"
    )
    if (data_folder_path/ "student_flow.parquet").exists() :
        db_student_flow=pd.read_parquet(data_folder_path/ "student_flow.parquet")
    
    else :
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
                "COMMUNE": "COMMUNE",
                "DCETUF": "DCLT",
                "AGEREV10": "Tranche_Age",
                "IPONDI": "IPONDI",
                
            },
            inplace=True,
        )
        
        new_rows = []
        for index, row in db_student_flow.iterrows():
            if row['Tranche_Age'] == 11:
                new_rows.append({'COMMUNE': row['COMMUNE'],'DCLT': row['DCLT'], 'Tranche_Age': 2, 'IPONDI': row['IPONDI']})
            elif row['Tranche_Age'] == 15:
                new_rows.append({'COMMUNE': row['COMMUNE'],'DCLT': row['DCLT'], 'Tranche_Age': 3, 'IPONDI': row['IPONDI']})
            elif row['Tranche_Age'] == 3  or row['Tranche_Age'] == 4 or row['Tranche_Age'] == 5  or row['Tranche_Age'] == 6:
                new_rows.append({'COMMUNE': row['COMMUNE'],'DCLT': row['DCLT'], 'Tranche_Age': 1, 'IPONDI': row['IPONDI']})
    
        db_student_flow = pd.DataFrame(new_rows)
        
        db_student_flow = db_student_flow.loc[
            :, 
            [
                "COMMUNE",
                "DCLT",
                "Tranche_Age",
                "IPONDI",
            ],
            ].groupby(["COMMUNE", "DCLT","Tranche_Age"]).sum().reset_index()
        # ------------------------------------------
        # Write datasets to parquet files
        db_student_flow.to_parquet(data_folder_path / "student_flow.parquet")

    return db_student_flow