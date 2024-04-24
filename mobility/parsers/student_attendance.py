import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def prepare_student_attendance(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data
    (hosted on data.gouv.fr by the mobility project at
    https://www.data.gouv.fr/fr/datasets/emploi-population-active-en-2019/),
    then creates one dataframe for the number of job per city
    and one for the active population per city
    and writes these into parquet files
    """

    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/schools"
    # data_folder_path =Path("C:/Users/bapti/OneDrive/Documents/GitHub/mobility/mobility/data/insee/schools")    
    
    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "TD_POP1A_2020_csv.zip"

    if not test:
        if path.exists() is False:
            # Download the zip file
            print("Downloading from insee.fr, it can take several minutes...")
            r = requests.get(
                url="https://www.insee.fr/fr/statistiques/fichier/7631680/TD_POP1A_2020_csv.zip",
                proxies=proxies,
            )
            with open(path, "wb") as file:
                file.write(r.content)

            # Unzip the content
            with zipfile.ZipFile(path, "r") as zip_ref:
                zip_ref.extractall(data_folder_path)
        path_csv = data_folder_path / "TD_POP1A_2020.csv"
    else:
        print("Using a restricted dataset for test")
        path_csv = data_folder_path / "TD_POP1A_2020.csv"

    # Informations about school and student population for each city
    db_student= pd.read_csv(
        path_csv,
        sep=";",
        usecols=[
            "CODGEO",
            "SEXE",
            "AGEPYR10",
            "NB"
        ],
        dtype={"CODGEO": str},
    )

    db_student.rename(
        columns={
            "AGEPYR10": "TrancheAge",
            "NB": "Nombre"
            
        },
        inplace=True,
    )
    
    db_student = db_student.loc[
        :, 
        [
            "CODGEO",
            "Nombre",
            "TrancheAge"
        ],
        ].groupby([ "CODGEO","TrancheAge"]).sum().reset_index()
    
    db_student = db_student.query("TrancheAge == 3 or TrancheAge == 6 or TrancheAge == 11")
    
    new_rows = []
    for index, row in db_student.iterrows():
        if row['TrancheAge'] == 11:  # Changer 'B2' à la valeur désirée
            new_rows.append({'CODGEO': row['CODGEO'], 'TrancheAge': 2, 'Nombre': row['Nombre'] * (3/7)})
            new_rows.append({'CODGEO': row['CODGEO'], 'TrancheAge': 3, 'Nombre': row['Nombre'] * (4/7)})
        elif row['TrancheAge'] == 6 or row['TrancheAge'] == 3:
            new_rows.append({'CODGEO': row['CODGEO'], 'TrancheAge': 1, 'Nombre': row['Nombre']})
        else:
            new_rows.append({'CODGEO': row['CODGEO'], 'TrancheAge': row['TrancheAge'], 'Nombre': row['Nombre']})


    # Création du nouveau DataFrame
    db_student = pd.DataFrame(new_rows)
    
    db_student = db_student.loc[
        :, 
        [
            "CODGEO",
            "Nombre",
            "TrancheAge"
        ],
        ].groupby(['CODGEO', 'TrancheAge'])['Nombre'].sum().reset_index()
    
    
    # ------------------------------------------
    # Write datasets to parquet files
    db_student.to_parquet(data_folder_path / "students.parquet")

    return db_student