"""Code ENSG."""

import pandas as pd
import os
from pathlib import Path
import requests
import zipfile



def prepare_french_schools_capacity(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data
    (hosted on data.education.gouv.fr by the  Ministère de l'Éducation Nationale et de la Jeunesse 
    https://data.education.gouv.fr/explore/dataset/fr-en-annuaire-education/),
    then creates one dataframe for the number schools by cities 
    and the type of school and the capacity
    and writes this into parquet file
    """

    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/schools"

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "fr-en-annuaire-education.csv"

    if not test:
        if path.exists() is False:
            # Download the zip file
            print("Downloading from data.education.gouv.fr, it can take several minutes...")
            r = requests.get(
                url="https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
                proxies=proxies,
            )
            with open(path, "wb") as file:
                file.write(r.content)

            
        path_csv = data_folder_path / "fr-en-annuaire-education.csv"
    else:
        print("Using a restricted dataset for test")
        path_csv = data_folder_path / "fr-en-annuaire-education.csv"

    # Informations about schools and attendance for each city
    db_schools= pd.read_csv(
        path_csv,
        sep=";",
        usecols=[

            "Code_commune" ,
            "Type_etablissement",
            "Nombre_d_eleves",
            "Identifiant_de_l_etablissement"

        ],
        dtype={
            "Code_commune": str,
            "Type_etablissement": str,
            "Identifiant_de_l_etablissement": str

            },
        )
    
    db_schools.rename(
        columns={
            "Code_commune": "CODGEO", 
            "Identifiant_de_l_etablissement": "Code_RNE"
        },
        inplace=True,
    )
    
    
    db_schools = db_schools.query("Type_etablissement == 'Ecole' or Type_etablissement == 'Collège' or Type_etablissement == 'Lycée'")
    
    db_schools['Type_etablissement'] = db_schools['Type_etablissement'].replace({'Ecole': 1, 'Collège': 2, 'Lycée': 3})
    
    db_schools = db_schools.loc[
        :, 
        [
            "CODGEO",
            "Nombre_d_eleves",
            "Type_etablissement",
            "Code_RNE"
        ],
        ].groupby(["CODGEO","Type_etablissement"]).sum().reset_index()
    
    
   
    # ------------------------------------------
    # Write datasets to parquet files
    db_schools.to_parquet(data_folder_path / "schools.parquet")
   

    return db_schools




def prepare_french_student_flows(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data at
    https://www.insee.fr/fr/statistiques/fichier/7637665/RP2020_mobsco_csv.zip,
    then creates one dataframe for the flows of the population in age to attend school, until high school
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



def prepare_french_student_population(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data
    (hosted on data.gouv.fr by the mobility project at
    https://www.data.gouv.fr/fr/datasets/emploi-population-active-en-2019/),
    then creates one dataframe for the number of job per city
    and one for the active population per city
    and writes these into parquet files
    """


    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/schools"

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