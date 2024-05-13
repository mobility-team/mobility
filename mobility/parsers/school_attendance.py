# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 11:32:26 2024

@author: @martaducamp
"""

import pandas as pd
import os
from pathlib import Path
import requests



def prepare_school_attendance(proxies={}, test=False):
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


