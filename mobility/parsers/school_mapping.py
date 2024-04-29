# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 14:18:26 2024

@author: @martaducamp
"""

import pandas as pd
import os
from pathlib import Path
import requests



def prepare_school_mapping(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data
    (hosted on data.education.gouv.fr by the  Ministère de l'Éducation Nationale et de la Jeunesse 
    https://data.education.gouv.fr/explore/dataset/fr-en-carte-scolaire-colleges-publics/),
    then creates a dataframe with addresses associated with each school
    """

    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/schools"
    
    #data_folder_path=Path("C:/Users/Formation/Documents/GitHub/mobility/mobility/data/insee/schools")

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "fr-en-carte-scolaire-colleges-publics.csv"

    if not test:
        if path.exists() is False:
            # Download the zip file
            print("Downloading from data.education.gouv.fr, it can take several minutes...")
            r = requests.get(
                url="https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-carte-scolaire-colleges-publics/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
                proxies=proxies,
            )
            with open(path, "wb") as file:
                file.write(r.content)

            
        path_csv = data_folder_path / "fr-en-carte-scolaire-colleges-publics.csv"
    else:
        print("Using a restricted dataset for test")
        path_csv = data_folder_path / "fr-en-carte-scolaire-colleges-publics.csv"

    # Informations about schools and attendance for each city
    db_schools_map= pd.read_csv(
        path_csv,
        sep=";",
        usecols=[
            "code_insee",
            "code_departement",
            "Code_RNE",
            "Secteur_unique",
        ],
        dtype={
            "Code_RNE": str, 
            "code_departement": str,
            "code_insee": str
            },
        )
    
    db_schools_map["Secteur_unique"]=db_schools_map["Secteur_unique"].map({"O": True, "N": False})
    

   
    # ------------------------------------------
    # Write datasets to parquet files
    db_schools_map.to_parquet(data_folder_path / "schools_map.parquet")
   

    return db_schools_map