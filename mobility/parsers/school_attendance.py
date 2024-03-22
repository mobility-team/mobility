# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 11:32:26 2024

@author: Formation
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

    #data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/school"
    
    data_folder_path=Path("C:/Users/Formation/Documents/GitHub/mobility/mobility/data/insee/schools")

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
            "Code_commune",
            "Code_departement",
            "Code_region",
            "Type_etablissement",
            "code_nature",
            "Nombre_d_eleves",
        ]
        )
    
    db_schools["code_nature_simp"]=db_schools["code_nature"]//100
    
    db_schools_filtered = db_schools.query("code_nature_simp != 8")

    db_schools_group = db_schools_filtered.loc[
        :, 
        [
            "Code_commune",
            "Nombre_d_eleves",
            "code_nature_simp",
        ],
        ].groupby(["Code_commune", "code_nature_simp"]).sum().reset_index()
    db_schools_group.set_index("Code_commune", inplace=True)
    
    
    #     dtype={"CODGEO": str},
    # )

    # db_schools = db_school_attendance.loc[
    #     :,
    #     [
    #         "CODGEO",
    #         "P19_EMPLT",
    #         "C19_EMPLT_CS1",
    #         "C19_EMPLT_CS2",
    #         "C19_EMPLT_CS3",
    #         "C19_EMPLT_CS4",
    #         "C19_EMPLT_CS5",
    #         "C19_EMPLT_CS6",
    #     ],
    # ]
    # db_.set_index("CODGEO", inplace=True)
    # db_jobs.rename(
    #     columns={
    #         "P19_EMPLT": "n_jobs_total",
    #         "C19_EMPLT_CS1": "n_jobs_CS1",
    #         "C19_EMPLT_CS2": "n_jobs_CS2",
    #         "C19_EMPLT_CS3": "n_jobs_CS3",
    #         "C19_EMPLT_CS4": "n_jobs_CS4",
    #         "C19_EMPLT_CS5": "n_jobs_CS5",
    #         "C19_EMPLT_CS6": "n_jobs_CS6",
    #     },
    #     inplace=True,
    # )

    # db_active_population = db_job_active_pop.loc[
    #     :,
    #     [
    #         "CODGEO",
    #         "P19_ACT15P",
    #         "C19_ACT1564_CS1",
    #         "C19_ACT1564_CS2",
    #         "C19_ACT1564_CS3",
    #         "C19_ACT1564_CS4",
    #         "C19_ACT1564_CS5",
    #         "C19_ACT1564_CS6",
    #     ],
    # ]
    # db_active_population.set_index("CODGEO", inplace=True)
    # db_active_population.rename(
    #     columns={
    #         "P19_ACT15P": "active_pop",
    #         "C19_ACT1564_CS1": "active_pop_CS1",
    #         "C19_ACT1564_CS2": "active_pop_CS2",
    #         "C19_ACT1564_CS3": "active_pop_CS3",
    #         "C19_ACT1564_CS4": "active_pop_CS4",
    #         "C19_ACT1564_CS5": "active_pop_CS5",
    #         "C19_ACT1564_CS6": "active_pop_CS6",
    #     },
    #     inplace=True,
    # )

    # ------------------------------------------
    # Write datasets to parquet files
    db_schools_group.to_parquet(data_folder_path / "schools.parquet")
    # db_active_population.to_parquet(data_folder_path / "active_population.parquet")

    return db_schools