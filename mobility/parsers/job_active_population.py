import pandas as pd
import os
from pathlib import Path
import requests
import zipfile

def prepare_job_active_population(proxies={}):
    """
    Downloads (if needed) the raw census data from the INSEE data
    (https://www.insee.fr/fr/statistiques/5395838?sommaire=5395900),
    then creates one dataframe for the number of job per city
    and one for the active population per city
    and writes these into parquet files
    """
    
    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/work"
    
    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)
    
    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "base-ccc-emploi-pop-active-2018.zip"
    
    if path.exists() is False:
        # Download the zip file
        r = requests.get(
            url="https://www.insee.fr/fr/statistiques/fichier/5395838/base-ccc-emploi-pop-active-2018.zip",
            proxies=proxies
        )
        with open(path, "wb") as file:
            file.write(r.content)
        
        # Unzip the content
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)
    
    # Informations about jobs and active population for each city
    db_job_active_pop = pd.read_csv(data_folder_path / "base-cc-emploi-pop-active-2018.csv",
                                    sep=';', usecols=['CODGEO',
                                                      'P18_ACT15P',
                                                      'C18_ACT1564_CS1', 'C18_ACT1564_CS2',
                                                      'C18_ACT1564_CS3', 'C18_ACT1564_CS4',
                                                      'C18_ACT1564_CS5', 'C18_ACT1564_CS6',
                                                      'P18_EMPLT',
                                                      'C18_EMPLT_CS1', 'C18_EMPLT_CS2',
                                                      'C18_EMPLT_CS3', 'C18_EMPLT_CS4',
                                                      'C18_EMPLT_CS5', 'C18_EMPLT_CS6'],
                                    dtype={'CODGEO':str})
    
    db_jobs = db_job_active_pop.loc[:, ['CODGEO', 'P18_EMPLT',
                                        'C18_EMPLT_CS1', 'C18_EMPLT_CS2', 'C18_EMPLT_CS3', 
                                        'C18_EMPLT_CS4', 'C18_EMPLT_CS5', 'C18_EMPLT_CS6']]
    db_jobs.set_index('CODGEO', inplace=True)
    db_jobs.rename(columns={'P18_EMPLT': 'n_jobs_total',
                            'C18_EMPLT_CS1': 'n_jobs_CS1', 'C18_EMPLT_CS2': 'n_jobs_CS2',
                            'C18_EMPLT_CS3': 'n_jobs_CS3', 'C18_EMPLT_CS4': 'n_jobs_CS4',
                            'C18_EMPLT_CS5': 'n_jobs_CS5', 'C18_EMPLT_CS6': 'n_jobs_CS6'},
                   inplace=True)
    
    db_active_population = db_job_active_pop.loc[:, ['CODGEO', 'P18_ACT15P',
                                                     'C18_ACT1564_CS1', 'C18_ACT1564_CS2',
                                                     'C18_ACT1564_CS3', 'C18_ACT1564_CS4',
                                                     'C18_ACT1564_CS5', 'C18_ACT1564_CS6']]
    db_active_population.set_index('CODGEO', inplace=True)
    db_active_population.rename(columns={'P18_ACT15P': 'active_pop',
                                         'C18_ACT1564_CS1': 'active_pop_CS1',
                                         'C18_ACT1564_CS2': 'active_pop_CS2',
                                         'C18_ACT1564_CS3': 'active_pop_CS3',
                                         'C18_ACT1564_CS4': 'active_pop_CS4',
                                         'C18_ACT1564_CS5': 'active_pop_CS5',
                                         'C18_ACT1564_CS6': 'active_pop_CS6'},
                   inplace=True)
    
    # ------------------------------------------
    # Write datasets to parquet files
    db_jobs.to_parquet(data_folder_path / "jobs.parquet")
    db_active_population.to_parquet(data_folder_path / "active_population.parquet")
    
    return db_jobs, db_active_population