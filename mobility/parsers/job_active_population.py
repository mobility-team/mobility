import pandas as pd
import os
from pathlib import Path
import requests
import zipfile


def prepare_job_active_population(proxies={}, test=False):
    """
    Downloads (if needed) the raw INSEE census data
    (hosted on data.gouv.fr by the mobility project at
    https://www.data.gouv.fr/fr/datasets/emploi-population-active-en-2019/),
    then creates one dataframe for the number of job per city
    and one for the active population per city
    and writes these into parquet files
    """

    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/work"

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "base-ccc-emploi-pop-active-2019.zip"

    if not test:
        if path.exists() is False:
            # Download the zip file
            print("Downloading from data.gouv.fr, it can take several minutes...")
            r = requests.get(
                url="https://www.data.gouv.fr/fr/datasets/r/02653cc4-76c0-4c3a-bc17-d5485c7ea2b9",
                proxies=proxies,
            )
            with open(path, "wb") as file:
                file.write(r.content)

            # Unzip the content
            with zipfile.ZipFile(path, "r") as zip_ref:
                zip_ref.extractall(data_folder_path)
        path_csv = data_folder_path / "base-cc-emploi-pop-active-2019.csv"
    else:
        print("Using a restricted dataset for test")
        path_csv = data_folder_path / "base-cc-emploi-pop-active-2019-90.csv"

    # Informations about jobs and active population for each city
    db_job_active_pop = pd.read_csv(
        path_csv,
        sep=";",
        usecols=[
            "CODGEO",
            "P19_ACT15P",
            "C19_ACT1564_CS1",
            "C19_ACT1564_CS2",
            "C19_ACT1564_CS3",
            "C19_ACT1564_CS4",
            "C19_ACT1564_CS5",
            "C19_ACT1564_CS6",
            "P19_EMPLT",
            "C19_EMPLT_CS1",
            "C19_EMPLT_CS2",
            "C19_EMPLT_CS3",
            "C19_EMPLT_CS4",
            "C19_EMPLT_CS5",
            "C19_EMPLT_CS6",
        ],
        dtype={"CODGEO": str},
    )

    db_jobs = db_job_active_pop.loc[
        :,
        [
            "CODGEO",
            "P19_EMPLT",
            "C19_EMPLT_CS1",
            "C19_EMPLT_CS2",
            "C19_EMPLT_CS3",
            "C19_EMPLT_CS4",
            "C19_EMPLT_CS5",
            "C19_EMPLT_CS6",
        ],
    ]
    db_jobs.set_index("CODGEO", inplace=True)
    db_jobs.rename(
        columns={
            "P19_EMPLT": "n_jobs_total",
            "C19_EMPLT_CS1": "n_jobs_CS1",
            "C19_EMPLT_CS2": "n_jobs_CS2",
            "C19_EMPLT_CS3": "n_jobs_CS3",
            "C19_EMPLT_CS4": "n_jobs_CS4",
            "C19_EMPLT_CS5": "n_jobs_CS5",
            "C19_EMPLT_CS6": "n_jobs_CS6",
        },
        inplace=True,
    )

    db_active_population = db_job_active_pop.loc[
        :,
        [
            "CODGEO",
            "P19_ACT15P",
            "C19_ACT1564_CS1",
            "C19_ACT1564_CS2",
            "C19_ACT1564_CS3",
            "C19_ACT1564_CS4",
            "C19_ACT1564_CS5",
            "C19_ACT1564_CS6",
        ],
    ]
    db_active_population.set_index("CODGEO", inplace=True)
    db_active_population.rename(
        columns={
            "P19_ACT15P": "active_pop",
            "C19_ACT1564_CS1": "active_pop_CS1",
            "C19_ACT1564_CS2": "active_pop_CS2",
            "C19_ACT1564_CS3": "active_pop_CS3",
            "C19_ACT1564_CS4": "active_pop_CS4",
            "C19_ACT1564_CS5": "active_pop_CS5",
            "C19_ACT1564_CS6": "active_pop_CS6",
        },
        inplace=True,
    )

    # ------------------------------------------
    # Write datasets to parquet files
    db_jobs.to_parquet(data_folder_path / "jobs.parquet")
    db_active_population.to_parquet(data_folder_path / "active_population.parquet")

    return db_jobs, db_active_population
