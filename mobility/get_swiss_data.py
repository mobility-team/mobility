import pandas as pd

import os
from pathlib import Path

SWISS_ACTIVITY_RATE = 0.8
# Source: https://www.bfs.admin.ch/bfs/fr/home/statistiques/travail-remuneration/
#         activite-professionnelle-temps-travail/population-active/personnes-actives-canton.html


def get_swiss_data(test=False):
    """
    Loads swiss data

    Returns
    -------
    dict:
        keys (list of str):
            ['jobs', 'active_population']
        values (list of pd.DataFrame):
            The corresponding dataframes wich have the following structure:
                Index:
                    Code (str): city geographic code
                Columns:
                    sink_volume (int): weight of the corresponding facilities
    """

    data_folder_path = Path(os.path.dirname(__file__)) / "data/CH"

    print("Getting local swiss data")

    # Load the dataframes into a dict
    swiss_data = {}

    jobs = pd.read_csv(data_folder_path / "CH-2021-emplois-communes.csv",
                       encoding='latin_1', sep=";")
    jobs[['Code', 'Commune']] = jobs.Commune.str.split(n=1, expand=True)

    active_population = pd.read_excel(
        data_folder_path / "CH-2022-population-communes.xlsx"
    )
    active_population[['Code', 'Commune']] = active_population.Territoire.str.split(n=1, expand=True)
    active_population['Code'] = active_population['Code'].str.slice(6)

    active_population['ACT'] = active_population['Total'] * SWISS_ACTIVITY_RATE

    swiss_data["jobs"] = jobs
    swiss_data["active_population"] = active_population

    return swiss_data
