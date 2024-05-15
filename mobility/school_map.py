# -*- coding: utf-8 -*-
"""
Created on Sat May  4 23:34:04 2024

@author: Hidekela
"""

from mobility.get_insee_data import get_insee_data
from mobility.parsers import download_work_home_flows,prepare_school_VT

import numpy as np
import pandas as pd
import os
from pathlib import Path


COMMUNES_COORDINATES_CSV = "donneesCommunesFrance.csv"
COMMUNES_SURFACES_CSV = "donneesCommunesFrance.csv"
work_home_fluxes_csv= download_work_home_flows()
school_home_fluxes_csv=prepare_school_VT()




def school_map_model(sources_territory, sinks_territory):
    
    
    # On récupère la quantité d'élèves qui habitent dans chaque ville.
    db_school_map_model = pd.merge(left=sinks_territory, right=sources_territory, 
                          left_on="code_insee", right_on="CODGEO", how="left")   
    
    
    # Sélectionner les colonnes utiles
    db_school_map_model = db_school_map_model[["code_insee", "CODGEO", "source_volume"]]
    # Supprime les doublons
    db_school_map_model = db_school_map_model.groupby(["code_insee", "CODGEO"]).first()
    
    
    # Calculer le volume moyen par code_insee
    db_school_map_model['volume_moyen'] = db_school_map_model.groupby('code_insee')['source_volume'].transform('count')
    db_school_map_model['volume_moyen'] = db_school_map_model['source_volume'] / db_school_map_model['volume_moyen']

    db_school_map_model = db_school_map_model.drop('source_volume', axis=1)
    db_school_map_model = db_school_map_model.rename_axis(['from', 'to'], axis='index')
    db_school_map_model = db_school_map_model.rename(columns={'volume_moyen': 'flow_volume'})
    db_school_map_model.dropna(subset=['flow_volume'], inplace=True)
    return db_school_map_model




def get_data_for_model_school_map(
    lst_departments,
    Age,
    communes_coordinates_csv=COMMUNES_COORDINATES_CSV,
    communes_surfaces_csv=COMMUNES_SURFACES_CSV,
    school_home_fluxes_csv=school_home_fluxes_csv,
    test=False,
):
   
    # ===================
    # IMPORT AND PROCESS THE DATA

    # Import the data (active population and jobs)
    insee_data = get_insee_data(test=test)
    db_students = insee_data["students"]
    db_schools = insee_data["schools"]
    db_school_map = insee_data["schools_map"]
    
    
    # Prepare sources_territory
    db_students_ = db_students[db_students['TrancheAge'] == Age].drop(columns=['TrancheAge'])
    
    # Only keep the sinks in the chosen departements
    sources_territory = db_students_.loc[:, ["CODGEO", "Nombre"]]
    sources_territory["DEP"] = sources_territory["CODGEO"].str.slice(0, 2)
    mask = sources_territory["DEP"].apply(lambda x: x in lst_departments)
    sources_territory = sources_territory.loc[mask]

    sources_territory = sources_territory.set_index("CODGEO")
    sources_territory = sources_territory.drop(columns=["DEP"])
    sources_territory.rename(columns={"Nombre": "source_volume"}, inplace=True)
    
    
    # Schools
    db_schools_ = db_schools[db_schools['Type_etablissement'] == Age].drop(columns=['Type_etablissement'])

    # Only keep the sinks in the chosen departements
    school = db_schools_.loc[:, ["CODGEO", "Code_RNE","Nombre_d_eleves"]]
    school.rename(columns={'Nombre_d_eleves': 'sink_volume'}, inplace=True)
    mask = school['CODGEO'].str.startswith(tuple(lst_departments))
    school = school.loc[mask]
    
    # School_map
    school_map = db_school_map.loc[:, ["code_insee", "Code_RNE"]]
    mask = school_map['code_insee'].str.startswith(tuple(lst_departments))
    school_map = school_map.loc[mask]



    # Prepare sinks_territory

    # Fait un LEFT JOIN sur le code RNE pour récupérer le code commune pour chaque établissement scolaire
    sinks_territory = pd.merge(school_map, school, how='left', on='Code_RNE')
    sinks_territory.dropna(subset=['CODGEO'], inplace=True)
    
    
    
    data_folder_path = Path(os.path.dirname(__file__)).joinpath("data").joinpath("insee").joinpath("territories")    # raw_flowDT = school_home_fluxes_csv
    raw_flowDT = school_home_fluxes_csv

    raw_flowDT["DEP"] = raw_flowDT["COMMUNE"].str.slice(0, 2)
    raw_flowDT["DEP2"] = raw_flowDT["DCLT"].str.slice(0, 2)
    mask = raw_flowDT["DEP"].apply(lambda x: x in lst_departments)
    mask2 = raw_flowDT["DEP2"].apply(lambda x: x in lst_departments)
    raw_flowDT = raw_flowDT.loc[mask]
    raw_flowDT = raw_flowDT.loc[mask2]
    

    raw_flowDT = raw_flowDT.loc[raw_flowDT['Tranche_Age'] == Age]
    print("raw_flowDT1")

    # Import the geographic data on the work-home mobility on Millau

    coordinates = pd.read_csv(
        data_folder_path / communes_coordinates_csv,
        sep=",",
        usecols=["NOM_COM", "INSEE_COM", "x", "y"],
        dtype={"INSEE_COM": str},
    )
    coordinates.set_index("INSEE_COM", inplace=True)
    # The multiplication by 1000 is only for visualization purposes
    coordinates["x"] = coordinates["x"] * 1000
    coordinates["y"] = coordinates["y"] * 1000

    surfaces = pd.read_csv(
        data_folder_path / communes_surfaces_csv,
        sep=",",
        usecols=["INSEE_COM", "distance_interne"],
        dtype={"INSEE_COM": str},
    )
    surfaces.set_index("INSEE_COM", inplace=True)

    # Compute the distance between cities
    #    distance between i and j = (x_i - x_j)**2 + (y_i - y_j)**2
    lst_communes = sources_territory.index.to_numpy()
    idx_from_to = np.array(np.meshgrid(lst_communes, lst_communes)).T.reshape(-1, 2)
    idx_from = idx_from_to[:, 0]
    idx_to = idx_from_to[:, 1]
    costs_territory = pd.DataFrame(
        {"from": idx_from, "to": idx_to, "cost": np.zeros(idx_to.shape[0])}
    )
    costs_territory = pd.merge(
        costs_territory, coordinates, left_on="from", right_index=True
    )
    costs_territory.rename(columns={"x": "from_x", "y": "from_y"}, inplace=True)
    costs_territory = pd.merge(
        costs_territory, coordinates, left_on="to", right_index=True
    )
    costs_territory.rename(columns={"x": "to_x", "y": "to_y"}, inplace=True)

    costs_territory = pd.merge(
        costs_territory, surfaces, left_on="from", right_index=True
    )

    costs_territory["cost"] = np.sqrt(
        (costs_territory["from_x"] / 1000 - costs_territory["to_x"] / 1000) ** 2
        + (costs_territory["from_y"] / 1000 - costs_territory["to_y"] / 1000) ** 2
    )

    # distance if the origin and the destination is the same city
    # is internal distance = 128*r / 45*pi
    # where r = sqrt(surface of the city)/pi
    mask = costs_territory["from"] != costs_territory["to"]
    costs_territory["cost"].where(
        mask, other=costs_territory["distance_interne"], inplace=True
    )
    
    return (
        sources_territory,
        sinks_territory,
        costs_territory,
        coordinates,
        raw_flowDT,
    )