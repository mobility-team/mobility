import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import os
from pathlib import Path
import pyarrow.parquet as pq

from mobility.radiation_departments import *
from mobility.radiation_model import radiation_model

def school_map_model(dep, sources_territory):
    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "mobility/data/insee/schools"

   
   # Charger les dataframes Parquet
    db_schools = pq.read_table(data_folder_path / 'schools.parquet').to_pandas()
    db_schools_map = pq.read_table(data_folder_path / 'schools_map.parquet').to_pandas()
    # db_students = pq.read_table(data_folder_path / 'students.parquet').to_pandas()
    
    # Fait un LEFT JOIN sur le code RNE pour récupérer le code commune pour chaque établissement scolaire
    joined_schools_schoolsmap = pd.merge(db_schools_map, db_schools, how='left', on='Code_RNE')
    
    
    result = pd.merge(left=joined_schools_schoolsmap, right=sources_territory, 
                         left_on="code_insee", right_on="CODGEO", how="left")   
    
    
    # Sélectionner les colonnes utiles
    db_school_map_data = result[["code_insee", "CODGEO", "source_volume"]]
    # Restriction au département étudié
    db_school_map_dep = db_school_map_data[db_school_map_data['code_insee'].str.startswith(dep)]
    
    db_school_map_model = db_school_map_dep.groupby(["code_insee", "CODGEO"]).first()
    
    
    # Calculer le volume moyen par code_insee
    db_school_map_model['volume_moyen'] = db_school_map_model.groupby('code_insee')['source_volume'].transform('count')
    db_school_map_model['volume_moyen'] = db_school_map_model['source_volume'] / db_school_map_model['volume_moyen']

    db_school_map_model = db_school_map_model.drop('source_volume', axis=1)
    return db_school_map_model