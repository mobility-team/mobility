import pandas as pd
import numpy as np
import os
from pathlib import Path
import requests
import zipfile

def prepare_facilities(proxies={}):
    """
    Downloads (if needed) the raw data from the INSEE Permanent database of facilities
    (https://www.insee.fr/fr/statistiques/3568638?sommaire=3568656),
    then creates one dataframe for 
        the malls
        the shops
        the schools
        the administration facilities
        the sport facilities
        the care facilities
        the show facilities
        the museum
        the restaurants
    and computes the corresponding weight in each city for each of these facilities
    
    Each dataframe has the following strcucture:
        Index:
            DEPCOM (str): geographic code of the city
        Columns:
            m_j (int): weight of the city in terms of the appropriate facilities
    
    and writes these into parquet files
    """
    
    data_folder_path = Path(os.path.dirname(__file__)).parents[0] / "data/insee/facilities"
    
    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)
    
    # Download the raw survey data from insee.fr if needed
    path = data_folder_path / "bpe20_ensemble_csv.zip"
    
    if path.exists() is False:
        # Download the zip file
        r = requests.get(
            url="https://www.insee.fr/fr/statistiques/fichier/3568629/bpe20_ensemble_csv.zip",
            proxies=proxies
        )
        with open(path, "wb") as file:
            file.write(r.content)
        
        # Unzip the content
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)
    
    # Informations about jobs and active population for each city
    db_facilities = pd.read_csv(data_folder_path / "bpe20_ensemble.csv",
                                    sep=';', usecols=['DEPCOM', 'TYPEQU', 'NB_EQUIP'],
                                    dtype={'DEPCOM': str})
    
    # Informations about the facilities (area, assumpted frequentation)
    facilities_features = pd.read_excel(data_folder_path.parent / "esane/equipments_features.xlsx",
                                 usecols=['insee_id', 'shop_area', 'frequentation', 'motive_id'],
                                 dtype={'motive_id': str})
    
    # Informations about the turnover of shop facilities
    facilities_turnover = pd.read_excel(data_folder_path.parent / "esane/shops_turnover.xlsx",
                                 usecols=['naf_id', 'turnover_psqm', 'turnover_per_shop'],
                                 dtype={'naf_id': str})
    
    # Table to convert from INSEE terminology to NAF terminology
    insee_to_naf = pd.read_excel(data_folder_path.parent / "esane/equipments_insee_to_naf.xlsx",
                                 usecols=['insee_id', 'naf_id'],
                                 dtype=str)
    
    # The turnover is used to determine the weights to attribute to the different shops
    facilities_turnover = pd.merge(facilities_turnover, insee_to_naf, on='naf_id')
    facilities_features = pd.merge(facilities_turnover, facilities_features, on='insee_id', how='right')
    
    # If the area is given, use the turnover per square meter
    mask = facilities_features['shop_area'].notna()
    facilities_features.loc[mask, 'weight'] = facilities_features.loc[mask, 'shop_area']*facilities_features.loc[mask, 'turnover_psqm']
    # If not, use the turnover per shop
    mask = facilities_features['turnover_per_shop'].notna()
    facilities_features.loc[mask, 'weight'] = facilities_features.loc[mask, 'turnover_per_shop']
    # In the other cases, use the frequentation
    mask = facilities_features['weight'].isna()
    facilities_features.loc[mask, 'weight'] = facilities_features.loc[mask, 'frequentation']
    
    facilities_features.rename(columns={'insee_id': 'TYPEQU'}, inplace=True)
    facilities_features.drop(columns=['shop_area', 'naf_id', 'turnover_psqm', 'turnover_per_shop',
                                      'frequentation'],
                             inplace=True)
    
    # Create one dataframe for each motive
    motives = facilities_features['motive_id'].unique()
    motive_db = {}
    for mot in motives :
        # get the facilities corresponding to the motive
        motive_facilities = facilities_features.loc[facilities_features['motive_id']==mot]
                
        motive_facilities = pd.merge(db_facilities, motive_facilities, on='TYPEQU', how='inner')

        motive_facilities['m_j'] = motive_facilities['weight']*motive_facilities['NB_EQUIP']
        motive_facilities = motive_facilities.drop(columns=['weight', 'TYPEQU', 'NB_EQUIP'])
        motive_facilities = motive_facilities.groupby('DEPCOM').sum()
        
        motive_db[mot] = motive_facilities
    
    db_mall = motive_db['2.1']
    db_shops = motive_db['2.2']
    db_schools = motive_db['1.4']
    db_admin = motive_db['4.1']
    db_sport = motive_db['7.6']
    db_care = motive_db['3.1']
    db_show = motive_db['7.5']
    db_museum = motive_db['7.4']
    db_restaurant = motive_db['7.3']
    
    # ------------------------------------------
    # Write datasets to parquet files
    db_mall.to_parquet(data_folder_path / "malls.parquet")
    db_shops.to_parquet(data_folder_path / "shops.parquet")
    db_schools.to_parquet(data_folder_path / "schools.parquet")
    db_admin.to_parquet(data_folder_path / "admin_facilities.parquet")
    db_sport.to_parquet(data_folder_path / "sport_facilities.parquet")
    db_care.to_parquet(data_folder_path / "care_facilities.parquet")
    db_show.to_parquet(data_folder_path / "show_facilities.parquet")
    db_museum.to_parquet(data_folder_path / "museum.parquet")
    db_restaurant.to_parquet(data_folder_path / "restaurants.parquet")
    
    return None