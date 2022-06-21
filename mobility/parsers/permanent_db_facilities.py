import pandas as pd
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
        
    def prepare_data_base(db_facilities, list_type_facilities, weight_type_facilities):
        """
        Filter the facilities database with the list of type facilities
        and assign a weight to each city depending on the number of facilities
        and the corresponding weights
        
        Args:
            db_facilites (pd.DataFrame): the dataframe from the INSEE Permanent base of facilities
                Columns:
                    DEPCOM (str): geographic code of the city
                    NB_EQUIP (int): number of facilities in the city
                    TYPEQU (str): type of facility
            list_type_facilities (list of string): the list of type of facilities
            weight_type_facilities (list of int): the corresponding weights
                (the length must be equal to the length of list_type_facilities)
        
        Returns:
            pd.DataFrame: a dataframe with one row per city with a weight corresponding
                          to the facilities from list_type_facilities that are in the city
                          Index:
                              DEPCOM (str): geographic code of the city
                          Columns:
                              m_j (int): the city's weight corresponding to the facilites
        """
        
        # Compute the facilities to select
        mask = db_facilities['TYPEQU'].apply(lambda x: x in list_type_facilities)
        db = db_facilities.loc[mask]
        
        weights = pd.DataFrame({'TYPEQU': list_type_facilities,
                                'm_j': weight_type_facilities
                                })
        db = pd.merge(db, weights, on='TYPEQU')
        db['m_j'] = db['m_j']*db['NB_EQUIP']
        db = db.drop(columns=['TYPEQU', 'NB_EQUIP'])
                
        db = db.groupby('DEPCOM').sum()
        
        return db

    
    # !!! voir quelle type d'équipement pour quels motifs !!!
    
    # Motif Achats Grande surface // petit commerce
    lst_mall_facilities = ['B101', 'B102', 'B103']
    weight_mall_facilities = [5000, 1500, 500]
    
    lst_shop_facilities = ['B201', 'B202', 'B203', 'B204', 'B205', 'B206',
                           'B301', 'B310', 'B311', 'B312']
    weight_shop_facilities = [200, 50, 300, 50, 50, 50,
                              50, 50, 50, 50]
    
    db_mall = prepare_data_base(db_facilities, lst_mall_facilities, weight_mall_facilities)
    db_shops = prepare_data_base(db_facilities, lst_shop_facilities, weight_shop_facilities)

    # Motif Etude
    list_school_facilities = ['C101', 'C102', 'C104', 'C105', 
                              'C201',
                              'C301', 'C302', 'C303', 'C304', 'C305',
                              'C401', 'C402', 'C403', 'C409',
                              'C501', 'C502', 'C503', 'C504', 'C505', 'C509',
                              'C601', 'C602', 'C603', 'C604', 'C605', 'C609']
    weight_school_facilities = [60, 20, 200, 70,
                                600,
                                500, 200, 200, 50, 50,
                                200, 200, 200, 50,
                                50, 500, 200, 50, 50, 50,
                                50, 50, 50, 50, 50, 50]
    db_schools = prepare_data_base(db_facilities, list_school_facilities, weight_school_facilities)
    
    # Motif Démarche administratives
    # équipements sportifs et de loisirs
    list_admin_facilities = ['A101', 'A104', 'A105', 'A106', 'A107', 'A108', 'A109',
                             'A120', 'A121', 'A122', 'A123', 'A124', 'A125', 'A126', 'A127',
                             'A206', 'A207', 'A208']
    weight_admin_facilities = [50]*len(list_admin_facilities)
    db_admin = prepare_data_base(db_facilities, list_admin_facilities, weight_admin_facilities)
    
    # Motif Faire du sport
    list_sport_facilities = ['F101', 'F102', 'F103', 'F104', 'F105', 'F106', 'F107', 
                             'F108', 'F109', 'F110', 'F111', 'F112', 'F113', 'F114',
                             'F115', 'F116', 'F117', 'F118', 'F119', 'F120', 'F121',
                             'F201', 'F202', 'F203']
    weight_sport_facilities = [50]*len(list_sport_facilities)
    db_sport = prepare_data_base(db_facilities, list_sport_facilities, weight_sport_facilities)
    
    # Motif Soins
    list_care_facilities = ['D101', 'D102', 'D103', 'D104', 'D105', 'D106', 'D107', 
                            'D108', 'D109', 'D110', 'D111', 'D112', 'D113',
                            'D201', 'D202', 'D203', 'D206', 'D207', 'D208', 'D209',
                            'D210', 'D211', 'D212', 'D213', 'D214', 'D221', 'D231',
                            'D232', 'D233', 'D235', 'D236', 'D237', 'D238', 'D239',
                            'D241', 'D242', 'D243'
                            'D302', 'D303', 'D304', 'D305', 'D307']
    weight_care_facilities = [20]*len(list_care_facilities)
    db_care = prepare_data_base(db_facilities, list_care_facilities, weight_care_facilities)
     
    # Motif Spectacle culturel ou sportif, conférence
    list_show_facilities = ['F303', 'F305', 'F306', 'F308']
    weight_show_facilities = [100]*len(list_show_facilities)
    db_show = prepare_data_base(db_facilities, list_show_facilities, weight_show_facilities)
    
    # Motif Visiter un monument ou un site historique
    # Lieu d'exposition et patrimoine
    list_museum_facilities = ['F309']
    weight_museum_facilities = [200]*len(list_museum_facilities)
    db_museum = prepare_data_base(db_facilities, list_museum_facilities, weight_museum_facilities)
    
    # Motif Manger ou boire à l'extérieur du domicile
    list_restaurant_facilities = ['A504']
    weight_restaurant_facilities = [25]*len(list_restaurant_facilities)
    db_restaurant = prepare_data_base(
        db_facilities, list_restaurant_facilities, weight_restaurant_facilities)
    
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
    
    return db_facilities, db_mall, db_shops, db_schools, db_admin, db_sport, db_care, db_show, db_museum, db_restaurant

db_facilities, db_mall, db_shops, db_schools, db_admin, db_sport, db_care, db_show, db_museum, db_restaurant = prepare_facilities()

