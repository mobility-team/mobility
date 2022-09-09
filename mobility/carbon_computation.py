import pandas as pd
import numpy as np
from pathlib import Path
import os


def carbon_computation(trips):
    
    # ---------------------------------------------
    # Retreive ADEME carbon factors     
    data_folder_path = Path(os.path.dirname(__file__)).parent / "data"
    ademe = pd.read_csv(data_folder_path / "ademe/Base_Carbone_V22.0.csv", error_bad_lines=False, 
                                encoding="latin-1",
                                sep=";",
                                dtype=str,
                                usecols=["Identifiant de l'élément", "Nom base français", "Nom attribut français", "Type Ligne", "Unité français", "Total poste non décomposé", "Code de la catégorie"]
                            )
    
    ademe = ademe.loc[ademe["Code de la catégorie"].str.contains("Transport de personnes")]
    ademe = ademe.drop(columns = "Code de la catégorie")
    ademe.columns = ["line_type", "ef_id", "name1", "name2", "unit", "value"]
    ademe = ademe[ademe["line_type"] == "Elément"]
    ademe["value"] = ademe["value"].str.replace(",", ".")
    ademe["value"] = ademe["value"].astype(float)
    ademe["name"] = np.where(ademe["name2"].isnull(), ademe["name1"], ademe["name1"] + " - " + ademe["name2"])
    ademe = ademe.rename(columns={"value":"ef"})
    ademe["database"] = "ademe"    
    
    # ---------------------------------------------
    # Link ENTD modes with ADEME carbon factors
    modes = pd.read_excel(data_folder_path / "surveys/entd-2008/entd_mode.xlsx", dtype=str)
    mapping = pd.DataFrame({
            "ef_name":["motorcycle",
                        "car",
                        "city_bus",
                        "shuttle_boat",
                        "long_distance_bus",
                        "tramway",
                        "subway",
                        "urban_train", 
                        "regional_train", 
                        "high_speed_train",
                        "long_distance_train", 
                        "airplane",
                        "cruise_ship"
                        ],
            "name":["Moto =< 250 cm3 - Mixte",
                    "Voiture - Motorisation moyenne",
                    "Autobus moyen - Agglomération de 100 000 à 250 000 habitants",
                    "Navette fluviale - Donnée 2009",
                    "Autocar - Gazole",
                    "Tramway - 2019",
                    "Métro - 2019",
                    "RER et transilien - 2019",
                    "TER - 2019",
                    "TGV - 2019",
                    "Train grandes lignes - 2019",
                    "Avion passagers - Moyen courrier, 2018",
                    "Ferry - de nuit (passagers)"
                    ],
            "ef_id":["27992",
                     "27970",
                     "21596",
                     "28259",
                     "28006",
                     "28148",
                     "28147",
                     "28149",
                     "28146",
                     "28145",
                     "28144",
                     "28132",
                     "21860"
                     ]
        })
    
    # Add custom factors
    custom =pd.DataFrame({"ef_name":["zero"],
                          "ef":[0],
                          "database":["custom"]
                          })
    
    factors = pd.merge(mapping[["ef_name", "ef_id"]], ademe[["ef_id", "ef", "database"]], how='left', on="ef_id")
    factors = pd.concat([factors.drop(columns="ef_id"), custom])
    
    # Final transportation carbon factors table
    mode_ef = pd.merge(modes, factors, on="ef_name", how="left")
    mode_ef = mode_ef[["mode_id", "ef", "database"]]
    
    

    # --------------------------------------------- 
    # Add carbon factors to each trip depending on transportation mode
    emissions = pd.merge(trips, mode_ef, on="mode_id", how="left")
    
    # ---------------------------------------------    
    # Car emission factors are corrected to account for other passengers
    # (no need to do this for the other modes, their emission factors are already in kgCO2e/passenger.km)
    k_ef_car = 1/(1 + emissions["n_other_passengers"])
    emissions["k_ef"] = np.where(emissions["mode_id"].str.slice(0, 1) == "3", k_ef_car, 1.0)
    
    # ---------------------------------------------
    # Compute GHG emissions [kgCO2e]
    emissions["carbon_emissions"] = emissions["ef"]*emissions["distance"]*emissions["k_ef"]
    emissions.drop(["k_ef", "ef"], axis=1, inplace=True)  
        
    return emissions