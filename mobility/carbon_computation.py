import pandas as pd
import numpy as np
from pathlib import Path
import os


def get_ademe_factors(file_path):
    """
    Get ADEME carbon factors from the last database

    Parameters
    ----------
    file_path : WindowsPath (pathlib module)
        path to the last ADEME carbon factors database

    Returns
    -------
    ademe : DataFrame
        ADEME carbon factors database

    """
    ademe = pd.read_csv(
        file_path,
        error_bad_lines=False,
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=[
            "Identifiant de l'élément",
            "Nom base français",
            "Nom attribut français",
            "Type Ligne",
            "Unité français",
            "Total poste non décomposé",
            "Code de la catégorie",
        ],
    )

    ademe = ademe.loc[
        ademe["Code de la catégorie"].str.contains("Transport de personnes")
    ]
    ademe = ademe.drop(columns="Code de la catégorie")
    ademe.columns = ["line_type", "ef_id", "name1", "name2", "unit", "value"]
    ademe = ademe[ademe["line_type"] == "Elément"]
    ademe["value"] = ademe["value"].str.replace(",", ".")
    ademe["value"] = ademe["value"].astype(float)
    ademe["name"] = np.where(
        ademe["name2"].isnull(), ademe["name1"], ademe["name1"] + " - " + ademe["name2"]
    )
    ademe = ademe.rename(columns={"value": "ef"})
    ademe["database"] = "ademe"
    return ademe


def carbon_computation(trips, ademe_database="Base_Carbone_V22.0.csv"):
    """


    Parameters
    ----------
    trips : DataFrame
        Trips of one or several individuals (purpose, mode, distance)
    ademe_database : str, optional
        ADEME database file name.
        The default is "Base_Carbone_V22.0.csv".
        Check if an update of the database has been uploaded on https://bilans-ges.ademe.fr/fr/accueil/contenu/index/page/telecharger_donnees/siGras/0

    Returns
    -------
    emissions : DataFrame
        trips with carbon emissions

    """

    # ---------------------------------------------
    # CREATE FACTORS DATABASE
    # ---------------------------------------------

    data_folder_path = Path(os.path.dirname(__file__)) / "data"

    # ENTD modes and carbon factors types
    modes = pd.read_excel(data_folder_path / "surveys/entd_mode.xlsx", dtype=str)

    # Retreive ADEME carbon factors
    ademe_file_path = data_folder_path / "ademe/" / ademe_database
    ademe = get_ademe_factors(ademe_file_path)

    # Link ENTD modes with ADEME carbon factors
    mapping_file = "mapping.csv"
    mapping_file_path = data_folder_path / "ademe/" / mapping_file
    mapping = pd.read_csv(mapping_file_path, encoding="latin1", dtype=str)

    # Add custom factors
    custom = pd.DataFrame({"ef_name": ["zero"], "ef": [0], "database": ["custom"]})

    factors = pd.merge(
        mapping[["ef_name", "ef_id"]],
        ademe[["ef_id", "ef", "unit", "database"]],
        how="left",
        on="ef_id",
    )
    factors = pd.concat([factors.drop(columns="ef_id"), custom])

    # Final transportation carbon factors table
    mode_ef = pd.merge(modes, factors, on="ef_name", how="left")
    mode_ef = mode_ef[["mode_id", "ef", "database"]]

    # ---------------------------------------------
    # COMPUTE CARBON EMISSIONS
    # ---------------------------------------------

    # Add carbon factors to each trip depending on transportation mode
    emissions = pd.merge(trips, mode_ef, on="mode_id", how="left")

    # Car emission factors are corrected to account for other passengers
    # (no need to do this for the other modes, their emission factors are already in kgCO2e/passenger.km)
    k_ef_car = 1 / (1 + emissions["n_other_passengers"])
    emissions["k_ef"] = np.where(
        emissions["mode_id"].str.slice(0, 1) == "3", k_ef_car, 1.0
    )

    # Compute GHG emissions [kgCO2e]
    emissions["carbon_emissions"] = (
        emissions["ef"] * emissions["distance"] * emissions["k_ef"]
    )
    # emissions.drop(["k_ef", "ef"], axis=1, inplace=True)

    return emissions
