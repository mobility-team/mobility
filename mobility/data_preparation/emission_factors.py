import os
import pandas as pd
import numpy as np
from pathlib import Path

data_folder_path = Path(os.path.dirname(__file__)).parent.parent / "data"

# -------------------------------------
# Load ADEME database
ademe = pd.read_csv(
    data_folder_path / "input/ademe/Base Carbone - données V18.1.csv",
    encoding="latin-1",
    sep=";",
    dtype=str,
    usecols=["Identifiant de l'élément", "Nom base français", "Nom attribut français", "Type Ligne", "Unité français", "Total poste non décomposé"]
)

ademe.columns = ["line_type", "ef_id", "name1", "name2", "unit", "value"]
ademe = ademe[ademe["line_type"] == "Elément"]
ademe["value"] = ademe["value"].str.replace(",", ".")
ademe["value"] = ademe["value"].astype(float)
ademe["name"] = np.where(ademe["name2"].isnull(), ademe["name1"], ademe["name1"] + " - " + ademe["name2"])

ademe = ademe[["ef_id", "value"]]
ademe.columns = ["ef_db_id", "value"]
ademe["database"] = "ademe"


# -------------------------------------
# Load the custom database
custom = pd.read_excel(
    data_folder_path / "input/mobility/emission_factors/custom.xlsx",
    dtype={"ef_id": str, "value": float}
)
custom = custom[["ef_id", "value"]]
custom.columns = ["ef_db_id", "value"]
custom["database"] = "custom"

# -------------------------------------
# Concatenate the dbs
ef_db = pd.concat([ademe, custom])


# -------------------------------------
# Load the mapping ef_name -> ef value
ef_mapping = pd.read_excel(data_folder_path / "input/mobility/emission_factors/ef_mapping.xlsx", dtype=str)



# -------------------------------------
# Load the mapping mode -> ef_name
mode_mapping = pd.read_excel(data_folder_path / "input/sdes/entd_2008/entd_mode.xlsx", dtype=str)


mode_ef = pd.merge(mode_mapping, ef_mapping, on="ef_name")
mode_ef = pd.merge(mode_ef, ef_db, on=["database", "ef_db_id"])

mode_ef = mode_ef[["mode_id", "value"]]
mode_ef.columns = ["mode_id", "ef"]

mode_ef.to_csv(data_folder_path / "input/mobility/emission_factors/mode_ef.csv", index=False)
