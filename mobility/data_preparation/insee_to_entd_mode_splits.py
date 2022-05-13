import sys
sys.path.append(".")

import pandas as pd
import numpy as np
from matplotlib import pyplot

import os
from pathlib import Path

data_folder_path = Path(os.path.dirname(__file__)).parent.parent / "data"


# Info about households
hh = pd.read_csv(
    data_folder_path / "input/sdes/entd_2008/Q_tcm_menage_0.csv",
    encoding="latin-1",
    sep=";",
    dtype=str,
    usecols=["idENT_MEN", "numcom_UU2010", "NPERS", "CS24PR"]
)
hh.columns = ["IDENT_MEN", "cs1", "n_pers", "city_category"]

hh["cs1"] = hh["cs1"].str.slice(0, 1)
hh["cs1_ref_pers"] = hh["cs1"]
hh["n_pers"] = hh["n_pers"].astype(int)


# ------------------------------------------
# Trips dataset
df = pd.read_csv(
    data_folder_path / "input/sdes/entd_2008/K_deploc.csv",
    encoding="latin-1",
    sep=";",
    dtype=str,
    usecols=["IDENT_IND", "PONDKI",
             "V2_TYPJOUR", "V2_DLOCAL",
             "V2_MMOTIFDES", "V2_MMOTIFORI",
             "V2_MDISTTOT",
             "V2_MTP",
             "V2_MACCOMPM", "V2_MACCOMPHM"]
)

df["V2_MDISTTOT"] = df["V2_MDISTTOT"].astype(float)
df["PONDKI"] = df["PONDKI"].astype(float)
df["n_trip_companions"] = df["V2_MACCOMPM"].astype(int) + df["V2_MACCOMPHM"].astype(int)

df["weekday"] = np.where(df["V2_TYPJOUR"] == "1", True, False)

# Remove long distance trips (> 80 km from home)
df = df[df["V2_DLOCAL"] == "1"]

# Remove trips with an unknown or zero distance
df = df[(df["V2_MDISTTOT"] > 0.0) | (~df["V2_MDISTTOT"].isnull())]