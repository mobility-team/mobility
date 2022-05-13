import os
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path

data_folder_path = Path(os.path.dirname(__file__)).parent.parent / "data"

# ---------------------------------------------
# Number of clients (consumption units)
consumption_units = pd.read_csv(
    data_folder_path / "input/insee/recensement/FD_LOGEMT_2016.zip",
    encoding="latin-1",
    sep=";",
    dtype=str,
    usecols=["COMMUNE", "ARM", "INPER", "INP15M", "IPONDL"]
)

consumption_units.loc[consumption_units["COMMUNE"].isin(["75056", "13201", "69123"]), "COMMUNE"] = consumption_units.loc[consumption_units["COMMUNE"].isin(["75056", "13201", "69123"]), "ARM"]
consumption_units["IPONDL"] = consumption_units["IPONDL"].astype(float)
consumption_units["INPER"] = pd.to_numeric(consumption_units["INPER"], errors="coerce")
consumption_units["INP15M"] = pd.to_numeric(consumption_units["INP15M"], errors="coerce")
consumption_units["n_uc"] = 1 + 0.5*(consumption_units["INPER"] - 1 - consumption_units["INP15M"]) + 0.3*consumption_units["INP15M"]
consumption_units["n_uc"] *= consumption_units["IPONDL"]

n_uc_total = consumption_units["n_uc"].sum()

consumption_units = consumption_units.groupby("COMMUNE")["n_uc"].sum()
consumption_units.index.rename("location_id", inplace=True)
consumption_units.name = "m_i"

consumption_units.to_csv(data_folder_path / "input/mobility/shops/sources.csv")

# ---------------------------------------------
# Shops area > number of clients
shops = pd.read_csv(data_folder_path / "input/insee/bpe/bpe19_ensemble_xy.zip", sep=";", dtype=str)
shops.set_index("TYPEQU", inplace=True)

shops = shops[~shops["REG"].isin(["01", "02", "03", "04", "06"])].copy()
shops = shops.loc[["B101", "B102", "B201", "B202"]]

areas = pd.DataFrame({
    "TYPEQU": ["B101", "B102", "B201", "B202"],
    "m_j": [5000, 1500, 250, 50]
})

shops = pd.merge(shops, areas, on="TYPEQU")
shops = shops.groupby("DEPCOM")["m_j"].sum()
shops.index.rename("location_id", inplace=True)

# Number of consumption units per city
print(n_uc_total/shops.sum())
shops = shops*n_uc_total/shops.sum()

shops.to_csv(data_folder_path / "input/mobility/shops/sinks.csv")

