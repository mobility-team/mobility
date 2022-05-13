import os
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path

data_folder_path = Path(os.path.dirname(__file__)).parent.parent / "data"

# ---------------------------------------------
# Cities coordinates
cities = gpd.read_file(data_folder_path / "input/arcep/COMMUNE.shp")
cities = cities.to_crs("EPSG:2154")

cities["x"] = np.round(cities.centroid.x)
cities["y"] = np.round(cities.centroid.y)

# From https://math.stackexchange.com/questions/135766/average-distance-between-two-points-in-a-circular-disk
cities["r"] = np.round(np.sqrt(cities.area/np.pi))
cities["d_internal"] = np.round(128*cities["r"]/45/np.pi)

cities = cities[["INSEE_COM", "x", "y", "r", "d_internal"]]
cities.set_index("INSEE_COM", inplace=True)
cities.index.rename("location_id", inplace=True)

cities.to_csv(data_folder_path / "input/elioth/work_home/locations.csv")


# ---------------------------------------------
# Number of employees in each city
n_employees = pd.read_csv(
    data_folder_path / "input/insee/recensement/BTT_TD_ACT4_2016.zip",
    encoding="latin-1",
    sep=";",
    dtype=str
)

# Format
n_employees["NB"] = n_employees["NB"].astype(float)
n_employees.rename({"CS1_6": "CS1"}, axis=1, inplace=True)

# Add the NA5 level job sector
na5_na17 = pd.read_csv(data_folder_path / "input/insee/na5_na17.csv")
n_employees = pd.merge(n_employees, na5_na17, on="NA17")

# Group by city, job sector and socio-professional category
n_employees = n_employees.groupby(["NA5", "CS1", "CODGEO"])["NB"].sum()
n_employees.name = "m_i"

n_employees.index.rename(["NA5", "CS1", "location_id"], inplace=True)

n_employees = n_employees[n_employees > 0]

n_employees.to_csv(data_folder_path / "input/elioth/work_home/sources.csv")


# ---------------------------------------------
# Number of jobs in each city
n_jobs = pd.read_csv(
    data_folder_path / "input/insee/recensement/BTT_TD_EMP3_2016.zip",
    encoding="latin-1",
    sep=";",
    dtype=str
)

# Format
n_jobs["NB"] = n_jobs["NB"].astype(float)
n_jobs["CS1"] = n_jobs["CS3_29"].str.slice(0, 1)

# Group by city, job sector and socio-professional category
n_jobs = n_jobs.groupby(["NA5", "CS1", "CODGEO"])["NB"].sum()
n_jobs.name = "m_j"

n_jobs.index.rename(["NA5", "CS1", "location_id"], inplace=True)

n_jobs = n_jobs[n_jobs > 0]

n_jobs.to_csv(data_folder_path / "input/elioth/work_home/destinations.csv")
