import pandas as pd
import numpy as np

import os
from pathlib import Path
import sqlite3

data_folder_path = Path(os.path.dirname(__file__)).parent.parent / "data"

# ---------------------------------------------
# Mode share for each origin - destination pair in the work-home trips dataset
with open(data_folder_path / "input/insee/recensement/RP2016_MOBPRO.parquet", "rb") as f:
    mob = pd.read_parquet(f, columns=["COMMUNE", "ARM", "DCLT", "TRANS", "VOIT", "IPONDI"])

mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "COMMUNE"] = mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "ARM"]
mob["IPONDI"] = mob["IPONDI"].astype(float)

mob["VOIT"] = pd.to_numeric(mob["VOIT"], downcast="integer", errors="coerce")

mob["has_cars"] = False
mob.loc[mob["VOIT"] > 0, "has_cars"] = True

mob = mob.groupby(["COMMUNE", "DCLT", "has_cars", "TRANS"])["IPONDI"].sum()

mob_tot = mob.groupby(["COMMUNE", "DCLT", "has_cars"]).sum()

p_mode = mob/mob_tot

p_mode.name = "p_mode"
p_mode.index.rename(["from", "to", "has_cars", "mode"], inplace=True)

# -------------------------------------------
# Compute the distances between locations
locations = pd.read_csv(
    data_folder_path / "input/mobility/work_home/locations.csv",
    dtype={"location_id": str}
)

p_mode_d = pd.merge(p_mode.reset_index(), locations, left_on="from", right_on="location_id")
p_mode_d = pd.merge(p_mode_d.reset_index(), locations, left_on="to", right_on="location_id")

p_mode_d["dist"] = np.sqrt(np.power(p_mode_d["x_x"] - p_mode_d["x_y"], 2) + np.power(p_mode_d["y_x"] - p_mode_d["y_y"], 2))
                
p_mode_d["dist"] = p_mode_d["r_x"]/3 + p_mode_d["dist"] + p_mode_d["r_y"]/3
                
# Apply a correction factor
# from the article "From crow-fly distances to real distances, or the origin of detours, Heran"
p_mode_d["dist"] = p_mode_d["dist"]*(1.1+0.3*np.exp(-p_mode_d["dist"]/20))
        
# If origin = destination
# the trip length is the expected distance between two points on the city's disc
p_mode_d.loc[p_mode_d["from"] == p_mode_d["to"], "dist"] = p_mode_d.loc[p_mode_d["from"] == p_mode_d["to"], "d_internal_x"]
p_mode_d.loc[p_mode_d["from"] == p_mode_d["to"], "dist"] = p_mode_d.loc[p_mode_d["from"] == p_mode_d["to"], "dist"]*(1.1+0.3*np.exp(-p_mode_d.loc[p_mode_d["from"] == p_mode_d["to"], "dist"]/20))
        
# Meters to kilometers
p_mode_d["dist"] /= 1000.0

p_mode_d = p_mode_d[["from", "to", "has_cars", "mode", "p_mode", "dist"]]



# -------------------------------------
# Add city category
cities_category = pd.read_csv(data_folder_path / "input/insee/cities_category.csv", dtype=str)
        
supp = pd.DataFrame({
    "codgeo": [str(75100 + i) for i in range(1, 21)] + [str(69380 + i) for i in range(1, 9)] + [str(13000 + i) for i in range(1, 16)],
    "city_category": "C"
})

cities_category = cities_category.append(supp)

p_mode_d = pd.merge(p_mode_d, cities_category, left_on="from", right_on="codgeo")


# -------------------------------------
# Map INSEE modes to ENTD modes
insee_to_entd = pd.read_excel(data_folder_path / "input/insee/recensement/insee_modes_to_entd_modes.xlsx", dtype=str)
insee_to_entd = insee_to_entd[["insee_mode_id", "entd_mode_id"]]


p_mode_d = pd.merge(p_mode_d, insee_to_entd, left_on='mode', right_on="insee_mode_id")


# ----------------------------------------
# Detail by distance class
with open(data_folder_path / "input/sdes/entd_2008/insee_modes_to_entd_modes.parquet", "rb") as f:
    p_mode_detail = pd.read_parquet(f)

bins_limits = p_mode_detail.groupby(["city_category", "dist_bin_left", "dist_bin_right"])["pondki"].count()
bins_limits = bins_limits.index.to_frame().melt(id_vars="city_category")
bins_limits = bins_limits.groupby("city_category")["value"].unique()
bins_limits = bins_limits[0]

p_mode_d["dist_bin"] = pd.cut(p_mode_d["dist"], bins_limits)
p_mode_d["dist_bin_left"] = p_mode_d["dist_bin"].apply(lambda x: x.left)
p_mode_d["dist_bin_right"] = p_mode_d["dist_bin"].apply(lambda x: x.right)

p_mode_detail.reset_index(inplace=True)
p_mode_detail.rename({"pondki": "p_mode_det"}, inplace=True, axis=1)


p_mode_d = pd.merge(
    p_mode_d,
    p_mode_detail,
    left_on=["city_category", "dist_bin_left", "dist_bin_right", "entd_mode_id"],
    right_on=["city_category", "dist_bin_left", "dist_bin_right", "mode_id"],
    how="left"
)


# Compute the probability of the detailed modes
# P(mode, detailed_mode) = P(detailed_mode|mode)*P(mode)
p_mode_d["p_mode_det"].fillna(0.0, inplace=True)

p_mode_d["p"] = p_mode_d["p_mode"]*p_mode_d["p_mode_det"]

p_mode_d.set_index(["from", "to", "has_cars", "mode"], inplace=True)

p_mode_d = pd.merge(p_mode_d, p_mode_d.groupby(["from", "to", "has_cars", "mode"])["p"].sum(), left_index=True, right_index=True)
p_mode_d["p"] = p_mode_d["p_x"]/p_mode_d["p_y"]

p_mode_d["p"] = p_mode_d["p"]*p_mode_d["p_mode"]

p_mode_d["p"].fillna(p_mode_d["p_mode"], inplace=True)

# Format the resulting dataframe
p_mode_d = p_mode_d[["entd_mode_id", "p"]]
p_mode_d.reset_index(inplace=True)
p_mode_d.drop("mode", axis=1, inplace=True)
p_mode_d.rename({"entd_mode_id": "mode_id", "p": "p_mode"}, axis=1, inplace=True)
p_mode_d.set_index(["from", "to", "has_cars", "mode_id"], inplace=True)

# Remove small probabilities
p_mode_d = p_mode_d[p_mode_d["p_mode"] > 0.01]
p_mode_d["p_mode"] = p_mode_d["p_mode"]/p_mode_d.groupby(["from", "to", "has_cars"])["p_mode"].sum()

# Check if the dataframe is OK
x = p_mode_d.xs("75116").xs("78297").xs(False)
x.sort_values("p_mode")
x["p_mode"].sum()

x = p_mode_d.xs("75116").xs("78297").xs(True)
x.sort_values("p_mode")

p_mode_d.to_parquet(data_folder_path / "input/mobility/modes/modes_probability.parquet")

