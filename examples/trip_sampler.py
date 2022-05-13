import sys
sys.path.append("..")
sys.path.append(".")

import os
import time
from pathlib import Path
import multiprocessing as mp

import pandas as pd
import numpy as np
from mobility import TripSampler, read_parquet
# from matplotlib import pyplot

import plotly.express as px
import plotly.io as pio
pio.renderers.default = 'svg'
pio.orca.config.executable = "C:\\ProgramData\\Anaconda3\\envs\\mobility\\orca_app\\orca.exe"

data_folder_path = Path(os.path.dirname(__file__)).parent / "data"


# Test the sampler for some persons
# (city, economic sector, csp of the ref person of the household,
# csp of the person, number of persons in the household)
persons = []


for i in range(10):
    persons.append({"id": i,
     "na5": "GU",
     "csp_ref_pers": "5",
     "csp": "5",
     "n_pers": "3+",
    })
    

trip_sampler = TripSampler("13202")


all_trips = []
for person in persons:
    
    trips = trip_sampler.get_trips(
        person_id=person["id"],
        na5=person["na5"],
        csp_ref_pers=person["csp_ref_pers"],
        csp=person["csp"],
        n_pers=person["n_pers"]
    )
    
    trips["person_id"] = person["id"]
    
    all_trips.append(trips)
    
    
trips = pd.concat(all_trips)





# ----------------------------------------
# Emissions by mode
modes = pd.read_excel(
    data_folder_path / "input/sdes/entd_2008/entd_mode.xlsx",
    dtype={"mode_id": str}
)

emissions = pd.merge(trips, modes[["mode_id", "mode", "mode_group"]], on="mode_id")

# Compute the average yearly emissions
em = emissions.groupby(["person_id", "mode_group"], as_index=False)["co2"].sum()
em["co2"] /= 5 

fig = px.bar(
    em,
    x="person_id",
    y="co2",
    color="mode_group",
    labels={"person_id": "Identifiant de la personne", "co2": "Emissions de GES [kgCO2e/an]", "mode_group": "Mode"}
)
fig.update_layout(
    autosize=False,
    width=1000,
    height=600
)
fig.show()

fig.write_image("mode_footprint.png")


# ----------------------------------------
# Total distances for each person by mode
# Compute the average yearly travelled distance
dist = pd.merge(trips, modes[["mode_id", "mode", "mode_group"]], on="mode_id")
dist = dist.groupby(["person_id", "mode_group"], as_index=False)["dist"].sum()

fig = px.bar(
    dist,
    x="person_id",
    y="dist",
    color="mode_group",
    labels={"person_id": "Identifiant de la personne", "dist": "Distances parcourues [km/an]", "mode_group": "Mode"}
)

fig.update_layout(
    autosize=False,
    width=1000,
    height=600
)
fig.show()

fig.write_image("distances.png")




# ----------------------------------------
# Emissions by motive
motives = pd.read_excel(
    data_folder_path / "input/sdes/entd_2008/entd_location_motive.xlsx",
    dtype={"loc_mot_id": str}
)

emissions = pd.merge(trips, motives[["loc_mot_id", "motive"]], left_on="dest_loc_mot_id", right_on="loc_mot_id")

em = emissions.groupby(["motive"], as_index=False)["co2"].sum()
top_9_motives = em.sort_values("co2").tail(9)["motive"].values.tolist()


emissions["motive_group"] = "Autres"
emissions.loc[emissions["motive"].isin(top_9_motives), "motive_group"] = emissions.loc[emissions["motive"].isin(top_9_motives), "motive"] 

# Compute the average yearly emissions
em = emissions.groupby(["person_id", "motive_group"], as_index=False)["co2"].sum() 

top_9_motives.append("Autres")
top_9_motives.reverse()

fig = px.bar(
    em,
    x="person_id",
    y="co2",
    color="motive_group",
    category_orders={"motive_group": top_9_motives},
    labels={"person_id": "Identifiant de la personne", "co2": "Emissions de GES [kgCO2e/an]", "motive_group": "Motif"}
)

fig.update_layout(
    autosize=False,
    width=1000,
    height=600,
    legend=dict(yanchor="bottom")
)
fig.show()

fig.write_image("motive_footprint.png")


