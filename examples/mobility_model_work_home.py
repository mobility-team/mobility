import sys
sys.path.append("..")

import os
from pathlib import Path
import pandas as pd
import numpy as np
from matplotlib import pyplot
from mobility import WorkMobilityModel

codgeo = "75113"
na5 = "GU"
cs1 = "3"

mob_model = WorkMobilityModel(codgeo)

mob_model.add_to_source(codgeo, na5, cs1, 1000)

sink_probabilities = mob_model.compute_sink_probabilities(origin=codgeo, na5=na5, cs1=cs1)
sink_probabilities.sort_values().tail(10)

source_probabilities = mob_model.compute_source_probabilities(destination=codgeo, na5=na5, cs1=cs1)
source_probabilities.sort_values().tail(10)



# ---------------------------------------------
# Comparison with INSEE reference flows
data_folder_path = Path(os.path.dirname(__file__)).parent / "data"

with open(data_folder_path / "input/insee/recensement/RP2016_MOBPRO.parquet", "rb") as f:
    mob = pd.read_parquet(f, columns=["COMMUNE", "ARM", "DCLT", "NA5", "CS1", "IPONDI"])

mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "COMMUNE"] = mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "ARM"]
mob["IPONDI"] = mob["IPONDI"].astype(float)

mob = mob.groupby(["COMMUNE", "DCLT", "NA5", "CS1"])["IPONDI"].sum()
mob.name = "p_ij_ref"


ref_sink_p = mob.xs(codgeo, level=0).xs(na5, level=1).xs(cs1, level=1)
ref_sink_p = ref_sink_p/ref_sink_p.sum()
ref_sink_p = ref_sink_p.to_frame()

sink_probabilities = sink_probabilities.to_frame()

comp = pd.merge(sink_probabilities, ref_sink_p, left_on="location_id", right_on="to", left_index=True, right_index=True)
comp.sort_values("p_ij_ref").tail(10)

fig2, ax = pyplot.subplots(nrows=1, ncols=1)
ax.scatter(np.log(comp["p_ij_ref"]), np.log(comp["p_ij"]))
ax.plot(np.log([1e-5, 1]), np.log([1e-5, 1]), color="red")




ref_source_p = mob.xs(codgeo, level=1).xs(na5, level=1).xs(cs1, level=1)
ref_source_p = ref_source_p/ref_source_p.sum()
ref_source_p = ref_source_p.to_frame()

source_probabilities = source_probabilities.to_frame()

comp = pd.merge(source_probabilities, ref_source_p, left_on="location_id", right_on="to", left_index=True, right_index=True)
comp.sort_values("p_ij_ref").tail(10)

fig2, ax = pyplot.subplots(nrows=1, ncols=1)
ax.scatter(np.log(comp["p_ij_ref"]), np.log(comp["p_ij"]))
ax.plot(np.log([1e-5, 1]), np.log([1e-5, 1]), color="red")


