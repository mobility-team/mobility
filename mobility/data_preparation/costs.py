import pandas as pd
import numpy as np

import os
from pathlib import Path

data_folder_path = Path(os.path.dirname(__file__)).parent.parent / "data"

# ---------------------------------------------
# Average cost of travel between cities
with open(data_folder_path / "input/insee/recensement/RP2016_MOBPRO.parquet", "rb") as f:
    mob = pd.read_parquet(f, columns=["COMMUNE", "ARM", "DCLT", "TRANS", "IPONDI"])

mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "COMMUNE"] = mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "ARM"]
mob["IPONDI"] = mob["IPONDI"].astype(float)

costs = pd.read_csv(data_folder_path / "input/mobility/costs/unit_costs.csv", dtype={"TRANS": str, "unit_cost": np.float64})

# Possibility to add a cost of time spent on the trip
costs["unit_cost"] = costs["cost_per_km"] + 0*costs["value_of_time"]/costs["speed"]

costs.to_csv(data_folder_path / "input/mobility/costs/unit_costs.csv", index=False)

mob = pd.merge(mob, costs, on="TRANS")

mob["cost_w"] = mob["IPONDI"]*mob["unit_cost"]

mob_cost = mob.groupby(["COMMUNE", "DCLT"], as_index=False)[["IPONDI", "cost_w"]].sum()
mob_cost["cost"] = mob_cost["cost_w"]/mob_cost["IPONDI"]

# Set a minimum cost to avoid getting a cost of zero
# when all people are in the "walk"/"no transport" category
# mob_cost["cost"] = np.maximum(mob_cost["cost"], 0.001)

# Interpolate costs for small flows (< 10 persons)
# between a base value of 0.15 €/km (car) and the average cost for this flow
mob_cost["cost"] = np.minimum(mob_cost["IPONDI"]/10, 1.0)*mob_cost["cost"] + (1 -np.minimum(mob_cost["IPONDI"]/10, 1.0))*costs.loc[costs["TRANS"] == "4", "unit_cost"].values[0]
mob_cost["cost"] = np.round(mob_cost["cost"], 4)

mob_cost = mob_cost[["COMMUNE", "DCLT", "cost"]]
mob_cost.columns = ["from", "to", "cost_per_km"]

mob_cost.set_index(["from", "to"], inplace=True)

mob_cost.to_csv(data_folder_path / "input/mobility/costs/trips_average_unit_cost.csv")


mob.groupby(["COMMUNE", "DCLT", "TRANS"])["IPONDI"].sum()
