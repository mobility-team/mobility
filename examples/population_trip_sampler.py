import sys
sys.path.append("..")
sys.path.append(".")

# Put the path to the population package
sys.path.append("D:\DATA\F.POUCHAIN\dev\population2")
from population import Population

import os
import time
import dask
from dask.distributed import Client

import pandas as pd
import numpy as np
from mobility import TripSampler

codgeo = "75112"

# Sample a population of 10 households
pop = Population(codgeo=codgeo)
pop.sample(N_households=100)

# Prepare the variables for the mobility model
hh_ref_pers_cs1 = pop.individuals.groupby("household_id", as_index=False)["socio_pro_category"]

individuals = pop.individuals.copy()
hh_ref_pers_cs1 = individuals[individuals["ref_person_link"] == "1"][["household_id", "socio_pro_category"]]
hh_ref_pers_cs1.columns = ["household_id", "csp_ref_pers"]

individuals = individuals[["household_id", "individual_id", "socio_pro_category", "economic_sector"]]
individuals.columns = ["household_id", "individual_id", "csp", "na5"]

individuals = pd.merge(individuals, hh_ref_pers_cs1, on="household_id")
individuals = pd.merge(individuals, pop.households[["household_id", "n_persons"]], on="household_id")

individuals["n_persons"] = np.where(individuals["n_persons"] < 3, individuals["n_persons"].astype(str), "3+")

individuals = individuals.to_dict(orient="records")

# Set up a dask client to handle parallelization and multi threading
client = Client()

# Task to initialize a TripSampler object
trip_sampler = dask.delayed(TripSampler)(codgeo=codgeo)

# Tasks to get the trips of each person
all_trips = []
for ind in individuals:
    trips = dask.delayed(trip_sampler.get_trips)(
          person_id=ind["individual_id"],
          na5=ind["na5"],
          csp_ref_pers=ind["csp_ref_pers"],
          csp=ind["csp"],
          n_pers=ind["n_persons"]
    )
    all_trips.append(trips)

# Task to merge the dataframes of trips of each person
def concat_trips(trips):
    return pd.concat(trips)

trips = dask.delayed(concat_trips)(all_trips)

# Run the tasks
trips = trips.compute()

# Don't forget to close the dask client once calculations are done
client.close()

