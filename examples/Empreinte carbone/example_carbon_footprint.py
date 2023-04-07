import pandas as pd
import numpy as np
# mobility utils
import trip_sampler as tp
import carbon_computation as cc
# population package
import population

"""
This script aims to estimate average mobility carbon impact of an inhabitant 
It samples a representative population of the city chosen 
and compute their annual trips and emisssions with Mobility
It is required to install population package created by elioth
(https://gitlab.com/elioth/population)
"""


# ===================
# PARAMETERS

# Location code
codgeo = "29019" 


# ===================
# SAMPLE REPRESENTATIVE HOUSEHOLDS

# Sample 200 households 
pop = population.Population(codgeo=codgeo)
pop.sample(N_households=200, year="2018")
print("CODGEO : " + codgeo)
print("Number of households :", pop.households.shape[0])
print("Number of persons :", pop.households["n_persons"].sum())

# TODO function inside population package ?
# Load people's caracteristics
hh_ref_pers_cs1 = pop.individuals.groupby("household_id", as_index=False)["socio_pro_category"]
individuals = pop.individuals.copy()
individuals_id = individuals[["household_id", "individual_id"]].drop_duplicates()
hh_ref_pers_cs1 = individuals[individuals["ref_person_link"] == "1"][["household_id", "socio_pro_category"]]
hh_ref_pers_cs1.columns = ["household_id", "csp_ref_pers"]
individuals = individuals[["household_id", "individual_id", "socio_pro_category", "economic_sector"]]
individuals.columns = ["household_id", "individual_id", "csp", "na5"]
individuals.loc[
    (individuals["na5"] == "ZZ") & (individuals["csp"].isin(["1", "2", "3", "4", "5", "6"])), "csp"] = "8"
employees = individuals.groupby(["csp", "na5"], as_index=False)["individual_id"].count().rename(
    columns={"csp": "cs1"})
employees = employees.loc[(employees["cs1"].isin(["1", "2", "3", "4", "5", "6"])) & (employees["na5"] != "ZZ")]
individuals = pd.merge(individuals, hh_ref_pers_cs1, on="household_id")
individuals = pd.merge(individuals, pop.households[["household_id", "n_persons"]], on="household_id")
individuals["n_persons"] = np.where(individuals["n_persons"] < 3, individuals["n_persons"].astype(str), "3+")
# Add data about car ownership
individuals = pd.merge(individuals, pop.households.copy().drop(columns="n_persons"), how="left", on="household_id")
individuals["n_cars"] = individuals["VOIT"]
individuals["VOIT"] = individuals["VOIT"].astype(int)
individuals.loc[individuals["VOIT"]>1, "n_cars"] = "2+"
individuals_list = individuals.to_dict(orient="records")


# ===================
# FIND CITY CATEGORY

# Associate a city category depending on codgeo
cities_category = pd.read_csv("./cities_category.csv", dtype=str)
cities_category.set_index("codgeo", inplace=True)
# Avoid problems due to arrondissement in Paris, Lyon, Marseille
if codgeo in [str(75100 + i) for i in range(1, 21)]:
    city_category = cities_category.loc["75056"].values[0]
elif codgeo in [str(69380 + i) for i in range(1, 10)]:
    city_category = cities_category.loc["69123"].values[0]
elif codgeo in [str(13200 + i) for i in range(1, 16)]:
    city_category = cities_category.loc["13055"].values[0]
    codgeo = "13055"
else:
    city_category = cities_category.loc[codgeo].values[0]


# ===================
# COMPUTE ANNUAL TRIPS AND CO2e EMISSIONS

# Compute trips and associated emissions
all_trips = pd.DataFrame()
# Compute trips
ts = tp.TripSampler()
for person in individuals_list:
    trips = ts.get_trips(
      csp=person["csp"],
      csp_household=person["csp_ref_pers"],
      urban_unit_category=city_category,
      n_pers=person["n_persons"],
      n_cars=person["n_cars"],
      n_years=1
    )
    trips["individual_id"] = person["individual_id"]
    # Add trips of every individuals to the master table
    all_trips = pd.concat([all_trips, trips])
all_trips["codgeo"] = codgeo
all_trips["city_category"] = city_category

# Compute CO2e emissions
emissions = cc.carbon_computation(all_trips)

# ===================
# COMPUTE AVERAGE INHABITANT MOBILITY CARBON FOOTPRINT
average_fp = emissions.groupby(["individual_id"], as_index=False)["carbon_emissions"].sum()
print("Average annual carbon footprint :", round(average_fp["carbon_emissions"].mean()/1000,2), "tCO2e/pers.yr")
print("Maximal annual carbon footprint :", round(average_fp["carbon_emissions"].max()/1000,2), "tCO2e/pers.yr")
print("Minimal annual carbon footprint :", round(average_fp["carbon_emissions"].min()/1000,2), "tCO2e/pers.yr")

