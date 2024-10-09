import os
import dotenv
import mobility
import pandas as pd

# Mobility can set up env variables from a .env file located next to the 
# running script, containing for example :
# MOBILITY_PACKAGE_DATA_FOLDER=D:/data/mobility/data
# MOBILITY_PROJECT_DATA_FOLDER=D:/data/mobility/projects/gtfs_download_dates
# MOBILITY_GTFS_DOWNLOAD_DATE="2024-09-02"

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    debug=True
)

transport_zones = mobility.TransportZones("ch-6621", radius=20.0, level_of_detail=1)

walk = mobility.WalkMode(transport_zones)
bicycle = mobility.BicycleMode(transport_zones)
car = mobility.CarMode(transport_zones)

carpool = mobility.CarpoolMode(
    car,
    modal_shift=mobility.ModalShift(
        max_travel_time=20.0/60.0,
        average_speed=50.0,
        shift_time=10.0
    )
)

walk_pt = mobility.PublicTransportMode(
    transport_zones,
    first_leg_mode=walk,
    last_leg_mode=walk,
    first_modal_shift=mobility.ModalShift(
        max_travel_time=20.0/60.0,
        average_speed=5.0,
        shift_time=10.0
    ),
    last_modal_shift=mobility.ModalShift(
        max_travel_time=20.0/60.0,
        average_speed=5.0,
        shift_time=2.0
    )
)

car_pt = mobility.PublicTransportMode(
    transport_zones,
    first_leg_mode=car,
    last_leg_mode=walk,
    first_modal_shift=mobility.ModalShift(
        max_travel_time=20.0/60.0,
        average_speed=50.0,
        shift_time=10.0
    ),
    last_modal_shift=mobility.ModalShift(
        max_travel_time=20.0/60.0,
        average_speed=5.0,
        shift_time=2.0
    )
)

from mobility.choice_models.work_destination_choice_model import WorkDestinationChoiceModelParameters
import numpy as np

min_ssr = None

for selection_lambda in np.arange(0.99985, 0.99999, 0.00001):
    for fr_utility in np.arange(120.0, 140.0, 5.0):
        for ch_utility in np.arange(120.0, 140.0, 5.0):

            parameters = WorkDestinationChoiceModelParameters(
                model={
                    "type": "radiation",
                    "lambda": selection_lambda
                },
                utility={
                    "fr": fr_utility,
                    "ch": ch_utility
                }
            )
            
            work_dest_cm_2024 = mobility.WorkDestinationChoiceModel(
                transport_zones,
                modes=[
                    walk,
                    bicycle,
                    car,
                    walk_pt,
                    car_pt,
                    carpool
                ],
                parameters=parameters
            )
            
            work_dest_cm_2024.get()
            comparison = work_dest_cm_2024.get_comparison()
            ssi = work_dest_cm_2024.compute_ssi(comparison, 200)
            
            ssr = np.log(1 + comparison["flow_volume"]) - np.log(1 + comparison["ref_flow_volume"])
            ssr = ssr*ssr
            ssr = ssr.sum()
            
            if min_ssr is None:
                min_ssr = ssr
                best_parameters = [selection_lambda, fr_utility, ch_utility]
            elif ssr < min_ssr:
                min_ssr = ssr
                best_parameters = [selection_lambda, fr_utility, ch_utility]
            
            print("-----------------")
            print("lambda : " + str(selection_lambda))
            print("utility_fr : " + str(fr_utility))
            print("utility_ch : " + str(ch_utility))
            print(ssr)
            
            print("-----------------")
            print("-----------------")
            print("-----------------")


# best_parameters = [0.99996, 130.0, 135.0]

parameters = WorkDestinationChoiceModelParameters(
    model={
        "type": "radiation",
        "lambda": best_parameters[0]
    },
    utility={
        "fr": best_parameters[1],
        "ch": best_parameters[2]
    }
)

work_dest_cm_2024 = mobility.WorkDestinationChoiceModel(
    transport_zones,
    modes=[
        walk,
        bicycle,
        car,
        walk_pt,
        car_pt,
        carpool
    ],
    parameters=parameters
)

work_dest_cm_2024.get()
comparison = work_dest_cm_2024.get_comparison()
ssi = work_dest_cm_2024.compute_ssi(comparison, 400)

ssi

work_dest_cm_2024.plot_model_fit(comparison)

comparison["country_from"] = comparison["local_admin_unit_id_from"].str[0:2]

x = comparison.groupby(["local_admin_unit_id_to", "country_from"])["flow_volume"].sum()
x /= x.groupby("local_admin_unit_id_to").sum()

comparison[comparison["local_admin_unit_id_from"] == "fr-74012"].flow_volume.sum()
comparison[comparison["local_admin_unit_id_from"] == "fr-74012"].ref_flow_volume.sum()


comparison[comparison["local_admin_unit_id_to"] == "fr-74012"].flow_volume.sum()
comparison[comparison["local_admin_unit_id_to"] == "fr-74012"].ref_flow_volume.sum()


jobs, active_population = mobility.parsers.JobsActivePopulationDistribution().get()
jobs.loc["fr-74012"]
active_population.loc["fr-74012"]


ref_flows = mobility.parsers.JobsActivePopulationFlows().get()
f = ref_flows[ref_flows["local_admin_unit_id_from"] == "fr-74012"]
ref_flows[ref_flows["local_admin_unit_id_to"] == "fr-74012"].ref_flow_volume.sum()


sources, sinks = work_dest_cm_2024.prepare_sources_and_sinks(transport_zones.get())

sources.sum()
sinks.sum()

tz = transport_zones.get().drop(columns="geometry")


sources = pd.merge(sources.reset_index(), tz, left_on="from", right_on="transport_zone_id")
sources[sources.local_admin_unit_id.str[0:2] == "ch"].source_volume.sum()

sinks = pd.merge(sinks.reset_index(), tz, left_on="to", right_on="transport_zone_id")
sinks[sinks.local_admin_unit_id.str[0:2] == "ch"].sink_volume.sum()


sources.loc[38]
sinks.loc[38]

x = comparison[comparison["local_admin_unit_id_from"] == "fr-74012"]
x = comparison[comparison["local_admin_unit_id_from"] == "fr-74243"]
x = comparison[comparison["local_admin_unit_id_from"] == "ch-6621"]
x = comparison[comparison["local_admin_unit_id_to"] == "ch-6621"]
x = comparison[comparison["local_admin_unit_id_from"] == "ch-6640"]
x = comparison[comparison["local_admin_unit_id_to"] == "ch-6640"]

from mobility.concat_costs import concat_generalized_cost, concat_travel_costs

gc = concat_generalized_cost(
    [
        walk,
        bicycle,
        car,
        walk_pt,
        car_pt,
        carpool
    ])

gc_annemasse = gc[gc["from"] == 38]


tc = concat_travel_costs(
    [
        walk,
        bicycle,
        car,
        walk_pt,
        car_pt,
        carpool
    ])

tc_annemasse = tc[tc["from"] == 38]
