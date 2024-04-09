import os
import dotenv
import mobility
import pandas as pd

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path="D:/data/mobility/projects/lyon"
)

transport_zones = mobility.TransportZones("69387", method="radius", radius=30.0)

travel_costs = mobility.MultimodalTravelCosts(transport_zones)
trans_mode_cm = mobility.TransportModeChoiceModel(travel_costs, cost_of_time=20.0)
work_dest_cm = mobility.WorkDestinationChoiceModel(transport_zones, travel_costs, cost_of_time=20.0, radiation_model_alpha=0.2, radiation_model_beta=0.8)

population = mobility.Population(transport_zones, 100)

trips = mobility.Trips(population)
loc_trips = mobility.LocalizedTrips(trips, cost_of_time=20.0, work_alpha=0.2, work_beta=0.8)
