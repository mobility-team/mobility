import os
import dotenv
import mobility
import pandas as pd

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

transport_zones = mobility.TransportZones("fr-74298", radius = 30)

travel_costs = mobility.MultimodalTravelCosts(transport_zones)
trans_mode_cm = mobility.TransportModeChoiceModel(travel_costs)
work_dest_cm = mobility.WorkDestinationChoiceModel(transport_zones, travel_costs)

costs = travel_costs.get()
mode_cm = trans_mode_cm.get()
work_cm = work_dest_cm.get()
