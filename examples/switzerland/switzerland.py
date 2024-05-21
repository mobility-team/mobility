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

# travel_costs_car = mobility.TravelCosts(transport_zones, "car").get()
travel_costs_pt = mobility.PublicTransportTravelCosts(transport_zones).get()
