import os
import dotenv
import mobility

dotenv.load_dotenv()

mobility.setup(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

transport_zones = mobility.TransportZones("34172", method="radius", radius=10)

car_travel_costs = mobility.TravelCosts(transport_zones, "car")
# walk_travel_costs = mobility.TravelCosts(transport_zones, "walk")
# bicycle_travel_costs = mobility.TravelCosts(transport_zones, "bicycle")
