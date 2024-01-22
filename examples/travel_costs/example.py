import os
import dotenv
import pathlib
import mobility

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

transport_zones = mobility.TransportZones("34172", method="radius", radius=10)

# car_travel_costs = mobility.TravelCosts(transport_zones, "car")
# walk_travel_costs = mobility.TravelCosts(transport_zones, "walk")
# bicycle_travel_costs = mobility.TravelCosts(transport_zones, "bicycle")

gtfs = mobility.GTFS(files=[pathlib.Path("D:/dev/mobility_oss/examples/travel_costs") / "TAM_MMM_GTFS.zip"])

pub_trans_travel_costs = mobility.TravelCosts(transport_zones, "public_transport", gtfs)
