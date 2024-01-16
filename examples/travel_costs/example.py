import os
import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    path_to_pem_file=os.environ["MOBILITY_CERT_FILE"],
    http_proxy_url=os.environ["HTTP_PROXY"],
    https_proxy_url=os.environ["HTTPS_PROXY"],
)

transport_zones = mobility.TransportZones("69123", method="radius", radius=10)


car_travel_costs = mobility.TravelCosts(transport_zones, "car")

walk_travel_costs = mobility.TravelCosts(transport_zones, "walk")

bicycle_travel_costs = mobility.TravelCosts(transport_zones, "bicycle")
