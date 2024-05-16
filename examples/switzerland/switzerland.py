import os
import dotenv
import mobility
import pandas as pd

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

transport_zones = mobility.TransportZones(["fr-74012", "fr-74298", "ch-6621"])
transport_zones = mobility.TransportZones("ch-6621", radius = 60)