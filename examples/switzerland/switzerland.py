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
transport_zones = mobility.TransportZones("fr-75113", radius = 70)

transport_zones.get().plot("urban_unit_category")

# from mobility.parsers import LocalAdminUnits

# lau = LocalAdminUnits()
# lau = lau.get()

# lau = lau.set_index("local_admin_unit_id")
# lau.loc["fr-75111"]
