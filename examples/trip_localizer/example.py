import os
import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path="D:/data/mobility/projects/lyon"
)

transport_zones = mobility.TransportZones("69383", method="radius", radius=20.0)

population = mobility.Population(transport_zones, sample_size=10000)
