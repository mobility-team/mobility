import os
import dotenv
import mobility

from mobility.parsers import MobilitySurvey

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path="D:/data/mobility/projects/lyon"
)

transport_zones = mobility.TransportZones("69387", method="radius", radius=30.0)
population = mobility.Population(transport_zones, sample_size=1000)
trips = mobility.Trips(population, source="ENTD-2008")