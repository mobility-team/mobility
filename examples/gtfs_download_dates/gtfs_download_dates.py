import dotenv
import mobility

# Mobility can set up env variables from a .env file located next to the 
# running script, containing for example :
# MOBILITY_PACKAGE_DATA_FOLDER=D:/data/mobility/data
# MOBILITY_PROJECT_DATA_FOLDER=D:/data/mobility/projects/gtfs_download_dates
# MOBILITY_GTFS_DOWNLOAD_DATE="2024-09-02"

dotenv.load_dotenv()
mobility.set_params()

transport_zones = mobility.TransportZones("fr-24037", radius=21.0)
gtfs_router = mobility.GTFSRouter(transport_zones)

gtfs_router.get()
