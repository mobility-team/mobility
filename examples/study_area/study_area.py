import os
import dotenv
import mobility


# Mobility can set up env variables from a .env file located next to the 
# running script, containing for example :
# MOBILITY_PACKAGE_DATA_FOLDER=D:/data/mobility/data
# MOBILITY_PROJECT_DATA_FOLDER=D:/data/mobility/projects/gtfs_download_dates
# MOBILITY_GTFS_DOWNLOAD_DATE="2024-09-02"

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    debug=True
)

transport_zones = mobility.TransportZones("ch-6621", radius=20.0, level_of_detail=1)

walk = mobility.WalkMode(transport_zones)

modes = [
    walk,
    mobility.PublicTransportMode(transport_zones, walk),
    mobility.BicycleMode(transport_zones),
    mobility.CarMode(transport_zones)
]

travel_costs = mobility.MultiModalMode(transport_zones, modes).travel_costs
work_dest_cm = mobility.WorkDestinationChoiceModel(transport_zones, travel_costs)

work_dest_cm.get()


flows = pd.read_parquet(work_dest_cm.cache_path["od_flows"])

comparison = work_dest_cm.get_comparison()

act_pop = comparison.groupby(["local_admin_unit_id_from"])[["flow_volume", "ref_flow_volume"]].sum()
jobs = comparison.groupby(["local_admin_unit_id_to"])[["flow_volume", "ref_flow_volume"]].sum()

work_dest_cm.compute_ssi(comparison, 200)
work_dest_cm.plot_model_fit(comparison)
