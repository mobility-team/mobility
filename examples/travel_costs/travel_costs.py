import os
import dotenv

from mobility.setup_mobility import setup_mobility
from mobility.transport_zones import get_transport_zones
from mobility.dodgr import prepare_dodgr_graph

dotenv.load_dotenv()

setup_mobility(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    path_to_pem_file=os.environ["MOBILITY_CERT_FILE"],
    http_proxy_url=os.environ["HTTP_PROXY"],
    https_proxy_url=os.environ["HTTPS_PROXY"]
)

transport_zones = get_transport_zones("77468", method="radius", radius=10)
graph = prepare_dodgr_graph(transport_zones, mode="car", force=True)
