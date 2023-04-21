from mobility.get_insee_data import get_insee_data

import datetime
from r5py import TravelTimeMatrixComputer, TransitMode, LegMode
from r5py import TransportNetwork

insee_data = get_insee_data()
workers = insee_data["active_population"]
jobs = insee_data["jobs"]

transport_network = TransportNetwork(
    "D:/data/osm/limousin-latest.osm.pbf",
    [
        "D:/data/gtfs/ca_limoges_metropole-aggregated-gtfs.zip"
    ]
)

travel_time_matrix_computer = TravelTimeMatrixComputer(
    transport_network,
    origins=origin,
    destinations=points,
    departure=datetime.datetime(2022,2,22,8,30),
    transport_modes=[TransitMode.TRANSIT, LegMode.WALK]
)