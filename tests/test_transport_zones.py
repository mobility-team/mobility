import os
import dotenv
import mobility
import pathlib

def test_pub_trans():
  mobility.set_params(str(pathlib.Path.home() / ".mobility/data"),str(pathlib.Path.home() / ".mobility/data/projects"))
  dotenv.load_dotenv()
  transport_zones = mobility.TransportZones("31404", method="radius", radius=40)
  pub_trans_travel_costs = mobility.PublicTransportTravelCosts(transport_zones)
