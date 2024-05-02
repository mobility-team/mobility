import os
import dotenv
import mobility

mobility.set_params()

def test_pub_trans():
  dotenv.load_dotenv()
  transport_zones = mobility.TransportZones("31404", method="radius", radius=40)
  pub_trans_travel_costs = mobility.PublicTransportTravelCosts(transport_zones)
