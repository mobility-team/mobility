# -*- coding: utf-8 -*-
"""
Éditeur de Spyder

Ceci est un script temporaire.
"""

import sys

sys.path.insert(0, "../..")

import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params()

transport_zones = mobility.TransportZones("87085", method="radius", radius=40)

# car_travel_costs = mobility.TravelCosts(transport_zones, "car")
# walk_travel_costs = mobility.TravelCosts(transport_zones, "walk")
# bicycle_travel_costs = mobility.TravelCosts(transport_zones, "bicycle")

pub_trans_travel_costs = mobility.PublicTransportTravelCosts(transport_zones)