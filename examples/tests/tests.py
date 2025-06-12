import os
import dotenv
import mobility
import pandas as pd

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

transport_zones = mobility.TransportZones("fr-12202", radius = 10.0)

population = mobility.Population(
    transport_zones=transport_zones,
    sample_size=100
)

travel_costs_car = mobility.TravelCosts(transport_zones, "car")
travel_costs_pt = mobility.PublicTransportTravelCosts(transport_zones)

trips = mobility.Trips(population)
loc_trips = mobility.LocalizedTrips(trips)

transport_zones.get()
population.get()
travel_costs_car.get()
travel_costs_pt.get()

trips.get()
loc_trips.get()
