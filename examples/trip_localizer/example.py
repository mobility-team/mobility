import os
import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path="D:/data/mobility/projects/lyon"
)

transport_zones = mobility.TransportZones("13201", method="radius", radius=20.0)

# walk_travel_costs = mobility.TravelCosts(transport_zones, "walk")

pub_trans_travel_costs = mobility.PublicTransportTravelCosts(transport_zones)

# population = mobility.Population(transport_zones, sample_size=1000)

# trip_sampler = mobility.TripSampler(population)
# trip_localizer = mobility.TripLocalizer(transport_zones, motives=["work"])

# trips = trip_sampler.sample()
# trips = trip_localizer.localize(trips)

