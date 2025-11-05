import os
import dotenv

import mobility

dotenv.load_dotenv()

mobility.set_params(
    # package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    # project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
        package_data_folder_path="D:/mobility-data",
        project_data_folder_path="D:/test-09",
)

# Using Foix (a small town) and a limited radius for quick results
transport_zones = mobility.TransportZones("fr-09122", radius = 20)

# Using EMP, the 
emp = mobility.parsers.mobility_survey.france.EMPMobilitySurvey()

pop = mobility.Population(transport_zones, sample_size = 1000)

pop_trips = mobility.PopulationTrips(
    pop,
    [mobility.CarMode(transport_zones), mobility.WalkMode(transport_zones), mobility.BicycleMode(transport_zones)],
    [mobility.HomeMotive(), mobility.WorkMotive()],
    [emp]
    )

#global_metrics = pop_trips.evaluate("global_metrics")
pop_trips.plot_od_flows()
