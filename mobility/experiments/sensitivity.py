import os
import dotenv
import logging

import mobility

import polars as pl

dotenv.load_dotenv()

os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"] = "2025/12/01"

mobility.set_params(
        package_data_folder_path="D:/mobility-data",
        project_data_folder_path="D:/test-09",
        debug=False
)

global_metrics = pl.DataFrame()
ssis = pl.DataFrame()
ssis200 = pl.DataFrame()

for radius in range(36, 51, 2):
    if radius not in [36]: #blockage for this one
        print(f"\nRADIUS: {radius}")
        transport_zones = mobility.TransportZones("fr-87085", radius = radius, level_of_detail=1)
        emp = mobility.EMPMobilitySurvey()
        pop = mobility.Population(transport_zones, sample_size = 1000)
        modes = [mobility.CarMode(transport_zones), mobility.WalkMode(transport_zones),
                 mobility.BicycleMode(transport_zones), mobility.PublicTransportMode(transport_zones)]
        surveys = [emp]
        motives = [mobility.HomeMotive(), mobility.WorkMotive(), mobility.OtherMotive(population=pop)]
        
        
        # Simulating the trips for this population for three modes : car, walk and bicyle, and only home and work motives (OtherMotive is mandatory)
        pop_trips = mobility.PopulationTrips(
            pop,
            modes,
            motives,
            surveys,
            parameters=mobility.PopulationTripsParameters(n_iterations=4, k_mode_sequences=42)
            )
    
        labels=pop_trips.get_prominent_cities()
        
        if global_metrics.is_empty():
            global_metrics = pop_trips.evaluate("global_metrics")
        else:
            suffix = str(radius)
            global_metrics = global_metrics.join(pop_trips.evaluate("global_metrics"), on =["country", "variable"], suffix=suffix)
        
        if radius % 40 == 0:            
            metrics_by_mode = pop_trips.evaluate("metrics_by_variable", variable="mode", plot=True)
            metrics_by_motive = pop_trips.evaluate("metrics_by_variable", variable="motive", plot=True)
        
        # # OD flows between transport zones
        pop_trips.plot_od_flows(mode="car", level_of_detail=1, labels=labels)
        pop_trips.plot_od_flows(mode="walk", level_of_detail=1, labels=labels)
        pop_trips.plot_od_flows(mode="bicycle", level_of_detail=1, labels=labels)
        pop_trips.plot_od_flows(mode="public_transport", labels=labels)
        
        if ssis.is_empty():
            rad = pl.DataFrame({"radius": radius})
            ssis = pl.concat([rad, pop_trips.evaluate("ssi")], how="horizontal")
            ssis200 = pl.concat([rad, pop_trips.evaluate("ssi", threshold=200)], how="horizontal")
        else:
            rad = pl.DataFrame({"radius": radius})
            ssis = pl.concat([ssis, pl.concat([rad, pop_trips.evaluate("ssi")], how="horizontal")])
            ssis200 = pl.concat([ssis200, pl.concat([rad, pop_trips.evaluate("ssi", threshold=200)], how="horizontal")])
        
        print(global_metrics)
        print(ssis)
        print(ssis200)
ssis.to_pandas().plot(x="radius")
ssis200.to_pandas().plot(x="radius")
    
    
        