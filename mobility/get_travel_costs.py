import r5py
import datetime
import pandas as pd
import numpy as np
import math

def get_travel_costs(transport_zones, osm_file_path, mode, departure_time=None, max_time=None):
    
    # Complete and transform user inputs
    r5py_modes = {
        "car": [r5py.TransportMode.CAR],
        "public_transport": [r5py.TransportMode.TRANSIT],
        "bicycle": [r5py.TransportMode.BICYCLE],
        "walk": [r5py.TransportMode.WALK]
    }
    
    transport_modes = r5py_modes[mode]
    
    default_max_times = {
        "car": datetime.timedelta(hours=1),
        "public_transport": datetime.timedelta(hours=1),
        "bicycle": datetime.timedelta(hours=1),
        "walk": datetime.timedelta(hours=1)
    }
    
    if max_time is None:
        max_time = default_max_times[mode]
        
    if departure_time is None:
        departure_time = datetime.datetime.now()
    
    if mode not in ["car", "public_transport", "bicycle", "walk"]:
        raise ValueError("Available modes are: car, public_transport, bicycle or walk.")
    
    # Load the OSM data as a r5py/r5 TransportNetwork
    transport_network = r5py.TransportNetwork(osm_file_path)
    
    # Prepare the origins of travel within the transport zones (centroids)
    origins = transport_zones.copy()
    origins["id"] = origins["transport_zone_id"]
    origins["geometry"] = origins.centroid
    
    # Transform to WGS 84
    origins = origins.to_crs("4326")
    
    # Snap centroids to the nearest street network node
    origins["geometry"] = transport_network.snap_to_network(origins["geometry"])
        
    # Compute the travel times between all cities
    # (chunk the request to 50-to-all cities, the request might be too large otherwise)
    travel_times = []
    
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
            
    chunks = chunk_list(range(origins.shape[0]), 50)
            
    for i, index in enumerate(chunks):
        
        print("Computing travel times for chunk of origin : " + str(i) + "/" + str(math.ceil(origins.shape[0]/50)))

        tt_mat_computer = r5py.TravelTimeMatrixComputer(
            transport_network,
            origins=origins.iloc[index],
            destinations=origins,
            departure=departure_time,
            transport_modes=transport_modes,
            max_time=max_time,
            snap_to_network=True
        )
    
        tt = tt_mat_computer.compute_travel_times()
        
        travel_times.append(tt)
        

    travel_times = pd.concat(travel_times)
    
    # Approximation : for same city trips, make the travel time half of the
    # minimum travel time to other cities
    travel_times_other_cities = travel_times[travel_times["from_id"] != travel_times["to_id"]]
    min_travel_times = travel_times_other_cities.groupby("from_id", as_index=False)["travel_time"].min()
    
    travel_times = pd.merge(travel_times, min_travel_times, on="from_id", suffixes=["", "_min"])
    
    travel_times["travel_time"] = np.where(
        travel_times["from_id"] == travel_times["to_id"],
        travel_times["travel_time_min"]/2,
        travel_times["travel_time"]
    )
    
    travel_times.drop("travel_time_min", axis=1, inplace=True)
    
    
    detailed_itineraries_computer = r5py.DetailedItinerariesComputer(
        transport_network,
        origins=origins[0:20],
        destinations=origins[0:50],
        departure=datetime.datetime(2022,2,22,8,30),
        transport_modes=[r5py.TransportMode.CAR]
    )
    
    travel_details = detailed_itineraries_computer.compute_travel_details()

    travel_details["time_seconds"] = travel_details["travel_time"].dt.total_seconds()/60
    
    
    travel_details[["from_id", "to_id", "time_seconds", "distance", "geometry"]].to_file("travel_details.gpkg")

    
    return travel_times

