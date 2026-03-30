import polars as pl
import numpy as np
import random

trips = pl.DataFrame({
    "index": [0, 1, 2, 3, 4, 5],
    "from": [0, 1, 2, 1, 0, 1],
    "to": [1, 2, 1, 0, 1, 0]
})

p_car = pl.DataFrame({
    "from": [0, 1, 2, 1],
    "to": [1, 2, 1, 0],
    "p": [0.5, 0.75, 0.5, 0.5],
    "cost": [0.0, 10.0, 1.0, 1.0],
    "mode": ["car", "car", "car", "car"]
})

p_walk = pl.DataFrame({
    "from": [0, 1, 2, 1],
    "to": [1, 2, 1, 0],
    "p": [0.5, 0.25, 0.5, 0.5],
    "cost": [1.0, 20.0, 1.0, 1.0],
    "mode": ["walk", "walk", "walk", "walk"]
})

p = pl.concat([p_car, p_walk])
p = p.join(trips, on=["from", "to"])

average_cost = p.group_by(["from", "to"]).agg(cost=(pl.col("p")*pl.col("cost")).sum())
average_cost = trips.join(average_cost, on=["from", "to"]).sort("index")
costs = average_cost["cost"].to_list()
average_cost = average_cost.to_dicts()

trips = trips.to_dicts()
n_trips = len(trips)

samples = []

for i in range(1000):
    
    max_cost_trip_index = np.argmax(costs)
    modes = [None]*n_trips
    
    car_location = [None]*(n_trips+1)
    car_location[0] = 0
    car_location[n_trips] = 0
    
    car_availability = [None]*(n_trips+1)
    car_availability[0] = True
    car_availability[n_trips] = True

    while any([m is None for m in modes]):
    
        max_cost_trip = trips[max_cost_trip_index]
        
        prob = p.filter(pl.col("from") == max_cost_trip["from"], pl.col("to") == max_cost_trip["to"])
        
        if car_availability[max_cost_trip_index] is False:
            prob = prob.filter(pl.col("mode") != "car")
        
        mode = random.choices(prob["mode"].to_list(), weights=prob["p"].to_list(), k=1)[0]
        modes[max_cost_trip_index] = mode
    
        if mode == "car":
            
            # Make the car available
            # car_availability[max_cost_trip_index] = True
            car_availability[max_cost_trip_index+1] = True
            
            # Track the location of the car
            car_location[max_cost_trip_index] = max_cost_trip["from"]
            car_location[max_cost_trip_index+1] = max_cost_trip["to"]
            
            # The first home departing trip must be made by car
            if modes[0] is None:
                modes[0] = "car"
                car_location[0] = trips[0]["from"]
                car_location[1] = trips[0]["to"]
            
            # The last remaining home returning trip must be made by car (not necessarily the last trip of the day)
            next_trips_home = [t for t in trips if t["to"] == 0 and t["index"] > max_cost_trip_index]
            
            if len(next_trips_home) == 1:
                
                trip = next_trips_home[0]
                trip_index = trip["index"]
                
                modes[trip_index] = "car"
                
                # car_availability[trip_index] = True
                car_availability[trip_index+1] = True
                
                car_location[trip_index] = trip["from"]
                car_location[trip_index+1] = 0
                
        else:
            
            if max_cost_trip["from"] != max_cost_trip["to"]:
                car_availability[max_cost_trip_index+1] = False
        
        # If the car has to change locations in a given trip, force the mode to car
        for i in range(len(modes)):
            if modes[i] is None and car_location[i] is not None and car_location[i+1] is not None and car_location[i] != car_location[i+1]:
                modes[i] = "car"
                
        remaining_trips_index = [i for i in range(len(modes)) if modes[i] is None]
        
        # If the previous and next trips have no mode yet, choose the highest cost trip as next target
        if max_cost_trip_index-1 in remaining_trips_index and max_cost_trip_index+1 in remaining_trips_index:
            max_cost_trip_index = max_cost_trip_index-1 if costs[max_cost_trip_index-1] > costs[max_cost_trip_index+1] else max_cost_trip_index+1
        
        # Else take the previous one
        elif max_cost_trip_index-1 in remaining_trips_index and max_cost_trip_index-1 >= 0:
            max_cost_trip_index = max_cost_trip_index-1
        
        # Else take the next one
        elif max_cost_trip_index+1 in remaining_trips_index and max_cost_trip_index+1 < n_trips:
            max_cost_trip_index = max_cost_trip_index+1
            
        # Else if both previous and next trips modes are already set, take the next unset trip
        elif len(remaining_trips_index) > 0:
            max_cost_trip_index = remaining_trips_index[0]
            
        else:
            pass
            
    samples.append(modes)
    

modes = pl.DataFrame(samples, orient="row")

mode_shares = modes.unpivot().group_by(["variable", "value"]).len().sort(["variable", "value"])

print(mode_shares)

m = modes.unique().to_pandas()

print(m)
