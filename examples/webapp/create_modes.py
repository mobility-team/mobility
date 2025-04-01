import mobility

def create_modes(transport_zones, constants, congestion, gtfs_paths, speed_modifiers={}, car_cost_of_distance=0.1):
    
    # Offset the value of time so that it matches the swiss survey of 2015 and not 2021
    # (see the survey results analysis that mentions anormal 2021 values)
    
    walk = mobility.WalkMode(
        transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_constant=constants["walk"],
            cost_of_distance=0.0,
            cost_of_time=mobility.CostOfTimeParameters(
                intercept=13.5 - 13.0,
                breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
                slopes=[0.484, 0.88, 0.074, 0.101],
                max_value=27.5 - 13.0
            )
        )
    )
    
    bicycle_speed_modifiers = []
    
    if "bicycle" in speed_modifiers.keys():
        bicycle_speed_modifiers.extend(speed_modifiers["bicycle"])

    bicycle = mobility.BicycleMode(
        transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_constant=constants["bicycle"],
            cost_of_distance=0.0,
            cost_of_time=mobility.CostOfTimeParameters(
                intercept=23.5 - 23.5,                             
                breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
                slopes=[4.46, 0.385, 0.073, 0.169],
                max_value=43.5 - 23.5
            )
        )
    )

    car_cost_constant = constants["car"]

    car_cost_of_time = mobility.CostOfTimeParameters(
        intercept=7.7 - 1.0,
        breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
        slopes=[1.3, 0.424, 0.265, 0.18],
        max_value=29.7 - 1.0
    )
    

    car = mobility.CarMode(
        transport_zones,
        congestion=congestion,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_constant=car_cost_constant,
            cost_of_distance=car_cost_of_distance,
            cost_of_time=car_cost_of_time
        )
    )


    routing_parameters = mobility.PublicTransportRoutingParameters(additional_gtfs_files=gtfs_paths)

    pt_gen_cost_parms = mobility.GeneralizedCostParameters(
        cost_constant=constants["public_transport"],
        cost_of_distance=0.0,
        cost_of_time=mobility.CostOfTimeParameters(
            intercept=11.0 - 4.0,
            breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
            slopes=[0.0, 1.0, 0.1, 0.067],
            max_value=25.0 - 4.0
        )
    )
    
    walk_intermodal_transfer = mobility.IntermodalTransfer(
        max_travel_time=20.0/60.0,
        average_speed=5.0,
        transfer_time=1.0
    )
    
    bicycle_intermodal_transfer = mobility.IntermodalTransfer(
        max_travel_time=20.0/60.0,
        average_speed=15.0,
        transfer_time=2.0
    )
    
    car_intermodal_transfer = mobility.IntermodalTransfer(
        max_travel_time=20.0/60.0,
        average_speed=50.0,
        transfer_time=15.0
    )

    walk_pt = mobility.PublicTransportMode(
        transport_zones,
        first_leg_mode=walk,
        last_leg_mode=walk,
        first_intermodal_transfer=walk_intermodal_transfer,
        last_intermodal_transfer=walk_intermodal_transfer,
        generalized_cost_parameters=pt_gen_cost_parms,
        routing_parameters=routing_parameters
    )

    car_pt = mobility.PublicTransportMode(
        transport_zones,
        first_leg_mode=car,
        last_leg_mode=walk,
        first_intermodal_transfer=car_intermodal_transfer,
        last_intermodal_transfer=walk_intermodal_transfer,
        generalized_cost_parameters=pt_gen_cost_parms,
        routing_parameters=routing_parameters
    )

    bicycle_pt = mobility.PublicTransportMode(
        transport_zones,
        first_leg_mode=bicycle,
        last_leg_mode=walk,
        first_intermodal_transfer=bicycle_intermodal_transfer,
        last_intermodal_transfer=walk_intermodal_transfer,
        generalized_cost_parameters=pt_gen_cost_parms,
        routing_parameters=routing_parameters
    )
    
    carpool_cost_of_time = mobility.CostOfTimeParameters(
        intercept=(7.7 - 1.0)*0.9,
        breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
        slopes=[1.3, 0.424, 0.265, 0.18],
        max_value=(29.7 - 1.0)*0.9
    )

    carpool = mobility.CarpoolMode(
        car,
        intermodal_transfer=mobility.IntermodalTransfer(
            max_travel_time=20.0/60.0,
            average_speed=50.0,
            transfer_time=10.0,
            shortcuts_transfer_time=4.0
        ),
        generalized_cost_parameters=mobility.DetailedCarpoolGeneralizedCostParameters(
            car_cost_of_time=car_cost_of_time,
            carpooling_cost_of_time=carpool_cost_of_time,
            car_cost_of_distance=car_cost_of_distance,
            carpooling_cost_of_distance=0.5*car_cost_of_distance,
            car_cost_constant=car_cost_constant,
            carpooling_cost_constant=constants["carpool"]
        )
    )

    modes = {
        "walk": walk,
        "bicycle": bicycle,
        "car": car,
        "walk_pt": walk_pt,
        "bicycle_pt": bicycle_pt,
        "car_pt": car_pt,
        "carpool": carpool
    }
    
    return modes
