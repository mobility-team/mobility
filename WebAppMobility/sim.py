import mobility
import pandas as pd
mobility.set_params()

# -----------------------------------------------------------------------------
# Transport modes

transport_zones = mobility.TransportZones(
    local_admin_unit_id="fr-21231",
    radius=40,
    level_of_detail=0
)
tz = transport_zones.get()

# -----------------------------------------------------------------------------
# Transport modes

car = mobility.CarMode(
    transport_zones=transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=0.1
    )
)

bicycle = mobility.BicycleMode(
    transport_zones=transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=0.0
    )
)

modes = [
    car,
    bicycle
]

# -----------------------------------------------------------------------------
# Work destination and mode choice models

work_choice_model = mobility.WorkDestinationChoiceModel(
    transport_zones,
    modes=modes
)

mode_choice_model = mobility.TransportModeChoiceModel(
    destination_choice_model=work_choice_model
)

work_choice_model.get()
mode_choice_model.get()

mc = mode_choice_model.get()

# -----------------------------------------------------------------------------
# Comparing to reference data

comparison = work_choice_model.get_comparison()

work_choice_model.plot_model_fit(comparison)

work_choice_model.compute_ssi(comparison, 200)
work_choice_model.compute_ssi(comparison, 400)
work_choice_model.compute_ssi(comparison, 1000)

# -----------------------------------------------------------------------------
# Extracting metrics

# Average travel time by origin
car_travel_costs = car.travel_costs.get()
car_travel_costs["mode"] = "car"

bicycle_travel_costs = bicycle.travel_costs.get()
bicycle_travel_costs["mode"] = "bicycle"

travel_costs = pd.concat([
    car_travel_costs,
    bicycle_travel_costs]
)


# -----------------------------------------------------------------------------
# Sample trips

population = mobility.Population(
    transport_zones=transport_zones,
    sample_size=1000
)

# Raw trips directly sampled from survey data
trips = mobility.Trips(population)

# Localized trips taking the local choice models into account
loc_trips = mobility.LocalizedTrips(
    trips=trips,
    work_dest_cm=work_choice_model,
    mode_cm=mode_choice_model
)

trips.get()
df_trips = loc_trips.get()
