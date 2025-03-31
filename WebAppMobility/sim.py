# -----------------------------------------------------------------------------
# Set up

import mobility
import pandas as pd

mobility.set_params(debug=False)


df_municipality = pd.read_csv('data\donneesCommunesFrance.csv')
df_county = pd.read_csv('data/departements-france.csv')






def simulation_launch(n_clicks, current_tab,input_radius_municipality, input_radius_value, input_county, input_municipality, input_municipality_value, input_transport_means, input_csp):
    if current_tab == "tab-rayon" :
        
        local_admin_unit_id = ['fr-' + df_municipality.loc[df_municipality["NOM_COM"]==i, "INSEE_COM"].iloc[0] for i in input_radius_municipality]
        
        transport_zones = mobility.TransportZones(
            local_admin_unit_id=local_admin_unit_id,
            radius= float(input_radius_value),
            level_of_detail=0
            )
        transport_zones.get().plot()
        
        
    if current_tab == "tab-municipality":
        return f"Ville d'origine choisie : {input_municipality_value} Liste des villes choisies : {input_municipality}"
    
    if current_tab == "tab-county":
        return f"Liste des départments : {input_county}"





"""
# -----------------------------------------------------------------------------
# Transport modes

transport_zones = mobility.TransportZones(
    local_admin_unit_id="fr-21231",
    radius=40,
    level_of_detail=0
)
transport_zones.get().plot()

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


# -----------------------------------------------------------------------------
# Comparing to reference data

comparison = work_choice_model.get_comparison()

work_choice_model.plot_model_fit(comparison)

work_choice_model.compute_ssi(comparison, 200)
work_choice_model.compute_ssi(comparison, 400)
work_choice_model.compute_ssi(comparison, 1000)

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
loc_trips.get()"""









