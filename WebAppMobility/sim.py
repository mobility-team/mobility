# -----------------------------------------------------------------------------
# Set up

import mobility
import pandas as pd

mobility.set_params(debug=False)


df_municipality = pd.read_csv('data\donneesCommunesFrance.csv')
df_county = pd.read_csv('data/departements-france.csv')






def simulation_launch(n_clicks, current_tab,input_radius_municipality, input_radius_value, input_county, input_municipality, input_municipality_value, input_transport_means, input_csp):
    if current_tab == "tab-rayon" :
        
        #local_admin_unit_id = ['fr-' + df_municipality.loc[df_municipality["NOM_COM"]==i, "INSEE_COM"].iloc[0] for i in input_radius_municipality]
        
        
        
        local_admin_unit_id = 'fr-' + df_municipality.loc[df_municipality["NOM_COM"]==input_radius_municipality, "INSEE_COM"].iloc[0]
        
        print('liste communes ', local_admin_unit_id)
        print('rayon', float(input_radius_value))
        
        """transport_zones = mobility.TransportZones(
            local_admin_unit_id=local_admin_unit_id,
            radius= float(input_radius_value),
            level_of_detail=0
            )
        transport_zones.get().plot()"""
        
        
    if current_tab == "tab-municipality":
        return f"Ville d'origine choisie : {input_municipality_value} Liste des villes choisies : {input_municipality}"
    
    if current_tab == "tab-county":
        return f"Liste des départments : {input_county}"




# Prepare transport zones
transport_zones = mobility.TransportZones("fr-21231", level_of_detail=1, radius=10)

# Choice model params
work_dest_parms = mobility.WorkDestinationChoiceModelParameters(
    model={
        "type": "radiation",
        "lambda": 0.99986,
        "end_of_contract_rate": 0.00,
        "job_change_utility_constant": -5.0,
        "max_iterations": 6,
        "tolerance": 0.01,
        "cost_update": True,
        "n_iter_cost_update": 3
    },
    utility={
        "fr": 0.0,
        "ch": 5.0
    }
)

# Mode constants
constants = {
    "walk": 0.0,
    "bicycle": 2.0,
    "public_transport": 0.0,
    "car": 1.0,
    "carpool": 0.0
}



walk = mobility.WalkMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=constants["walk"]))


car = mobility.CarMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=constants["car"]))


bicycle = mobility.BicycleMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=constants["bicycle"]))


public_transport = mobility.PublicTransportMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=constants["public_transport"]))








