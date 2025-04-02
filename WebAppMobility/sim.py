import mobility
import pandas as pd
import matplotlib.pyplot as plt
mobility.set_params()
import geopandas as gpd



def compute_by_radius(local_admin_unit_id, radius):
    
    # local_admin_unit_id="fr-21231"
    print('Transport zone --------------------------------')
    # -----------------------------------------------------------------------------
    # Transport modes
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=local_admin_unit_id,
        radius=radius,
        level_of_detail=0
    )
    tz = transport_zones.get()
    
    # -----------------------------------------------------------------------------
    # Transport modes
    print('Transport modes ---------------------------------')
    car = mobility.CarMode(
        transport_zones=transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_of_distance=6
        )
    )
    
    bicycle = mobility.BicycleMode(
        transport_zones=transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_of_distance=5
        )
    )
    
    walk = mobility.WalkMode(
        transport_zones=transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_of_distance=-1
        )
    )
    
    
    
    modes = [
        car,
        bicycle,
        walk
    ]
    
    # -----------------------------------------------------------------------------
    # Work destination and mode choice models
    print('Work model --------------------------------------')
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
    
    # comparison = work_choice_model.get_comparison()
    
    # work_choice_model.plot_model_fit(comparison)
    
    # work_choice_model.compute_ssi(comparison, 200)
    # work_choice_model.compute_ssi(comparison, 400)
    # work_choice_model.compute_ssi(comparison, 1000)
    
    # -----------------------------------------------------------------------------
    # Extracting metrics
    
    # Average travel time by origin
    car_travel_costs = car.travel_costs.get()
    car_travel_costs["mode"] = "car"
    
    bicycle_travel_costs = bicycle.travel_costs.get()
    bicycle_travel_costs["mode"] = "bicycle"
    
    walk_travel_costs = bicycle.travel_costs.get()
    walk_travel_costs["mode"] = "walk"
    
   
    travel_costs = pd.concat([
        car_travel_costs,
        bicycle_travel_costs,
        walk_travel_costs]
    )
    
    
    # -----------------------------------------------------------------------------
    # Sample trips
    print('Sample trips -----------------------------')
    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=1000
    )
    
    print('Raw trips')
    # Raw trips directly sampled from survey data
    trips = mobility.Trips(population)
    
    print('Localized trips')
    # Localized trips taking the local choice models into account
    loc_trips = mobility.LocalizedTrips(
        trips=trips,
        work_dest_cm=work_choice_model,
        mode_cm=mode_choice_model
    )
    
    print('Trips get -------------------------')
    df_trips = trips.get()
    
    print('Loc trips ---------------------------------')
    df_loc_trips = loc_trips.get()
    

    print('Traitement de dataframe ---------------------------')
    clean_loc_trip = df_loc_trips.dropna()
    print('A')
    local_id = tz.loc[tz["local_admin_unit_id"] == local_admin_unit_id, "transport_zone_id"].values[0]
    
    print("B")
    id_from_selected = clean_loc_trip.loc[clean_loc_trip["from_transport_zone_id"] == local_id, ["to_transport_zone_id", "mode_id"]]
    
    print("C")
    df_grouped_by_zone = id_from_selected.value_counts().reset_index(name='Count')
    print("D")
    df_with_geometry = df_grouped_by_zone.merge(tz[['local_admin_unit_id', 'transport_zone_id', 'geometry']], left_on='to_transport_zone_id', right_on='transport_zone_id', how='left')
    df_with_geometry.reset_index(drop=True)
    
    print("E")
    df_mode_max = df_with_geometry.loc[df_with_geometry.groupby('transport_zone_id')['Count'].idxmax()]
    
    
    
    print('Plotting . . . ------------------------------------')
    geo_df = gpd.GeoDataFrame(df_mode_max, geometry = 'geometry', crs="EPSG:2154")
    
    fig, ax = plt.subplots(figsize=(8, 8))
    geo_df.plot(ax=ax, column='mode_id', cmap='viridis', legend=True)  # Remplace 'valeur' par ta colonne
    plt.axis("off")  # Cache les axes
    
    print("Sauvegarde de l'image")
    plt.savefig("assets/map.png", bbox_inches="tight")  # Sauvegarde dans le dossier 'assets' pour Dash
    plt.close(fig)  # Ferme la figure pour éviter les doublons
    
    

    
    
    
    
    
    
    
    
    
    
