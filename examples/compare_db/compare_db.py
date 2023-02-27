import sys
sys.path.append("...")
import pandas as pd
import plotly.express as px
from mobility.get_survey_data import get_survey_data
import plotly.graph_objects as go


# ------------------------
# UTILS
# ------------------------

# Colors dict for charts
color_motive = {
    "Commerce": "rgb(236, 0, 141)",
    "Domicile": "rgb(11, 28, 45)",
    "Enseignement": "rgb(0, 164, 174)",
    "Médical": "rgb(139, 180, 186)",
    "Sport / loisirs / culture": "rgb(91, 134, 145)",
    "Travail": "rgb(15, 96, 126)",
    "Vacances": "rgb(159, 197, 54)",
    "Visites extérieures": "rgb(240, 98, 146)",

}
color_mode = {
    "Piéton": "rgb(159, 197, 54)",
    "Deux roues": "rgb(15, 96, 126)",
    "Automobile": "rgb(11, 28, 45)",
    "Transport spécialisé, scolaire, taxi": "rgb(139, 180, 186)",
    "Transport en commun urbain ou régional": "rgb(236, 0, 141)",
    "Autre": "rgb(0, 164, 174)",
    "Avion": "rgb(91, 134, 145)",
    "Train grande ligne ou TGV": "rgb(207, 216, 220)",
    "Bateau": "rgb(240, 98, 146)",
}
color_survey = {
    "ENTD-2008":"rgb(236, 0, 141)",
    "EMP-2019": "rgb(11, 28, 45)"
    }
variable_titles = {
    "city_category": "catégories urbaines",
    "csp": "csp",
    "motive_group": "motifs de déplacements",
    "mode_group": "modes de déplacements",
    "day_type": "jours de la semaine"
    }
survey_year = pd.DataFrame({
    "survey": ["ENTD-2008", "EMP-2019"],
    "year": [2008, 2019]
    })


# Load trips motives and modes
# TODO : atm excel files in same folder => load from other source or put in other folder ?
motive_group = pd.read_excel("./entd_location_motive_group.xlsx", engine='openpyxl', dtype=str)
motive_group.columns = ["motive_group", "motive", "location", "motive_explication"]
mode_transport = pd.read_excel("./entd_mode.xlsx", engine='openpyxl', dtype=str)



# ------------------------
# FUNCTIONS
# ------------------------

def trips_share(trips, var="trip_id"):
    """
    

    Parameters
    ----------
    trips : DataFrame
        Concat trips dataframe of different surveys
    var : str, optional
        Show data by number of trips in the database ("trip_id") or weighted ("pondki"). 
        The default is "trip_id".

    Returns
    -------
    None.
    Display graphics showing 
    trips share (by trips or distance) 
    depending on modes, motives, csp, urban category

    """

    
    # Add motive and mode groups 
    trips = pd.merge(trips, motive_group, on="motive", how='left')
    trips = pd.merge(trips, mode_transport, on="mode_id", how='left')
    trips["trip_id"] = "1"
    trips_source = trips.copy()
    

    # For each variable, create graphs to show
    # 1. Trips share depending on variable
    # 2. Number of trips depending on variable
    # 3. Average distance per trip
    for variable in ["city_category", "csp", "motive_group", "mode_group"]:
        trips = trips_source.copy()
        if var =="trip_id":
            action = "count"
            add_str = ""
        elif var == "pondki":
            trips["distance"] = trips["distance"] * trips["pondki"]
            action = "sum"
            add_str = " -avec pondération"
        trips = trips.groupby(["survey", variable], as_index=False).agg(
            {"distance": ['sum'], var: [action]})
        trips.columns = ["survey", variable, "distance", "n_trips"]
        trips["distance_per_trip"] = trips["distance"] / trips["n_trips"]
        for i in trips["survey"].unique().tolist():
            average_dist = trips.loc[trips["survey"]==i, "distance"].sum()/trips.loc[trips["survey"]==i, "n_trips"].sum()
            print("La distance moyenne dans la bdd " + i + " est de " + str(int(average_dist)) + " km." + add_str)
        trips = pd.merge(trips,
                         trips.groupby(["survey"], as_index=False)["n_trips"].sum().rename(columns={"n_trips":"n_trips_tot"}),
                         on="survey",
                         how="left")
        trips["ratio"] = trips["n_trips"] / trips["n_trips_tot"]
        
        # Sort values by survey year
        trips = pd.merge(trips, survey_year,
                         on="survey", how="left")
        trips = trips.sort_values(by=["year", variable])
        
        # Components for charts
        fig_list = []
        fig_list_2 = []
        fig_list_3 = []
        n = 0
        for i in trips["survey"].unique().tolist():
            fig_list.append(
                go.Bar(name=i, 
                       x=trips.loc[trips["survey"]==i, variable].tolist(),
                       y=trips.loc[trips["survey"]==i, "ratio"].tolist(),
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            fig_list_2.append(
                go.Bar(name=i,
                       x=trips.loc[trips["survey"]==i, variable].tolist(),
                       y=trips.loc[trips["survey"]==i, "n_trips"].tolist(), 
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            fig_list_3.append(
                go.Bar(name=i, 
                       x=trips.loc[trips["survey"]==i, variable].tolist(),
                       y=trips.loc[trips["survey"]==i, "distance_per_trip"].tolist(),
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            n +=1
        
        
        # FIGURE 1
        # Trips share depending on variable
        fig = go.Figure(

            data=fig_list,
            
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Répartition des trajets en fonction des " + variable_titles[variable] + add_str, 
            title_x=0.5,
            yaxis_title ="",
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        
        # FIGURE 2
        # Number of trips depending on variable
        fig = go.Figure(

            data=fig_list_2,

        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Nombre de trajets en fonction des " + variable_titles[variable] + add_str, 
            title_x=0.5,
            yaxis_title="",
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        
        # FIGURE 3
        # Average distance per trip depending on variable
        fig = go.Figure(

            data=fig_list_3,

        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distance par trajet en fonction des " + variable_titles[variable] + add_str, 
            title_x=0.5,
            yaxis_title="Distance (km)"
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        


def car_occupancy_rate(trips):
    """
    

    Parameters
    ----------
    trips : DataFrame
        Concat trips dataframe of different surveys

    Returns
    -------
    None.
    Display graphics showing 
    cars passengers numbers 
    depending on modes, motives, csp, urban category

    """


    # Add motive and mode groups 
    trips = pd.merge(trips, motive_group, on="motive", how='left')
    trips = pd.merge(trips, mode_transport, on="mode_id", how='left')
    trips["trip_id"] = "1"
    

    # Keep only trips in cars
    trips = trips.loc[trips["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]
    
    trips_source = trips.copy()
    for variable in ["city_category", "csp", "motive_group"]:
        trips = trips_source.copy()
        # Compute average number of passengers per trip
        trips["n_other_passengers"] = trips["n_other_passengers"].astype(int)
        trips = trips.loc[trips["n_other_passengers"]<10]
        trips["n_passengers"] = trips["n_other_passengers"].astype(int) + 1
        trips["n_passengers"] = trips["n_passengers"] * trips["pondki"]
        trips = trips.groupby(["survey", variable], as_index=False).agg(
            {"n_passengers": ['sum'], "pondki": ['sum'], "trip_id": ["count"]})
        trips["n_passengers"] = trips["n_passengers"] / trips["pondki"]
        trips.columns = ["survey", variable, "n_passengers", "pondki", "trip_id"]
        
        # Sort values by survey year
        trips = pd.merge(trips, survey_year,
                          on="survey", how="left")
        trips = trips.sort_values(by=["year", variable])
        
        # Components for charts
        fig_list = []
        n = 0
        for i in trips["survey"].unique().tolist():
            fig_list.append(
                go.Bar(name=i, 
                       x=trips.loc[trips["survey"]==i, variable].tolist(),
                       y=trips.loc[trips["survey"]==i, "n_passengers"].tolist(),
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            n +=1
        
        # FIGURE
        # Average passengers by variable
        fig = go.Figure(
    
            data=fig_list,
            
        )
        fig.update_layout(
    
            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Taux de remplissage moyen des voitures par "+ variable_titles[variable], 
            title_x=0.5,
            yaxis_title = 'Nb personnes par voiture'
        )
    
        fig.update_layout(barmode='group')
        fig.show(renderer="png")
    


def distance_trip_share(trips):
    """
    

    Parameters
    ----------
    trips : DataFrame
        Concat trips dataframe of different surveys

    Returns
    -------
    None.
    Display graphics showing :
        trips share by distance
        trips share by number of trips
    depending on modes and motives

    """

    # Add motive and mode groups 
    trips = pd.merge(trips, motive_group, on="motive", how='left')
    trips = pd.merge(trips, mode_transport, on="mode_id", how='left')
    trips["trip_id"] = "1"
    
    for variable in ["motive_group", "mode_group"]: 
        # Compute total weighted distance by mode
        distance = trips.copy()
        distance["distance"] = distance["distance"] * distance["pondki"]
        distance = pd.merge(
            distance.groupby(["survey", variable], as_index=False)["distance", "pondki"].sum(),
            distance.groupby(["survey"], as_index=False)["distance", "pondki"].sum().rename(columns={"distance":"total_distance", "pondki": "total_pondki"}),
            how="left", on="survey"
            )
        # Compute share by mode (in distance)
        distance["p_distance"] = distance["distance"] / distance["total_distance"] 
        distance["p_distance"] = round(distance["p_distance"], 2)
        # Compute share by modes (in trips number)
        distance["p_trip"] = distance["pondki"] / distance["total_pondki"]
        distance["p_trip"] = round(distance["p_trip"], 2)
        
        # Sort values by survey year
        distance = pd.merge(distance, survey_year,
                          on="survey", how="left")
        distance = distance.sort_values(by=["year", variable])
        
        # Color palette
        if variable == "mode_group":
            color_graph = color_mode
        elif variable == "motive_group":
            color_graph = color_motive
    
        # FIGURE 1
        # Trips share by distance depending on variable
        fig = px.bar(
            distance,
            x="survey",
            y="p_distance", 
            color=variable,
            text_auto=True,
            color_discrete_map=color_graph
        )
        fig.update_layout(
            uniformtext_minsize=10,
            uniformtext_mode='hide',
            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title="Répartition des distances parcourues en fonction des "+ variable_titles[variable],
            xaxis={'title': ''},
            yaxis={'title': ''},
            legend={'title': '', 'traceorder': 'reversed'},
    
        )
        fig.show(renderer="png")
        
        # FIGURE 2
        # Trips share by number of trips depending on variable
        fig = px.bar(
            distance, 
            x="survey",
            y="p_trip",
            color=variable, 
            text_auto=True,
            color_discrete_map=color_graph
        )
        fig.update_layout(
    
            font_family="Arial",
            uniformtext_minsize=10,
            uniformtext_mode='hide',
            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title={
                'text': "Répartition des trajets en fonction des "+ variable_titles[variable],
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            xaxis={'title': ''},
            yaxis={'title': ''},
            legend={'title': '', 'traceorder': 'reversed'},
    
        )
    
        fig.show(renderer="png")


def daily_distance(trips, filter_db=None):
    """
    

    Parameters
    ----------
    trips : DataFrame
        Concat trips dataframe of different surveys
        for SHORT TRIPS only
    filter_db : dict, optionnal
        None by default (No filter)    
        Contains filter on variables (csp, city_category ..)
        ex : {"city_category":"R"}

    Returns
    -------
    None.
    Display graphics showing :
        daily distance by csp, city category and day type
        daily distance by mode and motives
        for a day, a weekday, a weekend

    """

    if "weekday" in trips.columns:
        # Add motive and mode groups 
        trips["day_id"] = trips.index
        trips = pd.merge(trips, motive_group, on="motive", how='left')
        trips = pd.merge(trips, mode_transport, on="mode_id", how='left')
        trips["day_type"] = "jour de week-end"
        trips.loc[trips["weekday"], "day_type"] = "jour de semaine"
        
        # Filter database
        filter_str = ""
        if filter_db is not None:
            for key in list(filter_db.keys()):
                trips = trips.loc[trips[key]==filter_db[key]]
                filter_str += "<br>[" + key + " : " + str(filter_db[key]) +"]"
            
        
        # Chart showing average daily distance (km) depending on csp, city category, day type
        # for all surveys
        for variable in ["csp", "city_category", "day_type"]: 
            st = trips.copy()
            tot_indiv = st.groupby(["survey", "individual_id", "pondki", variable], as_index=False)["day_id"].nunique()
            tot_indiv["pondki"] = tot_indiv["pondki"] * tot_indiv["day_id"]
            tot_indiv = tot_indiv.groupby(["survey", variable], as_index=False)["pondki"].sum()
            st["distance"] = st["distance"] * st["pondki"]
            st = st.groupby(["survey", variable], as_index=False)["distance"].sum()
            st = pd.merge(st, tot_indiv.rename(columns={"pondki": "total_pondki"}),
                          how="left", on=["survey", variable])
            st["distance"] = st["distance"] / st["total_pondki"]
            st["rd_distance"] = round(st["distance"], 1)
            
            
            # Sort values by survey year
            st = pd.merge(st, survey_year,
                          on="survey", how="left")
            st = st.sort_values(by=["year", variable])
            
            # Components for charts
            fig_list = []
            n = 0
            for i in st["survey"].unique().tolist():
                fig_list.append(
                    go.Bar(name=i, 
                           x=st.loc[st["survey"]==i, variable].tolist(),
                           y=st.loc[st["survey"]==i, "distance"].tolist(),
                           yaxis='y', offsetgroup=n,
                           marker=dict(color=color_survey[i]))
                    )
                n +=1
            
            # FIGURE 1
            # Daily distance for short trips
            fig = go.Figure(
    
                data=fig_list,
    
            )
            fig.update_layout(
    
                template="simple_white",
                font=dict(family="Gilroy Light", size=12),
                title_text="Distance journalière en fonction des "+ variable_titles[variable] + filter_str,
                title_x=0.5,
                yaxis={'title': 'Distance (km/jour)'}
            )
    
            fig.update_layout(barmode='group')
            fig.show(renderer="png")
            
        # Chart showing repartition of daily distance (in modes or motives)
        # for all surveys 
        # for all days, for week day and for weekend
        for day_type in ["all", "weekday", "weekend"]:
            if day_type =="all":
                add_str = ""
                trips_df = trips.copy()
            elif day_type == "weekday":
                add_str = " - jour de semaine"
                trips_df = trips.loc[trips["weekday"]].copy()
            elif day_type == "weekend":
                add_str = " - jour de week-end"
                trips_df = trips.loc[~trips["weekday"]].copy()
            
            for variable in ["motive_group", "mode_group"]: 
                st = trips_df.copy()
                tot_indiv = st.groupby(["survey", "individual_id", "pondki"], as_index=False)["day_id"].nunique()
                tot_indiv["pondki"] = tot_indiv["pondki"] * tot_indiv["day_id"]
                tot_indiv = tot_indiv.groupby(["survey"], as_index=False)["pondki"].sum()
                st["distance"] = st["distance"] * st["pondki"]
                st = st.groupby(["survey", variable], as_index=False)["distance"].sum()
                st = pd.merge(st, tot_indiv.rename(columns={"pondki": "total_pondki"}),
                              how="left", on="survey")
                st["distance"] = st["distance"] / st["total_pondki"]
                st["rd_distance"] = round(st["distance"], 1)
                
                
                
                
                # Sort values by survey year
                st = pd.merge(st, survey_year,
                              on="survey", how="left")
                st = st.sort_values(by=["year", variable])
                
                # Color palette
                if variable == "mode_group":
                    color_graph = color_mode
                elif variable == "motive_group":
                    color_graph = color_motive
                
                # FIGURE 2
                # Daily distance [km/day] depending on mode or motive
                fig = px.bar(st, 
                             x="survey", y="rd_distance", 
                             color=variable, 
                             text_auto=True,
                             color_discrete_map=color_graph)
                fig.update_layout(
                    uniformtext_minsize=10,
                    uniformtext_mode='hide',
                    template="simple_white",
                    title={
                        'text': "Distance moyenne journalière en fonction des " + variable_titles[variable] + add_str + filter_str,
                        'y': 0.95,
                        'x': 0.5,
                        'xanchor': 'center',
                        'yanchor': 'top'},
                    title_font=dict(size=15),
                    font_family="Arial",
                    xaxis={'title': ''},
                    yaxis={'title': 'Distance (km)'},
                    legend={'title': '', 'traceorder': 'reversed'},
            
                )
                fig.show(renderer="png")
        

        
    else:
        print("La table renseignée n'est pas celle des déplacements quotidiens")
    


def modes_by_motive(trips):
    """
    

    Parameters
    ----------
    trips : DataFrame
        Concat trips dataframe of different surveys

    Returns
    -------
    None.
    Display graphics showing :
        transportation modes by motives 
        in trips number and in distance

    """    
    # Add motive and mode groups 
    trips = pd.merge(trips, motive_group, on="motive", how='left')
    trips = pd.merge(trips, mode_transport, on="mode_id", how='left')
    trips["trip_id"] = 1
    
    for motive in trips["motive_group"].unique().tolist():
        df = trips.loc[trips["motive_group"] == motive]
        df = df.groupby(["survey", "mode_group"], as_index=False)["trip_id", "distance"].sum()
        df.columns = ["survey", "mode_group", "n_trips", "distance"]
        
        
        for survey in df["survey"].unique().tolist():
            df_survey = df.loc[df["survey"] == survey]
            for var in ["n_trips", "distance"]:
                df_trip = df_survey.copy()
                df_trip["p_var"] = df_trip[var] / df_trip[var].sum()
                df_trip.loc[df_trip["p_var"]<0.02, "mode_group"] = "Autre"
                df_trip = df_trip.groupby(["mode_group"], as_index=False)["p_var"].sum()
                
                if var =="n_trips":
                    add_str = "nombre de déplacements"
                elif var == "distance":
                    add_str = "distance parcourue"
                
                # FIGURE
                # Transportation modes depending on motives
                
                fig = px.pie(df_trip,
                             values="p_var",
                             names="mode_group", 
                             color="mode_group", 
                             title= motive + " (par " + add_str + ") - " + survey,
                             color_discrete_map=color_mode)

                fig.update_layout(

                    template="simple_white",
                    title={

                        'y': 0.95,
                        'x': 0.5,
                        'xanchor': 'center',
                        'yanchor': 'top'},
                    title_font=dict(size=15),
                    font_family="Arial",
                    uniformtext_mode='hide'
                )

                fig.show(renderer="png")


def carbon_and_distance(trips):
    """
    

    Parameters
    ----------
    trips : DataFrame
        Concat trips dataframe of different surveys

    Returns
    -------
    None.
    Display graphic showing :
        average distance and CO2e emissions
        of a trip by motive

    """
    # Add motive and mode groups 
    trips = pd.merge(trips, motive_group, on="motive", how='left')
    trips = pd.merge(trips, mode_transport, on="mode_id", how='left')
    trips["trip_id"] = 1
    
    trips["distance"] = trips["distance"] * trips["pondki"]
    
    # ------------
    # TODO : change for carbon function when pushed to main
    # Add ef by mode 
    import numpy as np
    mode_ef = pd.read_csv("./mode_ef.csv", dtype=str)
    trips = pd.merge(trips, mode_ef, on="mode_id", how='left')
    k_ef_car = 1 / (1 + trips["n_other_passengers"])
    trips["k_ef"] = np.where(trips["mode_id"].str.slice(0, 1) == "3", k_ef_car, 1.0)
    trips["emissions"] = trips["ef"].astype(float) * trips["distance"] * trips["k_ef"]
    # ------------
    
    trips = trips.groupby(["survey", "motive_group"], as_index=False).agg({"distance": ['sum'], "emissions": ['sum'], "pondki": ['sum']})
    trips.columns = ["survey", "motive_group", "distance", "emissions", "pondki"]
    
    trips["distance"] = trips["distance"] / trips["pondki"]
    trips["emissions"] = trips["emissions"] / trips["pondki"]
    trips["distance"] = round(trips["distance"], 2)
    trips["distance"] = round(trips["distance"], 2)
    
    trips_source = trips.copy()
    for survey in trips["survey"].unique().tolist():
        trips = trips_source.loc[trips_source["survey"] == survey]
        trips = trips.sort_values(by="emissions")
    
        fig = go.Figure(
    
            data=[
                go.Bar(name='Emissions', 
                       x=trips["motive_group"].tolist(), 
                       y=trips["emissions"].tolist(),
                       yaxis='y',
                       offsetgroup=1, 
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='Distance',
                       x=trips["motive_group"].tolist(),
                       y=trips["distance"].tolist(), yaxis='y2', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],
            layout={
                'yaxis': {'title': 'Emissions (kgCO<sub>2</sub>e)'},
                'yaxis2': {'title': 'Distances (km)', 'overlaying': 'y', 'side': 'right'}
            }
        )
        fig.update_layout(
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distances et émissions moyennes par déplacement <br>en fonction du motif de déplacement - " + survey,
            title_x=0.5
        )
    
        fig.update_layout(barmode='group')
        fig.show(renderer="png")
    

            



def get_ls_trips(survey):
    """
    

    Parameters
    ----------
    survey : str
        Survey name
        possibilities : [ENTD-2008; EMP-2019].

    Returns
    -------
    st : DataFrame
        short trips database
    lt : DataFrame
        long trips database

    """
    # Load long and short trips
    data = get_survey_data(survey)
    lt = data["long_trips"]
    st = data["short_trips"]
    
    # Add survey name in a column
    lt["survey"] = survey
    st["survey"] = survey
    return st, lt




# ------------------------
# MAIN
# ------------------------

# ------------------------
# Load trips in each database
short_trips_2008, long_trips_2008 = get_ls_trips("ENTD-2008")
short_trips_2019, long_trips_2019 = get_ls_trips("EMP-2019")

# ------------------------
# Long trips
long_trips = pd.concat([
    long_trips_2008,
    long_trips_2019,
    ])

# Run functions
trips_share(long_trips)
trips_share(long_trips, var="pondki")
car_occupancy_rate(long_trips)
distance_trip_share(long_trips)

# ------------------------
# Short trips
short_trips = pd.concat([
    short_trips_2008,
    short_trips_2019,
    ])

# Run functions
trips_share(short_trips)
car_occupancy_rate(short_trips)
distance_trip_share(short_trips)
daily_distance(short_trips)
daily_distance(short_trips, {"city_category":"R"})
daily_distance(short_trips, {"city_category":"C"})
modes_by_motive(short_trips)
carbon_and_distance(short_trips)
