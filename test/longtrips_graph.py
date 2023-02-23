import sys
sys.path.append("..")
import pandas as pd
import plotly.express as px
from mobility.get_survey_data import get_survey_data
import plotly.graph_objects as go

# TODO [AH]: add short trips here : all in the same function
# TODO [AH]: instead of having 2 dataframes with 2008 and 2019
#            => concat them at the beginning after adding column "year" to it
# TODO [AH] : all in english if possible + adapt description
# TODO [AH] : pass "a" as a variable of your code and add dict
#            with titles, legend, colors depending on a and
#            rename a


# ---------------------
# Colors dict for charts
color_mode = {
    "Piéton": "rgb(124, 179, 66)",
    "Deux roues": "rgb(0, 184, 212)",
    "Automobile": "rgb(26, 35, 126)",
    "Transport spécialisé, scolaire, taxi": "rgb(144, 164, 174)",
    "Transport en commun urbain ou régional": "rgb(245, 0, 87)",
    "Autre": "rgb(77, 182, 172)",
    "Avion": "rgb(55, 71, 79)",
    "Train grande ligne ou TGV": "rgb(207, 216, 220)",
    "Bateau": "rgb(240, 98, 146)",
}
color_survey = {
    "ENTD-2008":"rgb(236, 0, 141)",
    "EMP-2019": "rgb(11, 28, 45)"
    }
variable_titles = {
    "city_category": "des catégories urbaines",
    "csp": "des csp",
    "motive_group": "des motifs de déplacements",
    "mode_group": "des modes de déplacements"
    }


# ---------------------
# Load trips motives and modes
# TODO : atm excel files in same folder => load from other source or put in other folder ?
motive_group = pd.read_excel("./entd_location_motive_group.xlsx", engine='openpyxl', dtype=str)
motive_group.columns = ["motive_group", "motive", "location", "motive_explication"]

mode_transport = pd.read_excel("./entd_mode.xlsx", engine='openpyxl', dtype=str)

# Load long trips in each database
survey_data_2019 = get_survey_data("EMP-2019")
long_trips_2019 = survey_data_2019["long_trips"]

survey_data_2008 = get_survey_data("ENTD-2008")
long_trips_2008 = survey_data_2008["long_trips"]

# # Load long trips for each database
# long_trips_2019 = survey_data_2019["long_trips"]
# long_trips_2008 = survey_data_2008["long_trips"]
# Add survey
long_trips_2019["survey"] = "EMP-2019"
long_trips_2008["survey"] = "ENTD-2008"
# Concat dataframes
long_trips = pd.concat([
    long_trips_2019,
    long_trips_2008
    ])



# ---------------------
def trips_share(long_trips):
    """
    Trips share by either :
    - urban categories,
    - social categories (csp),
    - motives,
    - transportation modes

    Parameters
    ----------
    a : str
        DESCRIPTION. "urban_categories", "csp", "motives", ou "mode"


    Returns
    -------
    None. Create several graphs to compare 2008 and 2019 db.

    """

    
    # Add motive and mode groups 
    long_trips = pd.merge(long_trips, motive_group, on="motive", how='left')
    long_trips = pd.merge(long_trips, mode_transport, on="mode_id", how='left')
    long_trips["trip_id"] = "1"
    long_trips_source = long_trips.copy()
    

    # For each variable, create graphs to show
    # 1. Trips share depending on variable
    # 2. Number of trips depending on variable
    # 3. Average distance per trip
    for variable in ["city_category", "csp", "motive_group", "mode_group"]:
        long_trips = long_trips_source.groupby(["survey", variable], as_index=False).agg(
            {"distance": ['sum'], "trip_id": ["count"]})
        long_trips.columns = ["survey", variable, "distance", "n_trips"]
        long_trips["distance_per_trip"] = long_trips["distance"] / long_trips["n_trips"]
        long_trips = pd.merge(long_trips,
                              long_trips.groupby(["survey"], as_index=False)["n_trips"].sum().rename(columns={"n_trips":"n_trips_tot"}),
                              on="survey",
                              how="left")
        long_trips["ratio"] = long_trips["n_trips"] / long_trips["n_trips_tot"]
        
        
        # Components for charts
        fig_list = []
        fig_list_2 = []
        fig_list_3 = []
        n = 0
        for i in long_trips["survey"].unique().tolist():
            fig_list.append(
                go.Bar(name=i, 
                       x=long_trips.loc[long_trips["survey"]==i, variable].tolist(),
                       y=long_trips.loc[long_trips["survey"]==i, "ratio"].tolist(),
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            fig_list_2.append(
                go.Bar(name=i,
                       x=long_trips.loc[long_trips["survey"]==i, variable].tolist(),
                       y=long_trips.loc[long_trips["survey"]==i, "n_trips"].tolist(), 
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            fig_list_3.append(
                go.Bar(name=i, 
                       x=long_trips.loc[long_trips["survey"]==i, variable].tolist(),
                       y=long_trips.loc[long_trips["survey"]==i, "distance_per_trip"].tolist(),
                       yaxis='y', offsetgroup=n,
                       marker=dict(color=color_survey[i]))
                )
            n +=1
        
        
        # FIGURE 1
        # Trips share depending on urban category
        fig = go.Figure(

            data=fig_list,
            
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Répartition des trajets en fonction " + variable_titles[variable], 
            title_x=0.5,
            yaxis_title ="",
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        
        # FIGURE 2
        # Number of trips depending on urban category
        fig = go.Figure(

            data=fig_list_2,

        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Nombre de trajets en fonction " + variable_titles[variable], 
            title_x=0.5,
            yaxis_title="",
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        
        # FIGURE 3
        # Average distance per trip
        fig = go.Figure(

            data=fig_list_3,

        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distance par trajet en fonction " + variable_titles[variable], 
            title_x=0.5,
            yaxis_title="Distance (km)"
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        
       
# TODO : reprendre ICI !

def taux_remplissage(long_trips):
    """
    Taux de remplissage moyen des voitures en fonction des motifs de déplacement

    Returns
    -------
    None.

    """

    long_trips_2019 = survey_data_2019["long_trips"]
    long_trips_2019["individual_id"] = "1"
    long_trips_2019 = pd.merge(long_trips_2019, motive_group, on="motive", how='left')
    long_trips_2008 = survey_data_2008["long_trips"]
    long_trips_2008["individual_id"] = "1"
    long_trips_2008 = pd.merge(long_trips_2008, motive_group, on="motive", how='left')

    # on regarde uniquement les individus possédant une voiture
    long_trips_2019 = long_trips_2019.loc[long_trips_2019["n_cars"] != "0"]
    long_trips_2008 = long_trips_2008.loc[long_trips_2008["n_cars"] != "0"]

    # on regarde uniquement les trajets en voiture
    long_trips_2019 = long_trips_2019.loc[long_trips_2019["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]
    long_trips_2008 = long_trips_2008.loc[long_trips_2008["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]

    long_trips_2019["n_other_passengers"] = long_trips_2019["n_other_passengers"].astype(int) + 1
    long_trips_2019["n_other_passengers"] = long_trips_2019["n_other_passengers"] * long_trips_2019["pondki"]
    long_trips_2019 = long_trips_2019.groupby(["motive_group"], as_index=False).agg(
        {"n_other_passengers": ['sum'], "pondki": ['sum'], "individual_id": ["count"]})
    long_trips_2019["n_other_passengers"] = long_trips_2019["n_other_passengers"] / long_trips_2019["pondki"]
    long_trips_2019["survey"] = "EMP-2019"
    long_trips_2019.columns = ["motive_group", "n_other_passengers", "pondki", "individual_id", "survey"]

    long_trips_2008["n_other_passengers"] = long_trips_2008["n_other_passengers"].astype(int) + 1
    long_trips_2008["n_other_passengers"] = long_trips_2008["n_other_passengers"] * long_trips_2008["pondki"]
    long_trips_2008 = long_trips_2008.groupby(["motive_group"], as_index=False).agg(
        {"n_other_passengers": ['sum'], "pondki": ['sum'], "individual_id": ["count"]})
    long_trips_2008["n_other_passengers"] = long_trips_2008["n_other_passengers"] / long_trips_2008["pondki"]
    long_trips_2008["survey"] = "ENTD-2008"
    long_trips_2008.columns = ["motive_group", "n_other_passengers", "pondki", "individual_id", "survey"]

    long_trips = pd.concat([long_trips_2008, long_trips_2019])

    fig = go.Figure(

        data=[
            go.Bar(name='ENTD-2008', x=long_trips_2008["motive_group"].tolist(),
                   y=long_trips_2008["n_other_passengers"].tolist(), yaxis='y', offsetgroup=1,
                   marker=dict(color="rgb(236, 0, 141)")),
            go.Bar(name='EMP-2019', x=long_trips_2019["motive_group"].tolist(),
                   y=long_trips_2019["n_other_passengers"].tolist(), yaxis='y', offsetgroup=2,
                   marker=dict(color="rgb(11, 28, 45)"))
        ],

        layout={
            'yaxis': {'title': '(nombre de personnes / voiture)'}

        }
    )
    fig.update_layout(

        template="simple_white",
        font=dict(family="Gilroy Light", size=12),
        title_text="taux de remplissage moyen des voitures par motifs de déplacement", title_x=0.5
    )

    fig.update_layout(barmode='group')
    fig.show(renderer="png")


def mode_trajet():
    """
    

    Returns
    -------
    None.

    """

    long_trips_2019 = survey_data_2019["long_trips"]
    long_trips_2019["individual_id"] = "1"
    long_trips_2019 = pd.merge(long_trips_2019, motive_group, on="motive", how='left')
    long_trips_2019 = pd.merge(long_trips_2019, mode_transport, on="mode_id", how='left')
    long_trips_2008 = survey_data_2008["long_trips"]
    long_trips_2008["individual_id"] = "1"
    long_trips_2008 = pd.merge(long_trips_2008, motive_group, on="motive", how='left')
    long_trips_2008 = pd.merge(long_trips_2008, mode_transport, on="mode_id", how='left')

    distance_totale_2019 = long_trips_2019.copy()
    distance_totale_2019["distance"] = distance_totale_2019["distance"] * distance_totale_2019["pondki"]
    distance_totale_2019 = distance_totale_2019.groupby(["individual_id"], as_index=False).agg({"distance": ["sum"]})
    distance_totale_2019.columns = ["individual_id", "distance"]
    distance_totale_2019 = distance_totale_2019["distance"].values[0]
    distance_totale_2008 = long_trips_2008.copy()
    distance_totale_2008["distance"] = distance_totale_2008["distance"] * distance_totale_2008["pondki"]
    distance_totale_2008 = distance_totale_2008.groupby(["individual_id"], as_index=False).agg({"distance": ["sum"]})
    distance_totale_2008.columns = ["individual_id", "distance"]
    distance_totale_2008 = distance_totale_2008["distance"].values[0]

    mode = long_trips_2008["mode_id"].unique().tolist()
    mode_df = pd.DataFrame(mode, columns=['mode_id'])

    distance_2019 = []

    long_trips = long_trips_2019.copy()

    for k in mode:
        long_trips = long_trips.loc[long_trips["mode_id"] == k]
        long_trips["distance"] = long_trips["distance"] * long_trips["pondki"]
        distance = long_trips["distance"].sum()
        distance_2019.append(distance)
        long_trips = long_trips_2019.copy()

    distance_2019 = pd.DataFrame(distance_2019, columns=['distance'])
    distance_2019["mode_id"] = mode_df["mode_id"]
    distance_2019["distance"] = distance_2019["distance"] / distance_totale_2019

    distance_2008 = []
    long_trips = long_trips_2008.copy()

    for k in mode:
        long_trips = long_trips.loc[long_trips["mode_id"] == k]
        long_trips["distance"] = long_trips["distance"] * long_trips["pondki"]
        distance = long_trips["distance"].sum()
        distance_2008.append(distance)
        long_trips = long_trips_2008.copy()

    distance_2008 = pd.DataFrame(distance_2008, columns=['distance'])
    distance_2008["mode_id"] = mode_df["mode_id"]
    distance_2008["distance"] = distance_2008["distance"] / distance_totale_2008

    distance_2008["survey"] = "ENTD-2008"
    distance_2019["survey"] = "EMP-2019"
    distance_totale = pd.concat([distance_2008, distance_2019])

    distance_totale = pd.merge(distance_totale, mode_transport, on="mode_id", how='left')
    distance_totale = distance_totale.groupby(["mode_group", "survey"], as_index=False).agg({"distance": ['sum']})
    distance_totale.columns = ["mode_group", "survey", "distance"]

    distance_totale["distance"] = round(distance_totale["distance"], 2)

    fig = px.bar(distance_totale, x="survey", y="distance", color='mode_group', text_auto=True,
                 color_discrete_map=color_mode)
    fig.update_layout(
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        template="simple_white",
        title_font=dict(size=15),
        title_font_family="Gill Sans MT",
        title="Répartition des distances parcourues en fonction des modes de déplacement",
        xaxis={'title': ''},
        yaxis={'title': ''},
        legend={'title': ''}

    )
    fig.show(renderer="png")

    long_trips_2019 = long_trips_2019.groupby(["mode_group"], as_index=False).agg({"pondki": ["sum"]})
    long_trips_2019.columns = ["mode_group", "nbre_trajets"]
    nbre_trajets_tot = long_trips_2019["nbre_trajets"].sum()
    long_trips_2019["ratio"] = long_trips_2019["nbre_trajets"] / nbre_trajets_tot

    long_trips_2008 = long_trips_2008.groupby(["mode_group"], as_index=False).agg({"pondki": ["sum"]})
    long_trips_2008.columns = ["mode_group", "nbre_trajets"]
    nbre_trajets_tot = long_trips_2008["nbre_trajets"].sum()
    long_trips_2008["ratio"] = long_trips_2008["nbre_trajets"] / nbre_trajets_tot

    long_trips_2019["survey"] = "EMP-2019"
    long_trips_2008["survey"] = "ENTD-2008"
    long_trips = pd.concat([long_trips_2008, long_trips_2019])

    long_trips["ratio"] = round(long_trips["ratio"], 2)

    fig = px.bar(long_trips, x="survey", y="ratio", color='mode_group', text_auto=True, color_discrete_map=color_mode)
    fig.update_layout(

        font_family="Arial",
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        template="simple_white",
        title={
            'text': "Répartition des trajets en fonction des modes de transports",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        title_font=dict(size=15),

        xaxis={'title': ''},
        yaxis={'title': 'distance (km)'},
        legend={'title': ''}

    )

    fig.show(renderer="png")

    fig.update_layout(

    )
