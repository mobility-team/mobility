import sys

sys.path.append("..")
from mobility import trip_sampler as tp
import pandas as pd
import plotly.express as px
import numpy as np
from mobility.get_survey_data import get_survey_data
from mobility.safe_sample import safe_sample
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

motive_group = pd.read_excel("./entd_location_motive_group.xlsx", engine='openpyxl', dtype=str)
motive_group.columns = ["motive_group", "motive", "location", "motive_explication"]

mode_transport = pd.read_excel("./entd_mode.xlsx", engine='openpyxl', dtype=str)

mode_ef = pd.read_csv("./mode_ef.csv", dtype=str)

survey_data_2019 = get_survey_data("EMP-2019")
short_trips_2019 = survey_data_2019["short_trips"]
short_trips_2019["day_id"] = short_trips_2019.index
survey_data_2008 = get_survey_data("ENTD-2008")
short_trips_2008 = survey_data_2008["short_trips"]
short_trips_2008["day_id"] = short_trips_2008.index

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


def proportion_déplacement(a):
    """
    Répartition des trajets par catégories urbaines, csp, motifs, modes de transport

    Parameters
    ----------
    a : str
        DESCRIPTION. "catégorie_urbaine", "csp", "motifs", ou "mode"


    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019["day_id"] = short_trips_2019.index
    short_trips_2019 = pd.merge(short_trips_2019, motive_group, on="motive", how='left')
    short_trips_2019 = pd.merge(short_trips_2019, mode_transport, on="mode_id", how='left')
    short_trips_2019 = short_trips_2019.loc[short_trips_2019["weekday"] == True]

    short_trips_2008 = survey_data_2008["short_trips"]
    short_trips_2008["day_id"] = short_trips_2008.index
    short_trips_2008 = pd.merge(short_trips_2008, motive_group, on="motive", how='left')
    short_trips_2008 = pd.merge(short_trips_2008, mode_transport, on="mode_id", how='left')
    short_trips_2008 = short_trips_2008.loc[short_trips_2008["weekday"] == True]
    if a == "catégorie_urbaine":
        short_trips_2019 = short_trips_2019.groupby(["city_category"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2019.columns = ["city_category", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2019["nbre_trajets"].sum()
        short_trips_2019["ratio"] = short_trips_2019["nbre_trajets"] / nbre_trajets_tot

        short_trips_2008 = short_trips_2008.groupby(["city_category"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2008.columns = ["city_category", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2008["nbre_trajets"].sum()
        short_trips_2008["ratio"] = short_trips_2008["nbre_trajets"] / nbre_trajets_tot

        short_trips_2019["survey"] = "EMP-2019"
        short_trips_2008["survey"] = "ENTD-2008"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["city_category"].tolist(),
                       y=short_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["city_category"].tolist(),
                       y=short_trips_2019["ratio"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Répartition des trajets en fonction des catégories urbaines", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["city_category"].tolist(),
                       y=short_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["city_category"].tolist(),
                       y=short_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Nombre de trajets en fonction des catégories urbaines", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

    if a == "csp":
        short_trips_2019 = short_trips_2019.groupby(["csp"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2019.columns = ["csp", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2019["nbre_trajets"].sum()
        short_trips_2019["ratio"] = short_trips_2019["nbre_trajets"] / nbre_trajets_tot

        short_trips_2008 = short_trips_2008.groupby(["csp"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2008.columns = ["csp", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2008["nbre_trajets"].sum()
        short_trips_2008["ratio"] = short_trips_2008["nbre_trajets"] / nbre_trajets_tot

        short_trips_2019["survey"] = "EMP-2019"
        short_trips_2008["survey"] = "ENTD-2008"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["csp"].tolist(), y=short_trips_2008["ratio"].tolist(),
                       yaxis='y', offsetgroup=1, marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["csp"].tolist(), y=short_trips_2019["ratio"].tolist(),
                       yaxis='y', offsetgroup=2, marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Répartition des trajets en fonction des csp", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["csp"].tolist(),
                       y=short_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["csp"].tolist(), y=short_trips_2019["nbre_trajets"].tolist(),
                       yaxis='y', offsetgroup=2, marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Nombre de trajets en fonction des csp", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

    if a == "motifs":
        short_trips_2019 = short_trips_2019.groupby(["motive_group"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2019.columns = ["motive_group", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2019["nbre_trajets"].sum()
        short_trips_2019["ratio"] = short_trips_2019["nbre_trajets"] / nbre_trajets_tot

        short_trips_2008 = short_trips_2008.groupby(["motive_group"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2008.columns = ["motive_group", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2008["nbre_trajets"].sum()
        short_trips_2008["ratio"] = short_trips_2008["nbre_trajets"] / nbre_trajets_tot

        short_trips_2019["survey"] = "EMP-2019"
        short_trips_2008["survey"] = "ENTD-2008"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["motive_group"].tolist(),
                       y=short_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["motive_group"].tolist(),
                       y=short_trips_2019["ratio"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Répartition des trajets en fonction des motifs de déplacement", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["motive_group"].tolist(),
                       y=short_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["motive_group"].tolist(),
                       y=short_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Nombre de trajets en fonction des motifs de déplacement", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

    if a == "mode":
        short_trips_2019 = short_trips_2019.groupby(["mode_group"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2019.columns = ["mode_group", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2019["nbre_trajets"].sum()
        short_trips_2019["ratio"] = short_trips_2019["nbre_trajets"] / nbre_trajets_tot

        short_trips_2008 = short_trips_2008.groupby(["mode_group"], as_index=False).agg({"individual_id": ["count"]})
        short_trips_2008.columns = ["mode_group", "nbre_trajets"]
        nbre_trajets_tot = short_trips_2008["nbre_trajets"].sum()
        short_trips_2008["ratio"] = short_trips_2008["nbre_trajets"] / nbre_trajets_tot

        short_trips_2019["survey"] = "EMP-2019"
        short_trips_2008["survey"] = "ENTD-2008"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["mode_group"].tolist(),
                       y=short_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["mode_group"].tolist(), y=short_trips_2019["ratio"].tolist(),
                       yaxis='y', offsetgroup=2, marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Répartition des trajets en fonction des modes de déplacement", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")
        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["mode_group"].tolist(),
                       y=short_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["mode_group"].tolist(),
                       y=short_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': ''}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Nombre de trajets en fonction des modes de déplacement", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")


def distance_par_trajet(a):
    """
    Distances par trajet en fonction des catégories urbaines ou des csp

    Parameters
    ----------
    a : str
        "distance_par_trajet", "csp"
        
        
    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019["day_id"] = short_trips_2019.index

    short_trips_2008 = survey_data_2008["short_trips"]
    short_trips_2008["day_id"] = short_trips_2008.index

    if a == "catégorie_urbaine":
        short_trips_2019 = short_trips_2019.groupby(["city_category"], as_index=False).agg(
            {"distance": ['sum'], "individual_id": ["count"]})
        short_trips_2019.columns = ["city_category", "distance", "nbre_trajets"]
        short_trips_2019["distance"] = short_trips_2019["distance"] / short_trips_2019["nbre_trajets"]

        short_trips_2008 = short_trips_2008.groupby(["city_category"], as_index=False).agg(
            {"distance": ['sum'], "individual_id": ["count"]})
        short_trips_2008.columns = ["city_category", "distance", "nbre_trajets"]
        short_trips_2008["distance"] = short_trips_2008["distance"] / short_trips_2008["nbre_trajets"]

        short_trips_2019["survey"] = "EMP-2019"
        short_trips_2008["survey"] = "ENTD-2008"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["city_category"].tolist(),
                       y=short_trips_2008["distance"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["city_category"].tolist(),
                       y=short_trips_2019["distance"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': 'distance (km)'}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distance par trajet en fonction de la catégorie urbaine", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

    if a == "csp":
        short_trips_2019 = short_trips_2019.groupby(["csp"], as_index=False).agg(
            {"distance": ['sum'], "individual_id": ["count"]})
        short_trips_2019.columns = ["csp", "distance", "nbre_trajets"]
        short_trips_2019["distance"] = short_trips_2019["distance"] / short_trips_2019["nbre_trajets"]

        short_trips_2008 = short_trips_2008.groupby(["csp"], as_index=False).agg(
            {"distance": ['sum'], "individual_id": ["count"]})
        short_trips_2008.columns = ["csp", "distance", "nbre_trajets"]
        short_trips_2008["distance"] = short_trips_2008["distance"] / short_trips_2008["nbre_trajets"]

        short_trips_2019["survey"] = "EMP-2019"
        short_trips_2008["survey"] = "ENTD-2008"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["csp"].tolist(), y=short_trips_2008["distance"].tolist(),
                       yaxis='y', offsetgroup=1, marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["csp"].tolist(), y=short_trips_2019["distance"].tolist(),
                       yaxis='y', offsetgroup=2, marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': 'distance (km)'}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distance par trajet en fonction de la csp", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")


def distance_quotidienne(a):
    """
    Distance quotidienne par catégorie urbaine ou csp

    Parameters
    ----------
    a : str
        "catégorie_urbaine", "csp"
        
        
    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019["day_id"] = short_trips_2019.index
    short_trips_2008 = survey_data_2008["short_trips"]
    short_trips_2008["day_id"] = short_trips_2008.index

    if a == "csp":
        short_trips_2019 = short_trips_2019.groupby(["individual_id", "pondki", "csp"], as_index=False).agg(
            {"distance": ['sum']})
        short_trips_2019.columns = ["individual_id", "pondki", "csp", "distance"]
        short_trips_2019["distance"] = short_trips_2019["distance"] * short_trips_2019["pondki"]
        short_trips_2019 = short_trips_2019.groupby(["csp"], as_index=False).agg(
            {"distance": ['sum'], "pondki": ["sum"], "individual_id": ["count"]})
        short_trips_2019.columns = ["csp", "distance_quotidienne", "somme_pondki", "nombre_personnes"]
        short_trips_2019["distance_quotidienne"] = short_trips_2019["distance_quotidienne"] / short_trips_2019[
            "somme_pondki"]

        short_trips_2008 = short_trips_2008.groupby(["individual_id", "pondki", "csp"], as_index=False).agg(
            {"distance": ['sum'], "day_id": ["nunique"]})
        short_trips_2008.columns = ["individual_id", "pondki", "csp", "distance", "nbre_jours"]
        short_trips_2008["distance"] = short_trips_2008["distance"] * short_trips_2008["pondki"]
        short_trips_2008["pondki"] = short_trips_2008["pondki"] * short_trips_2008["nbre_jours"]
        short_trips_2008 = short_trips_2008.groupby(["csp"], as_index=False).agg(
            {"distance": ['sum'], "pondki": ["sum"], "individual_id": ["count"], "nbre_jours": ["sum"]})
        short_trips_2008.columns = ["csp", "distance_quotidienne", "somme_pondki", "nombre_personnes", "nbre_jours"]
        short_trips_2008["distance_quotidienne"] = short_trips_2008["distance_quotidienne"] / short_trips_2008[
            "somme_pondki"]

        short_trips_2008["survey"] = "ENTD-2008"
        short_trips_2019["survey"] = "EMP-2019"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["csp"].tolist(),
                       y=short_trips_2008["distance_quotidienne"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["csp"].tolist(),
                       y=short_trips_2019["distance_quotidienne"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': 'distance (km)'}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distance quotidienne en fonction de la csp", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")

    if a == "catégorie_urbaine":
        short_trips_2019 = short_trips_2019.groupby(["individual_id", "pondki", "city_category"], as_index=False).agg(
            {"distance": ['sum']})
        short_trips_2019.columns = ["individual_id", "pondki", "city_category", "distance"]
        short_trips_2019["distance"] = short_trips_2019["distance"] * short_trips_2019["pondki"]
        short_trips_2019 = short_trips_2019.groupby(["city_category"], as_index=False).agg(
            {"distance": ['sum'], "pondki": ["sum"], "individual_id": ["count"]})
        short_trips_2019.columns = ["catégorie_urbaine", "distance_quotidienne", "somme_pondki", "nombre_personnes"]
        short_trips_2019["distance_quotidienne"] = short_trips_2019["distance_quotidienne"] / short_trips_2019[
            "somme_pondki"]

        # dans la base de donnée de 2008, certains individus ont des trajets sur plusieurs jours
        short_trips_2008 = short_trips_2008.groupby(["individual_id", "pondki", "city_category"], as_index=False).agg(
            {"distance": ['sum'], "day_id": ["nunique"]})
        short_trips_2008.columns = ["individual_id", "pondki", "city_category", "distance", "nbre_jours"]
        short_trips_2008["distance"] = short_trips_2008["distance"] * short_trips_2008["pondki"]
        short_trips_2008["pondki"] = short_trips_2008["pondki"] * short_trips_2008["nbre_jours"]
        short_trips_2008 = short_trips_2008.groupby(["city_category"], as_index=False).agg(
            {"distance": ['sum'], "pondki": ["sum"], "individual_id": ["count"], "nbre_jours": ["sum"]})
        short_trips_2008.columns = ["catégorie_urbaine", "distance_quotidienne", "somme_pondki", "nombre_personnes",
                                    "nbre_jours"]
        short_trips_2008["distance_quotidienne"] = short_trips_2008["distance_quotidienne"] / short_trips_2008[
            "somme_pondki"]

        short_trips_2008["survey"] = "ENTD-2008"
        short_trips_2019["survey"] = "EMP-2019"
        short_trips = pd.concat([short_trips_2008, short_trips_2019])

        fig = go.Figure(

            data=[
                go.Bar(name='ENTD-2008', x=short_trips_2008["catégorie_urbaine"].tolist(),
                       y=short_trips_2008["distance_quotidienne"].tolist(), yaxis='y', offsetgroup=1,
                       marker=dict(color="rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=short_trips_2019["catégorie_urbaine"].tolist(),
                       y=short_trips_2019["distance_quotidienne"].tolist(), yaxis='y', offsetgroup=2,
                       marker=dict(color="rgb(11, 28, 45)"))
            ],

            layout={
                'yaxis': {'title': 'distance (km)'}

            }
        )
        fig.update_layout(

            template="simple_white",
            font=dict(family="Gilroy Light", size=12),
            title_text="Distance quotidienne en fonction de la catégorie urbaine", title_x=0.5
        )

        fig.update_layout(barmode='group')
        fig.show(renderer="png")


def taux_remplissage():
    """
    Taux de remplissage moyen des voitures en fonction des motifs de déplacement

    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019["day_id"] = short_trips_2019.index
    short_trips_2019 = pd.merge(short_trips_2019, motive_group, on="motive", how='left')
    short_trips_2008 = survey_data_2008["short_trips"]
    short_trips_2008["day_id"] = short_trips_2008.index
    short_trips_2008 = pd.merge(short_trips_2008, motive_group, on="motive", how='left')

    # on regarde uniquement les individus possédant une voiture
    short_trips_2019 = short_trips_2019.loc[short_trips_2019["n_cars"] != "0"]
    short_trips_2008 = short_trips_2008.loc[short_trips_2008["n_cars"] != "0"]

    # on regarde uniquement les trajets en voiture
    short_trips_2019 = short_trips_2019.loc[short_trips_2019["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]
    short_trips_2008 = short_trips_2008.loc[short_trips_2008["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]

    short_trips_2019["n_other_passengers"] = short_trips_2019["n_other_passengers"].astype(int) + 1
    short_trips_2019["n_other_passengers"] = short_trips_2019["n_other_passengers"] * short_trips_2019["pondki"]
    short_trips_2019 = short_trips_2019.groupby(["motive_group"], as_index=False).agg(
        {"n_other_passengers": ['sum'], "pondki": ['sum'], "individual_id": ["count"]})
    short_trips_2019["n_other_passengers"] = short_trips_2019["n_other_passengers"] / short_trips_2019["pondki"]
    short_trips_2019["survey"] = "EMP-2019"
    short_trips_2019.columns = ["motive_group", "n_other_passengers", "pondki", "individual_id", "survey"]

    short_trips_2008["n_other_passengers"] = short_trips_2008["n_other_passengers"].astype(int) + 1
    short_trips_2008["n_other_passengers"] = short_trips_2008["n_other_passengers"] * short_trips_2008["pondki"]
    short_trips_2008 = short_trips_2008.groupby(["motive_group"], as_index=False).agg(
        {"n_other_passengers": ['sum'], "pondki": ['sum'], "individual_id": ["count"]})
    short_trips_2008["n_other_passengers"] = short_trips_2008["n_other_passengers"] / short_trips_2008["pondki"]
    short_trips_2008["survey"] = "ENTD-2008"
    short_trips_2008.columns = ["motive_group", "n_other_passengers", "pondki", "individual_id", "survey"]

    short_trips = pd.concat([short_trips_2008, short_trips_2019])

    print(short_trips_2008["n_other_passengers"].sum() / short_trips_2008["n_other_passengers"].count())
    print(short_trips_2019["n_other_passengers"].sum() / short_trips_2019["n_other_passengers"].count())

    fig = go.Figure(

        data=[
            go.Bar(name='ENTD-2008', x=short_trips_2008["motive_group"].tolist(),
                   y=short_trips_2008["n_other_passengers"].tolist(), yaxis='y', offsetgroup=1,
                   marker=dict(color="rgb(236, 0, 141)")),
            go.Bar(name='EMP-2019', x=short_trips_2019["motive_group"].tolist(),
                   y=short_trips_2019["n_other_passengers"].tolist(), yaxis='y', offsetgroup=2,
                   marker=dict(color="rgb(11, 28, 45)"))
        ],

        layout={
            'yaxis': {'title': '(nombre de personnes / voiture)'}

        }
    )
    fig.update_layout(

        template="simple_white",
        font=dict(family="Gilroy Light", size=12),
        title_text="Taux de remplissage moyen des voitures par motifs de déplacement", title_x=0.5
    )

    fig.update_layout(barmode='group')
    fig.show(renderer="png")


def distances_quotidiennes_motif(csp="", city_category="", day=""):
    """
    Distances quotidiennes par motif

    Parameters
    ----------
    csp : str
        "1", "2", "3", "4", "5", "6", "7", "8", "no_csp"
        permet de filtrer la base à une certaine csp
        
    city_category : str
        "R", "B", "C", "I"
        permet de filtrer la base à une certaine catégorie urbaine
    day : str
        "weekday", "weekend"
        permet de filtrer la base aux jours de la semaine ou du weekend

    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019["day_id"] = short_trips_2019.index
    short_trips_2008 = survey_data_2008["short_trips"]
    short_trips_2008["day_id"] = short_trips_2008.index

    if city_category != "":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["city_category"] == city_category]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["city_category"] == city_category]

    if csp != "":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["csp"] == csp]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["csp"] == csp]

    if day == "weekday":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["weekday"] == True]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["weekday"] == True]

    if day == "weekend":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["weekday"] == False]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["weekday"] == False]

    motive = short_trips_2008["motive"].unique().tolist()
    motive_df = pd.DataFrame(motive, columns=['motive'])

    distance_totale_2019 = []
    short_trips = short_trips_2019.copy()

    for k in motive:
        short_trips = short_trips.loc[short_trips["motive"] == k]
        short_trips["distance"] = short_trips["distance"] * short_trips["pondki"]
        distance = short_trips["distance"].sum()
        distance_totale_2019.append(distance)
        short_trips = short_trips_2019.copy()

    short_trips_2019 = short_trips_2019.groupby(["individual_id", "pondki"], as_index=False).agg(
        {"day_id": ["nunique"]})
    short_trips_2019.columns = ["individual_id", "pondki", "day_id"]
    short_trips_2019["pondki"] = short_trips_2019["pondki"] * short_trips_2019["day_id"]
    nbre_individus = short_trips_2019["pondki"].sum()
    distance_totale_2019 = distance_totale_2019 / nbre_individus
    distance_totale_2019 = pd.DataFrame(distance_totale_2019, columns=['distance'])
    distance_totale_2019["motive"] = motive_df["motive"]

    distance_totale_2008 = []
    short_trips = short_trips_2008.copy()

    for k in motive:
        short_trips = short_trips.loc[short_trips["motive"] == k]
        short_trips["distance"] = short_trips["distance"] * short_trips["pondki"]
        distance = short_trips["distance"].sum()
        distance_totale_2008.append(distance)
        short_trips = short_trips_2008.copy()

    short_trips_2008 = short_trips_2008.groupby(["individual_id", "pondki"], as_index=False).agg(
        {"day_id": ["nunique"]})
    short_trips_2008.columns = ["individual_id", "pondki", "day_id"]
    short_trips_2008["pondki"] = short_trips_2008["pondki"] * short_trips_2008["day_id"]
    nbre_individus = short_trips_2008["pondki"].sum()
    distance_totale_2008 = distance_totale_2008 / nbre_individus
    distance_totale_2008 = pd.DataFrame(distance_totale_2008, columns=['distance'])
    distance_totale_2008["motive"] = motive_df["motive"]

    distance_totale_2008["survey"] = "ENTD-2008"
    distance_totale_2019["survey"] = "EMP-2019"

    distance_totale = pd.concat([distance_totale_2008, distance_totale_2019])

    distance_totale = pd.merge(distance_totale, motive_group, on="motive", how='left')
    distance_totale = distance_totale.groupby(["motive_group", "survey"], as_index=False).agg({"distance": ['sum']})
    distance_totale.columns = ["motive_group", "survey", "distance"]

    distance_totale = distance_totale.sort_index(ascending=False)

    distance_totale["distance"] = round(distance_totale["distance"], 1)
    fig = px.bar(distance_totale, x="survey", y="distance", color='motive_group', text_auto=True,
                 color_discrete_map=color_motive)
    fig.update_layout(
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        template="simple_white",
        title={
            'text': "Distance moyenne journalière en fonction des motifs de déplacement",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        title_font=dict(size=15),
        font_family="Arial",
        xaxis={'title': ''},
        yaxis={'title': 'distance (km)'},
        legend={'title': ''}

    )
    fig.show(renderer="png")


def distances_quotidiennes_mode(csp="", city_category="", day=""):
    """
    Distances quotidiennes par mode de transport

    Parameters
    ----------
    csp : str
        "1", "2", "3", "4", "5", "6", "7", "8", "no_csp"
        permet de filtrer la base à une certaine csp
        
    city_category : str
        "R", "B", "C", "I"
        permet de filtrer la base à une certaine catégorie urbaine
        
    day : str
        "weekday", "weekend"
        permet de filtrer la base aux jours de la semaine ou du weekend

    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019["day_id"] = short_trips_2019.index
    short_trips_2008 = survey_data_2008["short_trips"]
    short_trips_2008["day_id"] = short_trips_2008.index

    if city_category != "":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["city_category"] == city_category]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["city_category"] == city_category]

    if csp != "":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["csp"] == csp]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["csp"] == csp]

    if day == "weekday":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["weekday"] == True]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["weekday"] == True]

    if day == "weekend":
        short_trips_2008 = short_trips_2008.loc[short_trips_2008["weekday"] == False]
        short_trips_2019 = short_trips_2019.loc[short_trips_2019["weekday"] == False]

    mode = short_trips_2008["mode_id"].unique().tolist()
    mode_df = pd.DataFrame(mode, columns=['mode_id'])

    distance_totale_2019 = []
    short_trips = short_trips_2019.copy()

    for k in mode:
        short_trips = short_trips.loc[short_trips["mode_id"] == k]
        short_trips["distance"] = short_trips["distance"] * short_trips["pondki"]
        distance = short_trips["distance"].sum()
        distance_totale_2019.append(distance)
        short_trips = short_trips_2019.copy()

    short_trips_2019 = short_trips_2019.groupby(["individual_id", "pondki"], as_index=False).agg(
        {"day_id": ["nunique"]})
    short_trips_2019.columns = ["individual_id", "pondki", "day_id"]
    short_trips_2019["pondki"] = short_trips_2019["pondki"] * short_trips_2019["day_id"]
    nbre_individus = short_trips_2019["pondki"].sum()
    distance_totale_2019 = distance_totale_2019 / nbre_individus
    distance_totale_2019 = pd.DataFrame(distance_totale_2019, columns=['distance'])
    distance_totale_2019["mode_id"] = mode_df["mode_id"]

    distance_totale_2008 = []
    short_trips = short_trips_2008.copy()

    for k in mode:
        short_trips = short_trips.loc[short_trips["mode_id"] == k]
        short_trips["distance"] = short_trips["distance"] * short_trips["pondki"]
        distance = short_trips["distance"].sum()
        distance_totale_2008.append(distance)
        short_trips = short_trips_2008.copy()

    short_trips_2008 = short_trips_2008.groupby(["individual_id", "pondki"], as_index=False).agg(
        {"day_id": ["nunique"]})
    short_trips_2008.columns = ["individual_id", "pondki", "day_id"]
    short_trips_2008["pondki"] = short_trips_2008["pondki"] * short_trips_2008["day_id"]
    nbre_individus = short_trips_2008["pondki"].sum()
    distance_totale_2008 = distance_totale_2008 / nbre_individus
    distance_totale_2008 = pd.DataFrame(distance_totale_2008, columns=['distance'])
    distance_totale_2008["mode_id"] = mode_df["mode_id"]

    distance_totale_2008["survey"] = "ENTD-2008"
    distance_totale_2019["survey"] = "EMP-2019"
    distance_totale = pd.concat([distance_totale_2008, distance_totale_2019])

    distance_totale = pd.merge(distance_totale, mode_transport, on="mode_id", how='left')
    distance_totale = distance_totale.groupby(["mode_group", "survey"], as_index=False).agg({"distance": ['sum']})
    distance_totale.columns = ["mode_group", "survey", "distance"]

    distance_totale.loc[
        (distance_totale["mode_group"].isin(["Avion", "Bateau", "Train grande ligne ou TGV"])), "mode_group"] = "Autre"

    distance_totale = distance_totale.sort_index(ascending=False)

    distance_totale["distance"] = round(distance_totale["distance"], 1)

    print(distance_totale)

    fig = px.bar(distance_totale, x="survey", y="distance", color='mode_group', text_auto=True,
                 color_discrete_map=color_mode)
    fig.update_layout(
        font_family="Arial",
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        template="simple_white",
        title={
            'text': "Distance moyenne journalière en fonction des modes de déplacement",
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


def émissions_dist_motifs():
    """
    Distances et émissions moyennes par déplacement en fonction des motifs de déplacement

    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019 = pd.merge(short_trips_2019, motive_group, on="motive", how='left')
    short_trips_2019 = pd.merge(short_trips_2019, mode_transport, on="mode_id", how='left')
    short_trips_2019 = pd.merge(short_trips_2019, mode_ef, on="mode_id", how='left')

    short_trips_2019["distance"] = short_trips_2019["distance"] * short_trips_2019["pondki"]
    # Car emission factors are corrected to account for other passengers
    #    (no need to do this for the other modes, their emission factors are already in kgCO2e/passenger.km)
    k_ef_car = 1 / (1 + short_trips_2019["n_other_passengers"])
    short_trips_2019["k_ef"] = np.where(short_trips_2019["mode_id"].str.slice(0, 1) == "3", k_ef_car, 1.0)

    short_trips_2019["ef"] = short_trips_2019["ef"].astype(float) * short_trips_2019["distance"] * short_trips_2019[
        "k_ef"]
    short_trips_2019 = short_trips_2019.groupby(["motive_group"], as_index=False).agg(
        {"distance": ['sum'], "ef": ['sum'], "pondki": ['sum']})
    short_trips_2019.columns = ["motive_group", "distance", "ef", "pondki"]
    short_trips_2019["distance"] = short_trips_2019["distance"] / short_trips_2019["pondki"]
    short_trips_2019["ef"] = short_trips_2019["ef"] / short_trips_2019["pondki"]
    short_trips_2019["survey"] = "EMP-2019"
    short_trips_2019["distance"] = round(short_trips_2019["distance"], 2)
    short_trips_2019["ef"] = round(short_trips_2019["ef"], 2)
    short_trips_2019 = short_trips_2019.sort_values(by=["ef"])

    fig = go.Figure(

        data=[
            go.Bar(name='émissions', x=short_trips_2019["motive_group"].tolist(), y=short_trips_2019["ef"].tolist(),
                   yaxis='y', offsetgroup=1, marker=dict(color="rgb(236, 0, 141)")),
            go.Bar(name='distances', x=short_trips_2019["motive_group"].tolist(),
                   y=short_trips_2019["distance"].tolist(), yaxis='y2', offsetgroup=2,
                   marker=dict(color="rgb(11, 28, 45)"))
        ],
        layout={
            'yaxis': {'title': 'émissions (kgCO<sub>2</sub>e)'},
            'yaxis2': {'title': 'distances (km)', 'overlaying': 'y', 'side': 'right'}
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
        title_text="Distances et émissions moyennes par déplacement <br>en fonction du motif de déplacement",
        title_x=0.5
    )

    fig.update_layout(barmode='group')
    fig.show(renderer="png")


def répartition_modes_par_motif(a):
    """
    Répartition des modes de transport par motif de déplacement
    
    Parameters
    ----------
    a : str
        'Domicile', 'Enseignement', 'Commerce', 'Médical', 'Visite extérieure', 'Sport / loisirs / culture', 'Vacance', 'Travail'

    Returns
    -------
    None.

    """

    short_trips_2019 = survey_data_2019["short_trips"]
    short_trips_2019 = pd.merge(short_trips_2019, motive_group, on="motive", how='left')
    short_trips_2019 = pd.merge(short_trips_2019, mode_transport, on="mode_id", how='left')
    nbre_trips = len(short_trips_2019.index)

    short_trips_2019 = short_trips_2019.loc[(short_trips_2019["motive_group"] == a)]
    short_trips_2019 = short_trips_2019.groupby(["mode_group"], as_index=False).agg({"individual_id": ['count']})
    short_trips_2019.columns = ["mode_group", "individual_id"]
    short_trips_2019["individual_id"] = short_trips_2019["individual_id"] / nbre_trips

    short_trips_2019["autre"] = short_trips_2019["mode_group"]
    short_trips_2019.loc[short_trips_2019["individual_id"] < 0.002, "autre"] = "Autre"
    short_trips_2019 = short_trips_2019.groupby(["autre"], as_index=False).agg({"individual_id": ['sum']})
    short_trips_2019.columns = ["mode_group", "individual_id"]

    short_trips_2019["individual_id"] = round(short_trips_2019["individual_id"], 3)

    fig = px.pie(short_trips_2019.loc[short_trips_2019["individual_id"] > 0.0001], values="individual_id",
                 names="mode_group", color="mode_group", title='Répartition des modes pour le motif : ' + a,
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
