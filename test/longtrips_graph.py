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

motive_group = pd.read_excel("./entd_location_motive_group.xlsx", engine='openpyxl', dtype=str)
motive_group.columns=["motive_group", "motive", "location", "motive_explication"]

mode_transport = pd.read_excel("./entd_mode.xlsx", engine='openpyxl', dtype=str)




survey_data_2019 = get_survey_data("EMP-2019")
long_trips_2019 = survey_data_2019["long_trips"]

survey_data_2008 = get_survey_data("ENTD-2008")
long_trips_2008 = survey_data_2008["long_trips"]




color_mode={
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
    
    long_trips_2019 = survey_data_2019["long_trips"]
    long_trips_2019 = pd.merge(long_trips_2019, motive_group, on="motive", how='left')
    long_trips_2019 = pd.merge(long_trips_2019, mode_transport, on="mode_id", how='left')
    long_trips_2019["individual_id"]="1"
    long_trips_2008 = survey_data_2008["long_trips"]
    long_trips_2008 = pd.merge(long_trips_2008, motive_group, on="motive", how='left')  
    long_trips_2008 = pd.merge(long_trips_2008, mode_transport, on="mode_id", how='left')
    long_trips_2008["individual_id"]="1"

    if a == "catégorie_urbaine":
        long_trips_2019 = long_trips_2019.groupby(["city_category"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2019.columns = ["city_category", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2019["nbre_trajets"].sum()
        long_trips_2019["ratio"] = long_trips_2019["nbre_trajets"] / nbre_trajets_tot
            
        long_trips_2008 = long_trips_2008.groupby(["city_category"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2008.columns = ["city_category", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2008["nbre_trajets"].sum()
        long_trips_2008["ratio"] = long_trips_2008["nbre_trajets"] / nbre_trajets_tot
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
        

        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["city_category"].tolist(), y=long_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["city_category"].tolist(), y=long_trips_2019["ratio"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Répartition des trajets en fonction des catégories urbaines", title_x=0.5
            )
        
      
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["city_category"].tolist(), y=long_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["city_category"].tolist(), y=long_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Nombre de trajets en fonction des catégories urbaines", title_x=0.5
            )
        
        
        fig.update_layout(barmode='group')
        fig.show(renderer="png")   

        
    if a == "csp":
        long_trips_2019 = long_trips_2019.groupby(["csp"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2019.columns = ["csp", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2019["nbre_trajets"].sum()
        long_trips_2019["ratio"] = long_trips_2019["nbre_trajets"] / nbre_trajets_tot
           
        long_trips_2008 = long_trips_2008.groupby(["csp"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2008.columns = ["csp", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2008["nbre_trajets"].sum()
        long_trips_2008["ratio"] = long_trips_2008["nbre_trajets"] / nbre_trajets_tot
           
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
           

        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["csp"].tolist(), y=long_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["csp"].tolist(), y=long_trips_2019["ratio"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Répartition des trajets en fonction des csp", title_x=0.5
            )
        
    
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["csp"].tolist(), y=long_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["csp"].tolist(), y=long_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Nombre de trajets en fonction des csp", title_x=0.5
            )
        
        
        fig.update_layout(barmode='group')
        fig.show(renderer="png")   
        
        
    if a == "motifs":
        long_trips_2019 = long_trips_2019.groupby(["motive_group"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2019.columns = ["motive_group", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2019["nbre_trajets"].sum()
        long_trips_2019["ratio"] = long_trips_2019["nbre_trajets"] / nbre_trajets_tot
            
        long_trips_2008 = long_trips_2008.groupby(["motive_group"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2008.columns = ["motive_group", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2008["nbre_trajets"].sum()
        long_trips_2008["ratio"] = long_trips_2008["nbre_trajets"] / nbre_trajets_tot
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
        

        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["motive_group"].tolist(), y=long_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["motive_group"].tolist(), y=long_trips_2019["ratio"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Répartition des trajets en fonction des motifs de déplacement", title_x=0.5
            )
        
      
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["motive_group"].tolist(), y=long_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["motive_group"].tolist(), y=long_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Nombre de trajets en fonction des motifs de déplacement", title_x=0.5
            )
        
        
        fig.update_layout(barmode='group')
        fig.show(renderer="png")   

    if a == "mode":
       
        
        
        


        long_trips_2019 = long_trips_2019.groupby(["mode_group"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2019.columns = ["mode_group", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2019["nbre_trajets"].sum()
        long_trips_2019["ratio"] = long_trips_2019["nbre_trajets"] / nbre_trajets_tot
            
        long_trips_2008 = long_trips_2008.groupby(["mode_group"], as_index=False).agg({"individual_id": ["count"]})
        long_trips_2008.columns = ["mode_group", "nbre_trajets"]
        nbre_trajets_tot = long_trips_2008["nbre_trajets"].sum()
        long_trips_2008["ratio"] = long_trips_2008["nbre_trajets"] / nbre_trajets_tot
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
    
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["mode_group"].tolist(), y=long_trips_2008["ratio"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["mode_group"].tolist(), y=long_trips_2019["ratio"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Répartition des trajets en fonction des modes de déplacement", title_x=0.5
            )
        
  
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["mode_group"].tolist(), y=long_trips_2008["nbre_trajets"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["mode_group"].tolist(), y=long_trips_2019["nbre_trajets"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="Nombre de trajets en fonction des modes de déplacement", title_x=0.5
            )
        
        
        fig.update_layout(barmode='group')
        fig.show(renderer="png")   
        
def distance_par_trajet(a):
    
    """
    Distances par trajet en fonction des catégories urbaines, des csp, des motifs ou des modes de déplacement

    Parameters
    ----------
    a : str
        "catégorie_urbaine", "csp", "motifs", "modes"
        
        
    Returns
    -------
    None.

    """
    
    long_trips_2019 = survey_data_2019["long_trips"]
    long_trips_2019["individual_id"] = "1"
    
    long_trips_2008 = survey_data_2008["long_trips"]
    long_trips_2008["individual_id"] = "1"
   

    if a == "catégorie_urbaine":
        long_trips_2019 = long_trips_2019.groupby(["city_category"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2019.columns = ["city_category", "distance", "nbre_trajets"]
        long_trips_2019["distance"] = long_trips_2019["distance"] / long_trips_2019["nbre_trajets"]
        
        long_trips_2008 = long_trips_2008.groupby(["city_category"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2008.columns = ["city_category", "distance", "nbre_trajets"]
        long_trips_2008["distance"] = long_trips_2008["distance"] / long_trips_2008["nbre_trajets"]       
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
        
        print(long_trips_2019["distance"].sum()/4)
        print(long_trips_2008["distance"].sum()/4)
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["city_category"].tolist(), y=long_trips_2008["distance"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["city_category"].tolist(), y=long_trips_2019["distance"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': 'distance (km)'}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="distance par trajet en fonction de la catégorie urbaine", title_x=0.5
            )
        
       
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
    if a == "csp" :
        
        long_trips_2019 = long_trips_2019.groupby(["csp"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2019.columns = ["csp", "distance", "nbre_trajets"]
        long_trips_2019["distance"] = long_trips_2019["distance"] / long_trips_2019["nbre_trajets"]
        
        long_trips_2008 = long_trips_2008.groupby(["csp"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2008.columns = ["csp", "distance", "nbre_trajets"]
        long_trips_2008["distance"] = long_trips_2008["distance"] / long_trips_2008["nbre_trajets"]       
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
        
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["csp"].tolist(), y=long_trips_2008["distance"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["csp"].tolist(), y=long_trips_2019["distance"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': 'distance (km)'}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="distance par trajet en fonction de la csp", title_x=0.5
            )
        
      
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
    
    if a == "motifs" :
        
        long_trips_2019 = pd.merge(long_trips_2019, motive_group, on="motive", how='left')
        long_trips_2019 = long_trips_2019.groupby(["motive_group"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2019.columns = ["motive_group", "distance", "nbre_trajets"]
        long_trips_2019["distance"] = long_trips_2019["distance"] / long_trips_2019["nbre_trajets"]
        
        long_trips_2008 = pd.merge(long_trips_2008, motive_group, on="motive", how='left')
        long_trips_2008 = long_trips_2008.groupby(["motive_group"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2008.columns = ["motive_group", "distance", "nbre_trajets"]
        long_trips_2008["distance"] = long_trips_2008["distance"] / long_trips_2008["nbre_trajets"]       
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
        

        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["motive_group"].tolist(), y=long_trips_2008["distance"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["motive_group"].tolist(), y=long_trips_2019["distance"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="distance par trajet (km) en fonction du motif de déplacement", title_x=0.5
            )
        
      
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        
        
        
    if a == "modes" :
        
        long_trips_2019 = pd.merge(long_trips_2019, mode_transport, on="mode_id", how='left')
        long_trips_2019 = long_trips_2019.groupby(["mode_group"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2019.columns = ["mode_group", "distance", "nbre_trajets"]
        long_trips_2019["distance"] = long_trips_2019["distance"] / long_trips_2019["nbre_trajets"]
        
        long_trips_2008 = pd.merge(long_trips_2008, mode_transport, on="mode_id", how='left')
        long_trips_2008 = long_trips_2008.groupby(["mode_group"], as_index=False).agg({"distance":['sum'], "individual_id": ["count"]})
        long_trips_2008.columns = ["mode_group", "distance", "nbre_trajets"]
        long_trips_2008["distance"] = long_trips_2008["distance"] / long_trips_2008["nbre_trajets"]       
        
        long_trips_2019["survey"]="EMP-2019"
        long_trips_2008["survey"]="ENTD-2008"
        long_trips = pd.concat([long_trips_2008,long_trips_2019])
        
        
        fig = go.Figure(
            
            data=[
                go.Bar(name='ENTD-2008', x=long_trips_2008["mode_group"].tolist(), y=long_trips_2008["distance"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
                go.Bar(name='EMP-2019', x=long_trips_2019["mode_group"].tolist(), y=long_trips_2019["distance"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
                ],

            layout={
                'yaxis': {'title': ''}
                
            }
        )
        fig.update_layout(
           
            template="simple_white",
            font=dict(family="Gilroy Light",size=12),
            title_text="distance par trajet (km) en fonction du mode de déplacement", title_x=0.5
            )
        
       
        fig.update_layout(barmode='group')
        fig.show(renderer="png")  
        



def taux_remplissage() :
    """
    Taux de remplissage moyen des voitures en fonction des motifs de déplacement

    Returns
    -------
    None.

    """
 
    
    long_trips_2019 = survey_data_2019["long_trips"]
    long_trips_2019["individual_id"]="1"
    long_trips_2019 = pd.merge(long_trips_2019, motive_group, on="motive", how='left')
    long_trips_2008 = survey_data_2008["long_trips"]
    long_trips_2008["individual_id"]="1"
    long_trips_2008 = pd.merge(long_trips_2008, motive_group, on="motive", how='left')    
    

    # on regarde uniquement les individus possédant une voiture
    long_trips_2019 = long_trips_2019.loc[long_trips_2019["n_cars"]!="0"]
    long_trips_2008 = long_trips_2008.loc[long_trips_2008["n_cars"]!="0"]
    
    # on regarde uniquement les trajets en voiture
    long_trips_2019 = long_trips_2019.loc[long_trips_2019["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]
    long_trips_2008 = long_trips_2008.loc[long_trips_2008["mode_id"].isin(["3.30", "3.31", "3.32", "3.33", "3.39"])]
    
    long_trips_2019["n_other_passengers"] = long_trips_2019["n_other_passengers"].astype(int) + 1
    long_trips_2019["n_other_passengers"] = long_trips_2019["n_other_passengers"] * long_trips_2019["pondki"]  
    long_trips_2019 = long_trips_2019.groupby(["motive_group"], as_index=False).agg({"n_other_passengers":['sum'], "pondki":['sum'], "individual_id": ["count"]})
    long_trips_2019["n_other_passengers"] = long_trips_2019["n_other_passengers"] / long_trips_2019["pondki"]
    long_trips_2019["survey"]="EMP-2019"
    long_trips_2019.columns = ["motive_group", "n_other_passengers", "pondki", "individual_id", "survey"]
    
    long_trips_2008["n_other_passengers"] = long_trips_2008["n_other_passengers"].astype(int) + 1
    long_trips_2008["n_other_passengers"] = long_trips_2008["n_other_passengers"] * long_trips_2008["pondki"]
    long_trips_2008 = long_trips_2008.groupby(["motive_group"], as_index=False).agg({"n_other_passengers":['sum'],"pondki":['sum'], "individual_id": ["count"]})
    long_trips_2008["n_other_passengers"] = long_trips_2008["n_other_passengers"] / long_trips_2008["pondki"]
    long_trips_2008["survey"]="ENTD-2008"
    long_trips_2008.columns = ["motive_group", "n_other_passengers", "pondki", "individual_id", "survey"]
    
    long_trips = pd.concat([long_trips_2008,long_trips_2019])
    

    
        
    fig = go.Figure(
        
        data=[
            go.Bar(name='ENTD-2008', x=long_trips_2008["motive_group"].tolist(), y=long_trips_2008["n_other_passengers"].tolist(), yaxis='y', offsetgroup=1, marker=dict(color = "rgb(236, 0, 141)")),
            go.Bar(name='EMP-2019', x=long_trips_2019["motive_group"].tolist(), y=long_trips_2019["n_other_passengers"].tolist(), yaxis='y', offsetgroup=2, marker=dict(color = "rgb(11, 28, 45)"))
            ],

        layout={
            'yaxis': {'title': '(nombre de personnes / voiture)'}
            
        }
    )
    fig.update_layout(
       
        template="simple_white",
        font=dict(family="Gilroy Light",size=12),
        title_text="taux de remplissage moyen des voitures par motifs de déplacement", title_x=0.5
        )
    
    fig.update_layout(barmode='group')
    fig.show(renderer="png")  
        


def mode_trajet () :
    """
    

    Returns
    -------
    None.

    """
    
    long_trips_2019 = survey_data_2019["long_trips"]
    long_trips_2019["individual_id"]="1"
    long_trips_2019 = pd.merge(long_trips_2019, motive_group, on="motive", how='left')
    long_trips_2019 = pd.merge(long_trips_2019, mode_transport, on="mode_id", how='left')
    long_trips_2008 = survey_data_2008["long_trips"]
    long_trips_2008["individual_id"]="1"
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
    mode_df = pd.DataFrame (mode, columns = ['mode_id'])
    
    distance_2019 = []
    
    long_trips = long_trips_2019.copy()
    
    for k in mode :
        long_trips = long_trips.loc[long_trips["mode_id"]==k]
        long_trips["distance"] = long_trips["distance"] * long_trips["pondki"]
        distance = long_trips["distance"].sum()
        distance_2019.append(distance)
        long_trips = long_trips_2019.copy()
    
    distance_2019 = pd.DataFrame (distance_2019, columns = ['distance'])
    distance_2019["mode_id"] = mode_df["mode_id"]
    distance_2019 ["distance"] = distance_2019["distance"] / distance_totale_2019
    

    distance_2008 = []
    long_trips = long_trips_2008.copy()    
    
    for k in mode :
        long_trips = long_trips.loc[long_trips["mode_id"]==k]
        long_trips["distance"] = long_trips["distance"] * long_trips["pondki"]
        distance = long_trips["distance"].sum()
        distance_2008.append(distance)
        long_trips = long_trips_2008.copy()
    
    distance_2008 = pd.DataFrame (distance_2008, columns = ['distance'])
    distance_2008["mode_id"] = mode_df["mode_id"]
    distance_2008 ["distance"] = distance_2008["distance"] / distance_totale_2008
    
    
    distance_2008["survey"]="ENTD-2008"
    distance_2019["survey"]="EMP-2019"
    distance_totale = pd.concat([distance_2008, distance_2019])
    
    distance_totale = pd.merge(distance_totale, mode_transport, on="mode_id", how='left')
    distance_totale = distance_totale.groupby(["mode_group","survey"], as_index=False).agg({"distance":['sum']})
    distance_totale.columns = ["mode_group", "survey", "distance"]

    
    distance_totale["distance"]=round(distance_totale["distance"],2)
    
    fig = px.bar(distance_totale, x="survey", y="distance", color='mode_group', text_auto=True, color_discrete_map=color_mode)
    fig.update_layout(
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        template="simple_white",
        title_font=dict(size=15),
        title_font_family="Gill Sans MT",
        title="Répartition des distances parcourues en fonction des modes de déplacement",
        xaxis={'title':''},
        yaxis={'title':''},
        legend={'title':''}
        
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
    
    long_trips_2019["survey"]="EMP-2019"
    long_trips_2008["survey"]="ENTD-2008"
    long_trips = pd.concat([long_trips_2008,long_trips_2019])
    
    long_trips["ratio"]=round(long_trips["ratio"],2)
    

    
    fig = px.bar(long_trips, x="survey", y="ratio", color='mode_group', text_auto=True, color_discrete_map=color_mode)
    fig.update_layout(

        font_family="Arial",
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        template="simple_white",
        title={
        'text': "Répartition des trajets en fonction des modes de transports",
        'y':0.95,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        title_font=dict(size=15),
        
        xaxis={'title':''},
        yaxis={'title':'distance (km)'},
        legend={'title':''}

                      )
    
    fig.show(renderer="png")


    fig.update_layout(
        
                      )

