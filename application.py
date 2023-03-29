import streamlit as st
import json
from streamlit_lottie import st_lottie
from streamlit_option_menu import option_menu
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd 
import seaborn as sns
from mobility.get_insee_data import get_insee_data
from mobility.parsers import download_work_home_flows
import mobility.radiation_model as rm
import numpy as np
import pandas as pd
import os
import time
from pathlib import Path
from mobility.parsers import communes
import time
import random
#==============================================Start=============================================================
import pandas as pd
depart=pd.read_csv("departements-france.csv",usecols=['code_departement','nom_departement'])

#========================================FUNCTION==========================================================

# Interface Application------------------------------------------------------------------------------------------
st.write(""" ## Mobility project""")

selected=option_menu(
    menu_title="Main Menu",
    options=["Home","Mobilty"],
    icons=["house","bar-chart"],
    menu_icon="cast",  # optional
    default_index=0,
    orientation="horizontal",  
    styles={
        "nav-link-selected": {"background-color": "#4B9DFF"},
    } 

     )
   

#========================================================Accueil===========================================
if selected=="Home":
    # creer une animation
    def load_lottiefile(filepath: str):
        with open(filepath, "r") as f:
            return json.load(f)
    lottie_coding = load_lottiefile("images\pc.json")  # replace link to local lottie file
    st_lottie(
    lottie_coding,
    speed=1,
    reverse=False,
    loop=True,
    quality="low", # medium ; high

    height=None,
    width=None,
    key=None,
)
#======================================================Data Overview=======================================
if selected=="Mobilty":

    st.sidebar.image('images\logo.png', width=300)
    #selected_option = st.sidebar.selectbox(" ", ["Select","Scipy", "Genetique", "Recuit simulé"])
    depart = pd.read_csv("departements-france.csv", usecols=['code_departement', 'nom_departement'])

    # Create a multiselect widget for 'nom_departement'
    selected_deps = st.sidebar.multiselect("Select departments", list(depart['nom_departement'].unique()))

    # Filter the data based on the selected departments
    filtered_depart = depart[depart['nom_departement'].isin(selected_deps)]

    # Get the values for 'code_departement' as a list of strings
    selected_codes = filtered_depart['code_departement'].astype(str).tolist()

    # Display the selected codes as a comma-separated string
    #st.success("Vous avez selectionné les departements suivants : {}".format(selected_deps))
    options = ["jobs", "shops", "schools", "admin", "sport", "care", "show", "museum", "restaurant"]
    selected_option = st.sidebar.selectbox("Select an option", options)

    st.success("You have selected the {} motif".format(selected_option))
    
    COMMUNES_COORDINATES_CSV = "donneesCommunesFrance.csv"
    COMMUNES_SURFACES_CSV = "donneesCommunesFrance.csv"
    WORK_HOME_FLUXES_CSV = download_work_home_flows()
    def compare_thresholds(
    predicted_flux, empirical_flux, thresholds=[400, 200, 100, 50, 25, 20, 15, 10, 5]
     ):
        for t in thresholds:
            compute_similarity_index(predicted_flux, empirical_flux, t)


    def compute_similarity_index(predicted_flux, empirical_flux, threshold=200):
        ssi = 0
        n = 0
        under_threshold = 0
        exc = 0
        for pair in empirical_flux.iterrows():
            pair_ids = pair[0]
            t2_ij = float(pair[1])
            if t2_ij >= threshold:
                try:
                    t_ij = float(predicted_flux.loc[pair_ids])
                    if t_ij + t2_ij > 0:
                        ssi += 2 * min(t_ij, t2_ij) / (t_ij + t2_ij)
                        n += 1
                except KeyError:
                    exc += 1
                    n += 1
                    pass
            else:
                under_threshold += 1
        ssi = ssi / n
        ssi_print = f"{ssi:.3f}"
        print(
            "Similarity index:",
            ssi_print,
            "after",
            n,
            "steps.",
            under_threshold,
            "pairs under a threshold of",
            threshold,
            "--",
            exc,
            "exceptions",
        )
        return ssi
    def run_model_for_territory(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordonnees,
    raw_flowDT,
    alpha=0,
    beta=1,
    ):
        print(
            "Model running with {} sources, {} sinks and {} costs".format(
                len(sources_territory), len(sinks_territory), len(costs_territory)
            )
        )
        print("still")

        # COMPUTE THE MODEL
        (total_flows, source_rest_volume, sink_rest_volume) = rm.iter_radiation_model(
            sources_territory,
            sinks_territory,
            costs_territory,
            alpha=alpha,
            beta=beta,
            plot=False,
        )
        print("ok")
        # PLOT THE SOURCES AND THE SINKS

        plot_sources = sources_territory.rename(columns={"source_volume": "volume"})
        rm.plot_volume(plot_sources, coordonnees, n_locations=10, title="Volume d'actifs")
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()
        plot_sinks = sinks_territory.rename(columns={"sink_volume": "volume"})
        rm.plot_volume(plot_sinks, coordonnees, n_locations=10, title="Volume {}".format(selected_option))
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()

        # PLOT THE FLOWS COMPUTED BY THE MODEL
        print("yes")
        plot_flows = total_flows.reset_index()
        plot_sources = sources_territory

        rm.plot_flow(
            plot_flows,
            coordonnees,
            sources=None,
            n_flows=500,
            n_locations=20,
            size=10,
            title=(
                "(1) Flux {} générés par le modèle"
                " - alpha = {} - beta = {}"
            ).format(selected_option,alpha, beta),
        )
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()

        # PLOT THE FLOWS FROM THE INSEE DATA
        print("no")
        plot_flowDT = raw_flowDT.groupby(["COMMUNE", "DCLT"])["IPONDI"].sum().reset_index()
        plot_flowDT.rename(
            columns={"IPONDI": "flow_volume", "COMMUNE": "from", "DCLT": "to"}, inplace=True
        )

        #rm.plot_flow(plot_flowDT,coordonnees,sources=plot_sources,n_flows=500,n_locations=20,size=10,title="(2) Flux {} mesurés par l'INSEE".format(selected_option),)
        #st.set_option('deprecation.showPyplotGlobalUse', False)
        #st.pyplot()

        # EXPORT THE MODEL AND THE INSEE DATA

        flowDT = raw_flowDT.rename(
            columns={"IPONDI": "flow_volume", "COMMUNE": "from", "DCLT": "to"}
        )
        flowDT = flowDT.groupby(["from", "to"])["flow_volume"].sum()
        flowDT = pd.DataFrame(flowDT)

        flowsRM = pd.DataFrame(total_flows)

        print("Model flow of {} and empirical flow of {}".format(len(flowsRM), len(flowDT)))

        return flowsRM, flowDT, coordonnees, plot_sources
    
    def optimise_parameters(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordonnees,
    raw_flowDT,
    coef=1,
    threshold=20,
):
        best_score = 0
        print("Finding the best α,β pair")
        for alpha in np.arange(0, 1.1, 0.1):
            for beta in np.arange(0, 1.1, 0.1):
                if alpha + beta < 1.05:
                    print(f"\n\nα = {alpha:.1f}, β ={beta:.1f}")
                    (
                        predicted_flux,
                        empirical_flux,
                        coordonnees,
                        plot_sources,
                    ) = run_model_for_territory(
                        sources_territory.copy(),
                        sinks_territory,
                        costs_territory,
                        coordonnees,
                        raw_flowDT,
                        alpha=alpha,
                        beta=beta,
                    )

                    ssi = compute_similarity_index(
                        coef * predicted_flux, empirical_flux, threshold
                    )
                    if ssi > best_score:
                        best_score = ssi
                        best_pair = [alpha, beta]
        print("Best α,β pair found is", best_pair)
        return best_pair
    
    def compare_insee_and_model(predicted_flux, empirical_flux, coordonnees, plot_sources):
    
        # Join on the couple origin destinations
        # how = 'inner' to keep only the couples that are in both dataframes
        flow_join = empirical_flux.join(
            predicted_flux, how="inner", lsuffix="DT", rsuffix="RM"
        )
        flow_join.reset_index(inplace=True)

        # Compare visually model and INSEE data
        fig = flow_join.plot(x="flow_volumeDT", y="flow_volumeRM", style="o")
        # fig.set_xlim(0,2500) # Use this to set the limit of the figure if needed
        # fig.set_ylim(0,2500)

        # Compare the total flow
        print("The 2 dataframes have {} OD in common\n".format(flow_join.shape[0]))

        sum_flow_DT = flow_join["flow_volumeDT"].sum()
        intra_flow_mask = flow_join["from"] == flow_join["to"]
        intra_flow_DT = flow_join.loc[intra_flow_mask, "flow_volumeDT"].sum()
        print(
            "Total flow of the INSEE data :\n"
            "   {:.0f} ({:.0f}% intra-city flow)".format(
                sum_flow_DT, 100 * intra_flow_DT / sum_flow_DT
            )
        )

        sum_flow_RM = flow_join["flow_volumeRM"].sum()
        intra_flow_RM = flow_join.loc[intra_flow_mask, "flow_volumeRM"].sum()
        intra_city_flow = 100 * intra_flow_RM / sum_flow_RM
        print(
            "Total flow of the model :\n"
            "   {:.0f} ({:.0f}% intra-city flow)\n".format(sum_flow_RM, intra_city_flow)
        )

        # Compare the repartition between the ODs
        flow_join["repartitionDT"] = (
            flow_join["flow_volumeDT"] / flow_join["flow_volumeDT"].sum()
        )
        flow_join["repartitionRM"] = (
            flow_join["flow_volumeRM"] / flow_join["flow_volumeRM"].sum()
        )

        error_repartition = np.abs(flow_join["repartitionDT"] - flow_join["repartitionRM"])

        print(
            "The repartitions from the INSEE data and the data"
            "have {:.2f}% in common.".format(100 - 50 * error_repartition.sum())
        )

        # similarity = compute_similarity_index(flowRM,flowDT)
        # print("Similarity between the model and the INSEE data is ", similarity)

        plot_DT = pd.DataFrame(flow_join[["from", "to", "repartitionDT"]])
        plot_DT.rename(columns={"repartitionDT": "flow_volume"}, inplace=True)
        plot_RM = pd.DataFrame(flow_join[["from", "to", "repartitionRM"]])
        plot_RM.rename(columns={"repartitionRM": "flow_volume"}, inplace=True)

        #rm.plot_flow(plot_DT,coordonnees,sources=plot_sources,n_flows=500,size=10,n_locations=20,title="(3) Flux {} mesurés par l'INSEE".format(selected_option),)

        #st.set_option('deprecation.showPyplotGlobalUse', False)
        #st.pyplot()
        rm.plot_flow(
            plot_RM,
            coordonnees,
            sources=plot_sources,
            n_flows=500,
            size=10,
            n_locations=20,
            title="Flux {} générés par le modèle ".format(selected_option),
        )
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()
        return error_repartition


    def get_data_for_model(selected_option,
        lst_departments,
        communes_coordinates_csv=COMMUNES_COORDINATES_CSV,
        communes_surfaces_csv=COMMUNES_SURFACES_CSV,
        alpha=0,
        beta=1,
    ):
        
        # ===================
        # IMPORT AND PROCESS THE DATA

            # Import the data (active population and jobs)
            insee_data = get_insee_data()
            db_actifs = insee_data["active_population"]
            db_emplois = insee_data[selected_option]
            raw_flowDT=insee_data["raw_flowDT"]
            
            #coordonnees=insee_data["coordonnees"]
            #surf=insee_data["coordonnees"]
           # db_sport = db_sport.rename_axis('CODGEO')
            if selected_option=="jobs":
                db_emplois["EMPLT"] = db_emplois[
                    [
                        "n_jobs_CS1",
                        "n_jobs_CS2",
                        "n_jobs_CS3",
                        "n_jobs_CS4",
                        "n_jobs_CS5",
                        "n_jobs_CS6",
                    ]
                ].sum(axis=1)
                db_emplois.reset_index(inplace=True)
                # Only keep the sinks in the chosen departements
                sinks_territory = db_emplois.loc[:, ["CODGEO", "EMPLT"]]
                sinks_territory["DEP"] = sinks_territory["CODGEO"].str.slice(0, 2)
                mask = sinks_territory["DEP"].apply(lambda x: x in lst_departments)
                sinks_territory = sinks_territory.loc[mask]
                
                sinks_territory = sinks_territory.set_index("CODGEO")
                sinks_territory.rename(columns={"EMPLT": "sink_volume"}, inplace=True)
                sinks_territory = sinks_territory.drop(columns=["DEP"])
            else :
                db_emplois = db_emplois.rename_axis('CODGEO')
                sinks_territory= db_emplois

            
            
            db_actifs["ACT"] = db_actifs[
                [
                    "active_pop_CS1",
                    "active_pop_CS2",
                    "active_pop_CS3",
                    "active_pop_CS4",
                    "active_pop_CS5",
                    "active_pop_CS6",
                ]
            ].sum(axis=1)
            db_actifs.reset_index(inplace=True)
            
            
            
            # Only keep the sinks in the chosen departements
            sources_territory = db_actifs.loc[:, ["CODGEO", "ACT"]]
            sources_territory["DEP"] = sources_territory["CODGEO"].str.slice(0, 2)
            mask = sources_territory["DEP"].apply(lambda x: x in lst_departments)
            sources_territory = sources_territory.loc[mask]

            sources_territory = sources_territory.set_index("CODGEO")
            sources_territory = sources_territory.drop(columns=["DEP"])
            sources_territory.rename(columns={"ACT": "source_volume"}, inplace=True)

            data_folder_path = Path(os.path.dirname("examples/Millau/"))

            # Import the INSEE data on the work-home mobility on Millau
            # Only keep the flows in the given departments

            raw_flowDT["DEP"] = raw_flowDT["COMMUNE"].str.slice(0, 2)
            raw_flowDT["DEP2"] = raw_flowDT["DCLT"].str.slice(0, 2)
            mask = raw_flowDT["DEP"].apply(lambda x: x in lst_departments)
            mask2 = raw_flowDT["DEP2"].apply(lambda x: x in lst_departments)
            raw_flowDT = raw_flowDT.loc[mask]
            raw_flowDT = raw_flowDT.loc[mask2]

            # Import the geographic data on the work-home mobility on Millau
            
            # Import the geographic data on the work-home mobility on Millau

            coordonnees = pd.read_csv(
                data_folder_path / communes_coordinates_csv,
                sep=",",
                usecols=["NOM_COM", "INSEE_COM", "x", "y"],
                dtype={"INSEE_COM": str},
            )
            coordonnees.set_index("INSEE_COM", inplace=True)
            coordonnees["x"] = coordonnees["x"] * 1000
            coordonnees["y"] = coordonnees["y"] * 1000

            surfaces = pd.read_csv(
                data_folder_path / communes_surfaces_csv,
                sep=",",
                usecols=["INSEE_COM", "distance_interne"],
                dtype={"INSEE_COM": str},
            )
            surfaces.set_index("INSEE_COM", inplace=True)

            # Compute the distance between cities
            #    distance between i and j = (x_i - x_j)**2 + (y_i - y_j)**2
            lst_communes = sources_territory.index.to_numpy()
            idx_from_to = np.array(np.meshgrid(lst_communes, lst_communes)).T.reshape(-1, 2)
            idx_from = idx_from_to[:, 0]
            idx_to = idx_from_to[:, 1]
            costs_territory = pd.DataFrame(
                {"from": idx_from, "to": idx_to, "cost": np.zeros(idx_to.shape[0])}
            )
            costs_territory = pd.merge(
                costs_territory, coordonnees, left_on="from", right_index=True
            )
            costs_territory.rename(columns={"x": "from_x", "y": "from_y"}, inplace=True)
            costs_territory = pd.merge(
                costs_territory, coordonnees, left_on="to", right_index=True
            )
            costs_territory.rename(columns={"x": "to_x", "y": "to_y"}, inplace=True)

            costs_territory = pd.merge(
                costs_territory, surfaces, left_on="from", right_index=True
            )

            costs_territory["cost"] = np.sqrt(
                (costs_territory["from_x"] / 1000 - costs_territory["to_x"] / 1000) ** 2
                + (costs_territory["from_y"] / 1000 - costs_territory["to_y"] / 1000) ** 2
            )

            # distance if the origin and the destination is the same city
            # is internal distance = 128*r / 45*pi
            # where r = sqrt(surface of the city)/pi
            mask = costs_territory["from"] != costs_territory["to"]
            costs_territory["cost"].where(
                mask, other=costs_territory["distance_interne"], inplace=True
            )

            return (
                sources_territory,
                sinks_territory,
                costs_territory,
                coordonnees,
                raw_flowDT,
            )
    # CHOOSE DEPARTMENTS
    lst_departments = selected_codes
    m = st.markdown("""
            <style>
            div.stButton > button:first-child {
                background-color: #0099ff;
                color:#ffffff;
            }
            div.stButton > button:hover {
                background-color: #00ff00;
                color:#ff0000;
                }
            </style>""", unsafe_allow_html=True)
    boutton=st.button("Run")
    if boutton:
        start_time = time.time()

        # GET DATA
        (
            sources_territory,
            sinks_territory,
            costs_territory,
            coordonnees,
            raw_flowDT,
        ) = get_data_for_model(selected_option,lst_departments)
    # FIRST RUN
        (
            predicted_flux,
            empirical_flux,
            coordonnees,
            plot_sources) = run_model_for_territory(
            sources_territory.copy(),
            sinks_territory,
            costs_territory,
            coordonnees,
            raw_flowDT,
            alpha=0,
            beta=1,
        )

        # COMPARE INSEE AND MODEL DATA
        compare_insee_and_model(predicted_flux, empirical_flux, coordonnees, plot_sources)
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()
        compare_thresholds(predicted_flux, empirical_flux)
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()
        compute_similarity_index(predicted_flux, empirical_flux, threshold=20)
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot()

    


            
                    
                        


                
                
    

    #============================================FIN=========================================================
    