import streamlit as st
import json
from streamlit_lottie import st_lottie
from streamlit_option_menu import option_menu
import pandas as pd
import time
from pathlib import Path
from Millau_example import compare_thresholds, get_data_for_model, run_model_for_territory
from Millau_example import compute_similarity_index, compare_insee_and_model

# ==============================================Start=============================================================


depart = pd.read_csv(
    "departements-france.csv", usecols=["code_departement", "nom_departement"]
)

# ========================================FUNCTION==========================================================

# Interface Application------------------------------------------------------------------------------------------
st.write(""" ## Mobility project""")

selected = option_menu(
    menu_title="Main Menu",
    options=["Home", "Radiation model"],
    icons=["house", "bar-chart"],
    menu_icon="cast",  # optional
    default_index=0,
    orientation="horizontal",
    styles={"nav-link-selected": {"background-color": "#4B9DFF"}, },
)


# ========================================================Accueil===========================================
if selected == "Home":
    # creer une animation
    def load_lottiefile(filepath: str):
        with open(filepath, "r") as f:
            return json.load(f)

    lottie_coding = load_lottiefile(
        "images\pc.json"
    )  # replace link to local lottie file
    st_lottie(
        lottie_coding,
        speed=1,
        reverse=False,
        loop=True,
        quality="low",  # medium ; high
        height=None,
        width=None,
        key=None,
    )
# ======================================================Data Overview=======================================
if selected == "Radiation model":

    st.sidebar.image("images\logo.png", width=300)
    # selected_option = st.sidebar.selectbox(" ", ["Select","Scipy", "Genetique", "Recuit simulé"])
    depart = pd.read_csv(
        "departements-france.csv", usecols=["code_departement", "nom_departement"]
    )

    # Create a multiselect widget for 'nom_departement'
    selected_deps = st.sidebar.multiselect(
        "Select departments", list(depart["nom_departement"].unique())
    )

    # Filter the data based on the selected departments
    filtered_depart = depart[depart["nom_departement"].isin(selected_deps)]

    # Get the values for 'code_departement' as a list of strings
    selected_codes = filtered_depart["code_departement"].astype(str).tolist()

    # Display the selected codes as a comma-separated string
    # st.success("Vous avez selectionné les departements suivants : {}".format(selected_deps))
    options = [
        "jobs",
        "shops",
        "schools",
        "admin",
        "sport",
        "care",
        "show",
        "museum",
        "restaurant",
    ]
    selected_option = st.sidebar.selectbox("Select an option", options)

    st.success("You have selected the {} mobility purpose".format(selected_option))

    # CHOOSE DEPARTMENTS
    lst_departments = selected_codes
    m = st.markdown(
        """
            <style>
            div.stButton > button:first-child {
                background-color: #0099ff;
                color:#ffffff;
            }
            div.stButton > button:hover {
                background-color: #00ff00;
                color:#ff0000;
                }
            </style>""",
        unsafe_allow_html=True,
    )
    boutton = st.button("Run")
    if boutton:
        start_time = time.time()

        # GET DATA
        (
            sources_territory,
            sinks_territory,
            costs_territory,
            coordonnees,
            raw_flowDT,
        ) = get_data_for_model(selected_option, lst_departments)
        # FIRST RUN
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
            alpha=0,
            beta=1,
        )

        # COMPARE INSEE AND MODEL DATA
        compare_insee_and_model(
            predicted_flux, empirical_flux, coordonnees, plot_sources
        )
        st.set_option("deprecation.showPyplotGlobalUse", False)
        st.pyplot()
        compare_thresholds(predicted_flux, empirical_flux)
        st.set_option("deprecation.showPyplotGlobalUse", False)
        st.pyplot()
        compute_similarity_index(predicted_flux, empirical_flux, threshold=20)
        st.set_option("deprecation.showPyplotGlobalUse", False)
        st.pyplot()
    # ============================================FIN=========================================================

selected_option = "jobs"
lst_departments = ["12"]

"""(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordonnees,
    raw_flowDT,
) = get_data_for_model(lst_departments)

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
    alpha=0,
    beta=1,
)

print (raw_flowDT)"""
