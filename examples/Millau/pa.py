import sys
sys.path.insert(0,"../..")
import pandas as pd

from mobility.radiation_departments import *
from mobility.school_map import *

Age=2 # 1 pour les écoles marternelles et primaires, 2 pour les collèges et 3 pour les lycées
dep = ["59"]
model="radiation" #"radiation" pour utiliser le modèle de radiation, "proximity" pour utiliser le modèle de proximité, "school_map" pour utiliser le modèle de carte scolaire


(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordinates,
    raw_flowDT,
) = get_data_for_model_school(dep, Age)


flowsRM, flowDT, coordinates_res, plot_sources=run_model_for_territory(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT,model,"school")
ssi2=compute_similarity_index(flowsRM, flowDT)
print(ssi2)

