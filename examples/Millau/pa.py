import sys
sys.path.insert(0,"../..")
import pandas as pd

from mobility.radiation_departments import *
from mobility.school_map import *


lst1=[["0"+str(x)] for x in range(1,10)]
lst2=[[str(x)] for x in range(10,96)]
lst=lst1+lst2
lst.pop(19)
lst.append(["2A"])
lst.append(["2B"])
departements =lst
Age=3

(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordinates,
    raw_flowDT,
) = get_data_for_model_school_multi(["55"])



flowsRM, flowDT, coordinates_res, plot_sources=run_model_for_territory(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT,"radiation","school")
ssi=compute_similarity_index(flowsRM, flowDT)

#Gros calcul radiation
cumul_proximity = pd.DataFrame(columns=['Département', 'école', 'collège','lycée','multi'])
for dep in departements :
    lst=[dep]
    for Age in range (1,4):
        (
            sources_territory,
            sinks_territory,
            costs_territory,
            coordinates,
            raw_flowDT,
        ) = get_data_for_model_school(dep,Age)
        flowsRM, flowDT, coordinates_res, plot_sources=run_model_for_territory(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT,"proximity","school",plot=False)
        ssi=compute_similarity_index(flowsRM, flowDT)
        lst.append(ssi)
    (
        sources_territory,
        sinks_territory,
        costs_territory,
        coordinates,
        raw_flowDT,
    ) = get_data_for_model_school_multi(dep)
    flowsRM, flowDT, coordinates_res, plot_sources=run_model_for_territory(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT,"proximity","school",plot=False)
    ssi=compute_similarity_index(flowsRM, flowDT)
    lst.append(ssi)
    cumul_proximity.loc[len(cumul_proximity)] = lst

cumul_proximity.to_csv("cumul_proximity.csv")