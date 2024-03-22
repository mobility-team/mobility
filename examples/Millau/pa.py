from mobility.radiation_departments import *

dep =["64"]

(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordinates,
    raw_flowDT,
) = get_data_for_model(dep)

run_model_for_territory(sources_territory, sinks_territory, costs_territory, coordinates, raw_flowDT)
