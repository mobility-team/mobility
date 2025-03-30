import mobility
import os
import geopandas as gpd
import pandas as pd

def create_2024_model(transport_zones, modes, work_dest_parms, ls=0.99986):
    
    modes = [m for m in modes.values()]

    work_dest_cm = mobility.WorkDestinationChoiceModel(
        transport_zones,
        modes=modes,
        parameters=work_dest_parms
    )

    sp = mobility.ShoppingDestinationChoiceModelParameters()
    sp.model={"lambda":ls}
    sm = mobility.ShoppingDestinationChoiceModel(transport_zones, modes)
    t = transport_zones.get()
    lm = mobility.LeisureDestinationChoiceModel(transport_zones, modes)

    mode_cm = mobility.TransportModeChoiceModel(work_dest_cm)
    shops_modal_choice = mobility.TransportModeChoiceModel(sm)
    leisure_modal_choice = mobility.TransportModeChoiceModel(lm)
    
    return {
        "work_dest_choice_model": work_dest_cm,
        "mode_choice_model": mode_cm,
        "shops_model": sm,
        "shops_modal_choice": shops_modal_choice,
        "leisure_model": lm,
        "leisure_modal_choice": leisure_modal_choice
    }