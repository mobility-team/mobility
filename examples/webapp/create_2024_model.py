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

    mode_cm = mobility.TransportModeChoiceModel(work_dest_cm)

    
    return {
        "work_dest_choice_model": work_dest_cm,
        "mode_choice_model": mode_cm
    }