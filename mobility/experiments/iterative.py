import os
import dotenv
import mobility as mobility
import pandas as pd
import numpy as np
import pathlib
import geopandas as gpd

from mobility import CarMode, WalkMode, BicycleMode, PublicTransportMode, ModalShift
from mobility import PublicTransportRoutingParameters
from mobility.concat_costs import concat_generalized_cost, concat_travel_costs

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    debug=True
)

# 12202
transport_zones = mobility.TransportZones("fr-12202", radius=40.0, level_of_detail=1)




ssi = []


for car_vot in [0.0]:
    
    walk = mobility.WalkMode(
        transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_constant=0.0,
            cost_of_distance=0.0,
            cost_of_time=mobility.CostOfTimeParameters(
                intercept=14.0,
                breaks=[0.0, 2.0, 10.0, 10000.0],
                slopes=[0.0, (18.0-14.0)/(6.0-2.0), (26.0-22.0)/(50.0-10.0)],
                max_value=26.0
            )
        )
    )

    bicycle = mobility.BicycleMode(
        transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_constant=0.0,
            cost_of_distance=0.0,
            cost_of_time=mobility.CostOfTimeParameters(
                intercept=28.0,
                breaks=[0.0, 2.0, 10.0, 10000.0],
                slopes=[0.0, (34.0-28.0)/(6.0-2.0), (41.0-40.0)/(50.0-10.0)],
                max_value=41.0
            )
        )
    )

    car_cost_constant = 0.0
    car_cost_of_distance = 0.1

    car_cost_of_time = mobility.CostOfTimeParameters(
        intercept=9.0,
        breaks=[0.0, 2.0, 10.0, 10000.0],
        slopes=[0.0, (12.0-9.0)/(6.0-2.0), (27.0-15.0)/(50.0-10.0)],
        max_value=26.0
    )

    car = mobility.CarMode(
        transport_zones,
        generalized_cost_parameters=mobility.GeneralizedCostParameters(
            cost_constant=car_cost_constant,
            cost_of_distance=car_cost_of_distance,
            cost_of_time=car_cost_of_time
        ),
        congestion=True
    )

    pt_gen_cost_parms = generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=5.0,
        cost_of_distance=0.0,
        cost_of_time=mobility.CostOfTimeParameters(
            intercept=11.0,
            breaks=[0.0, 2.0, 10.0, 10000.0],
            slopes=[0.0, (15.0-11.0)/(6.0-2.0), (21.0-19.0)/(50.0-10.0)],
            max_value=21.0
        )
    )

    walk_pt = mobility.PublicTransportMode(
        transport_zones,
        first_leg_mode=walk,
        last_leg_mode=walk,
        first_modal_shift=ModalShift(
            max_travel_time=20.0/60.0,
            average_speed=5.0,
            shift_time=1.0
        ),
        last_modal_shift=ModalShift(
            max_travel_time=20.0/60.0,
            average_speed=5.0,
            shift_time=1.0
        ),
        generalized_cost_parameters=pt_gen_cost_parms
    )

    modes = [
        walk,
        car,
        walk_pt,
        bicycle
    ]


    work_dest_parms = mobility.WorkDestinationChoiceModelParameters(
        model={
            "type": "radiation",
            "lambda": 0.9998,
            "end_of_contract_rate": 0.001,
            "job_change_utility_constant": -5.0,
            "max_iterations": 20,
            "tolerance": 0.005,
            "cost_update": True
        }
    )
    
    work_dest_cm = mobility.WorkDestinationChoiceModel(
        transport_zones,
        modes=modes,
        parameters=work_dest_parms
    )
    
    work_dest_cm.get()
    
    
    flows = pd.read_parquet(work_dest_cm.cache_path["od_flows"])
    
    comparison = work_dest_cm.get_comparison()
    work_dest_cm.plot_model_fit(comparison[comparison["flow_volume"] > 1.0])
    work_dest_cm.compute_ssi(comparison, 100)
    work_dest_cm.compute_ssi(comparison, 200)
    work_dest_cm.compute_ssi(comparison, 400)
    work_dest_cm.compute_ssi(comparison, 1000)
    
    tc = car.travel_costs.get()
    
    
    comparison.groupby("local_admin_unit_id_from")[["flow_volume", "ref_flow_volume"]].sum().sum()
    x = comparison.groupby("local_admin_unit_id_to")[["flow_volume", "ref_flow_volume"]].sum()
    
    
    ssi.append([car_vot, work_dest_cm.compute_ssi(comparison, 200)])



print(ssi)


comparison = comparison[(comparison["ref_flow_volume"] > 200.0) | (comparison["flow_volume"] > 200.0)]
comparison["delta"] = comparison["flow_volume"] - comparison["ref_flow_volume"]
comparison["relative_error"] = comparison["delta"]/comparison["ref_flow_volume"]

rmse = np.sqrt((comparison["delta"].pow(2)).sum()/comparison.shape[0])
nrmse = rmse/comparison["ref_flow_volume"].mean()

mape = (comparison["delta"].abs()/comparison["ref_flow_volume"]).mean()

r2 = 1.0 - comparison["delta"].pow(2).sum()/(comparison["ref_flow_volume"] - comparison["ref_flow_volume"].mean()).pow(2).sum()
