import dotenv
import mobility as mobility

from create_modes import create_modes
from create_2024_model import create_2024_model

dotenv.load_dotenv()

mobility.set_params(debug=True)

# Prepare transport zones
transport_zones = mobility.TransportZones("fr-74166", level_of_detail=1, radius=10)

# Choice model params
work_dest_parms = mobility.WorkDestinationChoiceModelParameters(
    model={
        "type": "radiation",
        "lambda": 0.99986,
        "end_of_contract_rate": 0.00,
        "job_change_utility_constant": -5.0,
        "max_iterations": 6,
        "tolerance": 0.01,
        "cost_update": True,
        "n_iter_cost_update": 3
    },
    utility={
        "fr": 0.0,
        "ch": 5.0
    }
)

# Mode constants
constants = {
    "walk": 0.0,
    "bicycle": 2.0,
    "public_transport": 0.0,
    "car": 1.0,
    "carpool": 0.0
}

# Year 2024
modes_2024 = create_modes(transport_zones, constants, congestion=True, gtfs_paths=[])
model_2024 = create_2024_model(transport_zones, modes_2024, work_dest_parms)

modes = [m for m in modes_2024.values()]

work_dest_cm = mobility.WorkDestinationChoiceModel(
    transport_zones,
    modes=modes,
    parameters=work_dest_parms
)


mode_cm = mobility.TransportModeChoiceModel(work_dest_cm)

choice_model = mode_cm.get()

