from pathlib import Path
import pandas as pd
from mobility.trips import Trips
from mobility.transport_modes.default_gwp import DefaultGWP


def test_get_population_trips_happy_path(
    fake_transport_zones,
    patch_mobility_survey,
    deterministic_shortuuid,
):
    population_dataframe = pd.DataFrame(
        {
            "individual_id": [10, 11],
            "transport_zone_id": [101, 102],
            "socio_pro_category": ["1", "1"],
            "ref_pers_socio_pro_category": ["1", "1"],
            "n_pers_household": ["2", "2"],
            "n_cars": ["1", "1"],
            "country": ["FR", "FR"],
        }
    )

    class DummyPopulationAsset:
        def __init__(self, transport_zones_asset):
            self.inputs = {"transport_zones": transport_zones_asset}
        def get(self):
            return {"individuals": "unused.parquet"}

    trips_instance = Trips(population=DummyPopulationAsset(fake_transport_zones["asset"]), gwp=DefaultGWP())

    mobility_survey_mapping = trips_instance.inputs["mobility_survey"].get()
    trips_instance.short_trips_db = mobility_survey_mapping["short_trips"]
    trips_instance.days_trip_db = mobility_survey_mapping["days_trip"]
    trips_instance.long_trips_db = mobility_survey_mapping["long_trips"]
    trips_instance.travels_db = mobility_survey_mapping["travels"]
    trips_instance.n_travels_db = mobility_survey_mapping["n_travels"]
    trips_instance.p_immobility = mobility_survey_mapping["p_immobility"]
    trips_instance.p_car = mobility_survey_mapping["p_car"]

    result_trips_dataframe = trips_instance.get_population_trips(
        population=population_dataframe,
        transport_zones=fake_transport_zones["transport_zones"],
        study_area=fake_transport_zones["study_area"],
    )

    expected_columns = {
        "trip_id", "mode_id", "distance", "n_other_passengers", "date",
        "previous_motive", "motive", "trip_type", "individual_id", "gwp"
    }
    assert expected_columns.issubset(set(result_trips_dataframe.columns))
    assert set(result_trips_dataframe["individual_id"].unique()) == {10, 11}
    assert (result_trips_dataframe["gwp"] >= 0).all()
