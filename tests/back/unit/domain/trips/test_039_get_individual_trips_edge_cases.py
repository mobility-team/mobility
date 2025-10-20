import pandas as pd
from mobility.trips import Trips
from mobility.transport_modes.default_gwp import DefaultGWP


def test_get_individual_trips_edge_cases_zero_mobile_days(
    seed_trips_with_minimal_databases,
    deterministic_shortuuid,
):
    class DummyPopulationAsset:
        def __init__(self):
            self.inputs = {"transport_zones": object()}
        def get(self):
            return {"individuals": "unused.parquet"}

    trips_instance = Trips(population=DummyPopulationAsset(), gwp=DefaultGWP())
    seed_trips_with_minimal_databases(trips_instance)

    trips_instance.p_immobility.loc[("FR", "1"), "immobility_weekday"] = 1.0
    trips_instance.p_immobility.loc[("FR", "1"), "immobility_weekend"] = 1.0

    simulation_year = 2025
    days_dataframe = pd.DataFrame({"date": pd.date_range(f"{simulation_year}-01-01", f"{simulation_year}-12-31", freq="D")})
    days_dataframe["month"] = days_dataframe["date"].dt.month
    days_dataframe["weekday"] = days_dataframe["date"].dt.weekday
    days_dataframe["day_of_year"] = days_dataframe["date"].dt.dayofyear

    trips_dataframe = trips_instance.get_individual_trips(
        csp="1",
        csp_household="1",
        urban_unit_category="C",
        n_pers="2",
        n_cars="1",
        country="FR",
        df_days=days_dataframe,
    )

    expected_columns = {
        "trip_id", "previous_motive", "motive", "mode_id", "distance",
        "n_other_passengers", "date", "trip_type"
    }
    assert expected_columns.issubset(set(trips_dataframe.columns))
    assert (trips_dataframe["trip_type"].isin(["long", "short"])).all()
