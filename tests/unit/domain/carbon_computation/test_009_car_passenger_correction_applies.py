import numpy as np
import pandas as pd

from mobility import carbon_computation as cc


def test_car_passenger_correction(monkeypatch):
    """
    Verify k_ef = 1 / (1 + n_other_passengers) applies only to car (mode_id starting with '3').
    """
    modes_dataframe = pd.DataFrame({"mode_id": ["31"], "ef_name": ["car_thermique"]}, dtype="object")
    mapping_dataframe = pd.DataFrame({"ef_name": ["car_thermique"], "ef_id": ["EF1"]}, dtype="object")
    ademe_factors_dataframe = pd.DataFrame(
        {"ef_id": ["EF1"], "ef": [0.180], "unit": ["kgCO2e/km"], "database": ["ademe"]}
    )

    original_read_csv_function = pd.read_csv

    monkeypatch.setattr(cc.pd, "read_excel", lambda *a, **k: modes_dataframe.copy(), raising=True)
    monkeypatch.setattr(
        cc.pd,
        "read_csv",
        lambda path, *a, **k: mapping_dataframe.copy() if str(path).endswith("mapping.csv") else original_read_csv_function(path, *a, **k),
        raising=True,
    )
    monkeypatch.setattr(cc, "get_ademe_factors", lambda _path: ademe_factors_dataframe.copy(), raising=True)

    trips_dataframe = pd.DataFrame({"mode_id": ["31"], "distance": [20.0], "n_other_passengers": [1]}, dtype=object)
    result_dataframe = cc.carbon_computation(trips_dataframe)

    expected_emissions_value = 0.180 * 20.0 * 0.5
    assert np.isclose(float(result_dataframe["carbon_emissions"].iloc[0]), expected_emissions_value)
    assert np.isclose(float(result_dataframe["k_ef"].iloc[0]), 0.5)

