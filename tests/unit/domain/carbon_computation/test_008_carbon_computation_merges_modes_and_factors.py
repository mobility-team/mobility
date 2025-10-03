import numpy as np
import pandas as pd

from mobility import carbon_computation as cc


def test_carbon_computation_merges_and_computes(monkeypatch):
    """
    Pure in-memory test:
      - stub get_ademe_factors -> tiny factors frame
      - stub pandas.read_excel  -> tiny modes frame
      - stub pandas.read_csv for 'mapping.csv' -> mapping frame
    Then verify passenger correction for car and direct factor for bus.
    """
    # Prepare tiny in-memory tables
    modes_dataframe = pd.DataFrame(
        {
            "mode_id": ["31", "21"],
            "ef_name": ["car_thermique", "bus_articule"],
        },
        dtype="object",
    )

    mapping_dataframe = pd.DataFrame(
        {"ef_name": ["car_thermique", "bus_articule"], "ef_id": ["EF1", "EF2"]},
        dtype="object",
    )

    ademe_factors_dataframe = pd.DataFrame(
        {
            "ef_id": ["EF1", "EF2"],
            "ef": [0.200, 0.100],
            "unit": ["kgCO2e/km", "kgCO2e/p.km"],
            "database": ["ademe", "ademe"],
        }
    )

    # Monkeypatch the I/O boundaries
    original_read_csv_function = pd.read_csv

    def fake_read_excel(*args, **kwargs):
        # cc reads the modes Excel with dtype=str; we return our modes
        return modes_dataframe.copy()

    def selective_read_csv(file_path, *args, **kwargs):
        # Return mapping when reading mapping.csv, else fall back to real pandas.read_csv
        if str(file_path).endswith("mapping.csv"):
            return mapping_dataframe.copy()
        return original_read_csv_function(file_path, *args, **kwargs)

    monkeypatch.setattr(cc.pd, "read_excel", fake_read_excel, raising=True)
    monkeypatch.setattr(cc.pd, "read_csv", selective_read_csv, raising=True)
    monkeypatch.setattr(cc, "get_ademe_factors", lambda _path: ademe_factors_dataframe.copy(), raising=True)

    # Trips input
    trips_dataframe = pd.DataFrame(
        {"mode_id": ["31", "21"], "distance": [10.0, 5.0], "n_other_passengers": [1, 0]},
        dtype=object,
    )

    result_dataframe = cc.carbon_computation(trips_dataframe, ademe_database="Base_Carbone_V22.0.csv")

    assert set(result_dataframe.columns) >= {
        "mode_id", "distance", "n_other_passengers", "ef", "database", "k_ef", "carbon_emissions"
    }

    # Car passenger correction: 1 other passenger -> factor halved
    expected_car_emissions = 0.200 * 10.0 * 0.5
    expected_bus_emissions = 0.100 * 5.0 * 1.0

    car_row = result_dataframe.loc[result_dataframe["mode_id"].eq("31")].iloc[0]
    bus_row = result_dataframe.loc[result_dataframe["mode_id"].eq("21")].iloc[0]

    assert np.isclose(float(car_row["carbon_emissions"]), expected_car_emissions)
    assert np.isclose(float(bus_row["carbon_emissions"]), expected_bus_emissions)
    assert result_dataframe["database"].isin(["ademe", "custom"]).any()

