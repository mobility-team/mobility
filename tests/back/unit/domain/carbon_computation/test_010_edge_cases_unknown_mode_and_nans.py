import numpy as np
import pandas as pd

from mobility import carbon_computation as cc


def test_unknown_mode_uses_custom_zero_and_noncar_nan_passengers(monkeypatch):
    """
    - Mode mapped to 'zero' gets emission factor 0 (custom).
    - Non-car (mode_id not starting with '3') ignores NaN passengers and uses k_ef == 1.0.
    """
    modes_dataframe = pd.DataFrame(
        {"mode_id": ["99", "21"], "ef_name": ["zero", "bus_articule"]},
        dtype="object",
    )
    mapping_dataframe = pd.DataFrame({"ef_name": ["bus_articule"], "ef_id": ["EF2"]}, dtype="object")
    ademe_factors_dataframe = pd.DataFrame(
        {"ef_id": ["EF2"], "ef": [0.050], "unit": ["kgCO2e/p.km"], "database": ["ademe"]}
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

    trips_dataframe = pd.DataFrame(
        {"mode_id": ["99", "21"], "distance": [3.0, 4.0], "n_other_passengers": [0, np.nan]},
        dtype=object,
    )
    result_dataframe = cc.carbon_computation(trips_dataframe)

    unknown_row = result_dataframe.loc[result_dataframe["mode_id"].eq("99")].iloc[0]
    bus_row = result_dataframe.loc[result_dataframe["mode_id"].eq("21")].iloc[0]

    assert np.isclose(float(unknown_row["ef"]), 0.0)
    assert np.isclose(float(unknown_row["carbon_emissions"]), 0.0)

    assert np.isclose(float(bus_row["k_ef"]), 1.0)
    assert np.isclose(float(bus_row["carbon_emissions"]), 0.050 * 4.0)

