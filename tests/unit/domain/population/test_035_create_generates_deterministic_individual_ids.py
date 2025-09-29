import pandas as pandas
import pytest


def test_create_and_get_asset_generates_deterministic_individual_ids(
    fake_transport_zones,
    deterministic_sampling,
    deterministic_shortuuid,
    monkeypatch,
):
    """
    Ensure create_and_get_asset generates sequential shortuuid-based individual_id values
    using our deterministic shortuuid fixture. We capture the DataFrame passed to to_parquet
    for the 'individuals' output to assert IDs and row count.
    """
    # Local capture for the 'individuals' DataFrame that is written to parquet
    captured_individuals_data_frame = {"value": None}

    # Wrap the already-installed parquet stub to also capture the DataFrame content for 'individuals'
    import pandas as pandas_module

    original_to_parquet = pandas_module.DataFrame.to_parquet

    def capturing_to_parquet(self, path, *args, **kwargs):
        # Call the existing (stubbed) to_parquet so other tests/fixtures still see the write path
        original_to_parquet(self, path, *args, **kwargs)
        # If this write is for the individuals parquet, capture the DataFrame content
        path_str = str(path)
        if path_str.endswith("individuals.parquet") or "individuals.parquet" in path_str:
            captured_individuals_data_frame["value"] = self.copy()

    monkeypatch.setattr(pandas_module.DataFrame, "to_parquet", capturing_to_parquet, raising=True)

    import mobility.population as population_module
    population = population_module.Population(
        transport_zones=fake_transport_zones,
        sample_size=5,  # any small positive sample size
        switzerland_census=None,
    )

    # Execute creation, which should write individuals and population_groups parquet files
    population.create_and_get_asset()

    # Verify we captured the individuals DataFrame
    assert captured_individuals_data_frame["value"] is not None
    individuals_data_frame = captured_individuals_data_frame["value"]

    # Basic sanity: required columns present
    required_columns = {
        "individual_id",
        "transport_zone_id",
        "age",
        "socio_pro_category",
        "ref_pers_socio_pro_category",
        "n_pers_household",
        "country",
        "n_cars",
    }
    assert required_columns.issubset(individuals_data_frame.columns)

    # Deterministic shortuuid fixture yields id-0001, id-0002, ... up to row count
    expected_count = len(individuals_data_frame)
    expected_individual_ids = [f"id-{i:04d}" for i in range(1, expected_count + 1)]
    assert individuals_data_frame["individual_id"].tolist() == expected_individual_ids
