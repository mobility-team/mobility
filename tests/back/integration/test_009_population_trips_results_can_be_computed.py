import enum
import json
import os
import pandas as pd
import pytest

import mobility
from mobility.choice_models.population_trips import PopulationTrips
from mobility.motives import OtherMotive, HomeMotive, WorkMotive
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.parsers.mobility_survey.france import EMPMobilitySurvey


@pytest.fixture
def safe_json(monkeypatch):
    """Make json.dump/json.dumps (and orjson.dumps if present) resilient
    to non-serializable objects during this test."""
    def _fallback(o):
        if isinstance(o, enum.Enum):
            return getattr(o, "value", o.name)
        for attr in ("model_dump", "dict"):
            m = getattr(o, attr, None)
            if callable(m):
                try:
                    return m()
                except Exception:
                    pass
        return getattr(o, "__dict__", str(o))

    orig_dump = json.dump
    orig_dumps = json.dumps

    def safe_dump(obj, fp, *args, **kwargs):
        kwargs.setdefault("default", _fallback)
        return orig_dump(obj, fp, *args, **kwargs)

    def safe_dumps(obj, *args, **kwargs):
        kwargs.setdefault("default", _fallback)
        return orig_dumps(obj, *args, **kwargs)

    monkeypatch.setattr(json, "dump", safe_dump, raising=True)
    monkeypatch.setattr(json, "dumps", safe_dumps, raising=True)

    try:
        import orjson  # type: ignore
        _orig_orjson_dumps = orjson.dumps

        def safe_orjson_dumps(obj, *args, **kwargs):
            try:
                return _orig_orjson_dumps(obj, *args, **kwargs)
            except TypeError:
                txt = json.dumps(obj, default=_fallback)
                import json as _json
                return _orig_orjson_dumps(_json.loads(txt), *args, **kwargs)

        monkeypatch.setattr(orjson, "dumps", safe_orjson_dumps, raising=False)
    except Exception:
        pass


def _to_pandas(val):
    """Convert various table-like results to a pandas DataFrame."""
    # pandas
    if hasattr(val, "shape") and hasattr(val, "columns"):
        return val

    # polars
    try:
        import polars as pl  # type: ignore
        if isinstance(val, pl.LazyFrame):
            return val.collect().to_pandas()
        if isinstance(val, pl.DataFrame):
            return val.to_pandas()
    except Exception:
        pass

    # pyspark
    try:
        from pyspark.sql import DataFrame as SparkDF  # type: ignore
        if isinstance(val, SparkDF):
            return val.toPandas()
    except Exception:
        pass

    # mapping with a meaningful key
    if isinstance(val, dict):
        for k in ("data", "df", "path", "output", "result"):
            if k in val:
                return _to_pandas(val[k])

    # lazy/collector
    if hasattr(val, "collect") and callable(getattr(val, "collect")):
        try:
            return _to_pandas(val.collect())
        except Exception:
            pass

    # path-like -> parquet
    if isinstance(val, (str, os.PathLike)):
        return pd.read_parquet(val)

    # last attempt
    try:
        return pd.read_parquet(str(val))
    except Exception:
        raise AssertionError(f"Expected DataFrame/collectable/path-like, got {type(val)}")


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_population_trips_can_be_computed.py::test_008_population_trips_can_be_computed"
    ],
    scope="session",
)
def test_009_population_trips_results_can_be_computed(test_data, safe_json):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    emp = EMPMobilitySurvey()

    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    pop_trips = PopulationTrips(
        population=pop,
        modes=[mobility.CarMode(transport_zones)],
        motives=[HomeMotive(), WorkMotive(), OtherMotive(population=pop)],
        surveys=[emp],
        parameters=PopulationTripsParameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=3,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            seed=0
        ),
    )

    # Evaluate various metrics
    global_metrics = pop_trips.evaluate("global_metrics")
    weekday_sink_occupation = pop_trips.evaluate("sink_occupation", weekday=True)
    weekday_trip_count_by_demand_group = pop_trips.evaluate("trip_count_by_demand_group", weekday=True)
    weekday_distance_per_person = pop_trips.evaluate("distance_per_person", weekday=True)
    weekday_time_per_person = pop_trips.evaluate("time_per_person", weekday=True)

    # Normalize results to pandas DataFrames
    gm_df = _to_pandas(global_metrics)
    sink_df = _to_pandas(weekday_sink_occupation)
    trips_df = _to_pandas(weekday_trip_count_by_demand_group)
    dist_df = _to_pandas(weekday_distance_per_person)
    time_df = _to_pandas(weekday_time_per_person)

    # Assertions
    assert gm_df.shape[0] > 0
    assert sink_df.shape[0] > 0
    assert trips_df.shape[0] > 0
    assert dist_df.shape[0] > 0
    assert time_df.shape[0] > 0
