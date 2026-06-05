from types import SimpleNamespace

import geopandas as gpd
import polars as pl
from shapely.geometry import Point

from mobility.trips.group_day_trips.core.metrics import RunMetrics


def test_get_prominent_cities_accepts_small_study_area():
    plan_steps = pl.DataFrame(
        {
            "from": [1, 2, 3, 4, 5, 6],
            "n_persons": [60.0, 50.0, 40.0, 30.0, 20.0, 10.0],
        }
    )
    transport_zones = gpd.GeoDataFrame(
        {
            "transport_zone_id": [1, 2, 3, 4, 5, 6],
            "local_admin_unit_id": [f"city-{i}" for i in range(1, 7)],
        },
        geometry=[Point(i * 5000, 0) for i in range(6)],
    )
    study_area = gpd.GeoDataFrame(
        {
            "local_admin_unit_id": [f"city-{i}" for i in range(1, 7)],
            "local_admin_unit_name": [f"City {i}" for i in range(1, 7)],
        },
        geometry=[Point(i * 5000, 0) for i in range(6)],
    )
    results = SimpleNamespace(
        plan_steps=SimpleNamespace(collect=lambda: plan_steps),
        transport_zones=SimpleNamespace(
            get=lambda: transport_zones,
            study_area=SimpleNamespace(get=lambda: study_area),
        ),
    )

    labels = RunMetrics(results).get_prominent_cities(n_cities=20)

    assert labels.shape[0] <= study_area.shape[0]
    assert {
        "local_admin_unit_id",
        "local_admin_unit_name",
        "prominence",
        "x",
        "y",
    }.issubset(labels.columns)
