from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from pydantic import BaseModel
from shapely.geometry import Point

from mobility.runtime.assets.input_hashing import hash_inputs, normalize_for_hash


class _Params(BaseModel):
    label: str
    count: int


@dataclass
class _Record:
    path: Path
    enabled: bool


def test_normalize_for_hash_tags_sets_dataclasses_paths_and_pydantic_models(tmp_path):
    normalized_set = normalize_for_hash({2, 1})
    normalized_dataclass = normalize_for_hash(_Record(path=tmp_path / "a.txt", enabled=True))
    normalized_path = normalize_for_hash(tmp_path / "folder" / "file.txt")
    normalized_model = normalize_for_hash(_Params(label="x", count=2))

    assert normalized_set == {"__set__": [1, 2]}
    assert normalized_dataclass["__dataclass__"] == "_Record"
    assert normalized_dataclass["fields"]["enabled"] is True
    assert normalized_dataclass["fields"]["path"]["__path__"].endswith("a.txt")
    assert normalized_path["__path__"].endswith("file.txt")
    assert normalized_model["__pydantic__"] == "_Params"
    assert normalized_model["value"]["__dict__"][0][0] == "count"


def test_normalize_for_hash_hashes_pandas_and_geopandas_frames():
    dataframe = pd.DataFrame({"value": [1, 2], "label": ["a", "b"]})
    geodataframe = gpd.GeoDataFrame(
        {"value": [1, 2]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )
    changed_geodataframe = gpd.GeoDataFrame(
        {"value": [1, 2]},
        geometry=[Point(0, 0), Point(2, 2)],
        crs="EPSG:4326",
    )

    normalized_dataframe = normalize_for_hash(dataframe)
    normalized_geodataframe = normalize_for_hash(geodataframe)

    assert set(normalized_dataframe) == {"__dataframe__"}
    assert set(normalized_geodataframe) == {"__geodataframe__"}
    assert hash_inputs({"frame": geodataframe}) != hash_inputs({"frame": changed_geodataframe})


def test_normalize_for_hash_raises_for_unsupported_objects():
    with pytest.raises(TypeError, match="Unsupported input type"):
        normalize_for_hash(object())
