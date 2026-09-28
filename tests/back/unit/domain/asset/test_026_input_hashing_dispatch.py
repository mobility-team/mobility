from dataclasses import dataclass
import hashlib
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from pydantic import BaseModel, field_serializer
from shapely.geometry import Point

from mobility.runtime.assets.input_hashing import hash_inputs, normalize_for_hash, to_stable_json_bytes
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters


def test_dependency_hash_is_stable_when_disk_cache_needs_repair(tmp_path):
    """Cache validation can fail without changing the identity of the inputs."""
    class File(FileAsset):
        def create_and_get_asset(self):
            self.cache_path.write_text("output")
            return self.cache_path

        def get_cached_asset(self):
            return self.cache_path

    child = File({"value": 1}, tmp_path / "child.txt")
    child.cache_path.write_text("output")
    parent_hash = hash_inputs({"child": child})
    assert normalize_for_hash(child) == {"__asset__": child.get_cached_hash()}
    assert not child.is_update_needed()

    child.hash_path.write_text("outdated")
    assert child.is_update_needed()
    assert hash_inputs({"child": child}) == parent_hash
    child.hash_path.unlink()
    assert child.is_update_needed()
    assert hash_inputs({"child": child}) == parent_hash


class _Params(BaseModel):
    label: str
    count: int


class _DisplayParams(BaseModel):
    """Existing field serializers remain part of parameter hash compatibility."""

    count: int

    @field_serializer("count")
    def display_count(self, value):
        return "hidden"


class _NestedParams(BaseModel):
    """Cover Python types that the existing JSON representation flattens."""

    child: _Params
    path: Path
    values: tuple[int, int]


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


def test_parameter_asset_hashes_follow_inputs():
    """Equivalent nested assets share a hash; changing their inputs changes it."""
    first = ParameterValue.by_scenario_and_iteration(
        default=None, serm={5: [InMemoryAsset({"period": 300})]}
    )
    repeated = ParameterValue.by_scenario_and_iteration(
        default=None, serm={5: [InMemoryAsset({"period": 300})]}
    )
    changed = ParameterValue.by_scenario_and_iteration(
        default=None, serm={5: [InMemoryAsset({"period": 150})]}
    )
    assert hash_inputs({"parameters": first}) == hash_inputs({"parameters": repeated})
    assert hash_inputs({"parameters": first}) != hash_inputs({"parameters": changed})


@pytest.mark.parametrize("parameters", [
    _NestedParams(child=_Params(label="x", count=2), path=Path("inputs/feed.zip"), values=(1, 2)),
    _DisplayParams(count=1),
    ParameterValue.by_scenario_and_iteration(default=None, serm={1: [], 5: ["feed.zip"]}),
    GroupDayTripsParameters(),
])
def test_parameter_hashes_match_existing_json_strategy(parameters):
    """Existing nested and simulation parameters keep their full cache hashes."""
    # Reconstruct the previous hash representation, without the new fallback.
    old_model = {
        "__pydantic__": type(parameters).__qualname__,
        "value": normalize_for_hash(parameters.model_dump(mode="json")),
    }
    old_inputs = {"__dict__": [["parameters", old_model]]}
    expected = hashlib.md5(to_stable_json_bytes(old_inputs)).hexdigest()
    assert hash_inputs({"parameters": parameters}) == expected
