import hashlib
import json
import pathlib
from dataclasses import fields, is_dataclass
from typing import TYPE_CHECKING, Any

import geopandas as gpd
import pandas as pd
from pandas.util import hash_pandas_object
from pydantic import BaseModel

if TYPE_CHECKING:
    from .asset import Asset


def to_stable_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def to_stable_json_key(value: Any) -> str:
    return to_stable_json_bytes(value).decode("utf-8")


def normalize_asset_for_hash(value: "Asset") -> dict[str, str]:
    return {"__asset__": value.get_cached_hash()}


def normalize_sequence_for_hash(value: list[Any] | tuple[Any, ...]) -> dict[str, list[Any]]:
    tag = "__tuple__" if isinstance(value, tuple) else "__list__"
    return {tag: [normalize_for_hash(item) for item in value]}


def normalize_set_for_hash(value: set[Any]) -> dict[str, list[Any]]:
    items = [normalize_for_hash(item) for item in value]
    items.sort(key=to_stable_json_key)
    return {"__set__": items}


def normalize_mapping_for_hash(value: dict[Any, Any]) -> dict[str, list[list[Any]]]:
    items = [
        [normalize_for_hash(key), normalize_for_hash(item)]
        for key, item in value.items()
    ]
    items.sort(key=lambda pair: to_stable_json_key(pair[0]))
    return {"__dict__": items}


def normalize_dataclass_for_hash(value: Any) -> dict[str, Any]:
    return {
        "__dataclass__": value.__class__.__qualname__,
        "fields": {
            field.name: normalize_for_hash(getattr(value, field.name))
            for field in fields(value)
        },
    }


def normalize_path_for_hash(value: pathlib.Path) -> dict[str, str]:
    return {"__path__": str(value)}


def normalize_geodataframe_for_hash(value: gpd.GeoDataFrame) -> dict[str, str]:
    geom_hash = hashlib.sha256(b"".join(value.geometry.to_wkb())).hexdigest()
    attr_hash = hash_pandas_object(value.drop(columns="geometry"), index=True).sum()
    digest = hashlib.sha256((geom_hash + str(attr_hash)).encode("utf-8")).hexdigest()
    return {"__geodataframe__": digest}


def normalize_dataframe_for_hash(value: pd.DataFrame) -> dict[str, str]:
    digest = str(hash_pandas_object(value, index=True).sum())
    return {"__dataframe__": digest}


def normalize_pydantic_for_hash(value: BaseModel) -> dict[str, Any]:
    return {
        "__pydantic__": value.__class__.__qualname__,
        "value": normalize_for_hash(value.model_dump(mode="json")),
    }


def normalize_scalar_for_hash(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"Unsupported input type for asset hashing: {type(value).__qualname__}")


def normalize_for_hash(value: Any) -> Any:
    from .asset import Asset

    if isinstance(value, Asset):
        return normalize_asset_for_hash(value)
    if isinstance(value, (list, tuple)):
        return normalize_sequence_for_hash(value)
    if isinstance(value, set):
        return normalize_set_for_hash(value)
    if is_dataclass(value):
        return normalize_dataclass_for_hash(value)
    if isinstance(value, dict):
        return normalize_mapping_for_hash(value)
    if isinstance(value, pathlib.Path):
        return normalize_path_for_hash(value)
    if isinstance(value, gpd.GeoDataFrame):
        return normalize_geodataframe_for_hash(value)
    if isinstance(value, pd.DataFrame):
        return normalize_dataframe_for_hash(value)
    if isinstance(value, BaseModel):
        return normalize_pydantic_for_hash(value)
    return normalize_scalar_for_hash(value)


def hash_inputs(inputs: dict[str, Any]) -> str:
    normalized = normalize_for_hash(inputs)
    payload = to_stable_json_bytes(normalized)
    return hashlib.md5(payload).hexdigest()
