import json
import hashlib
import pathlib
from typing import Any, TypeVar

import geopandas as gpd
import pandas as pd

from abc import ABC, abstractmethod
from dataclasses import is_dataclass, fields
from pandas.util import hash_pandas_object
from pydantic import BaseModel

P = TypeVar("P", bound=BaseModel)

class Asset(ABC):
    """
    Abstract base class representing an Asset, with functionality for cache validation
    based on input hash comparison.

    Attributes:
        inputs (Dict): A dictionary of inputs used to generate the Asset.
        cache_path (pathlib.Path): The file path for storing the Asset.
        hash_path (pathlib.Path): The file path for storing the hash of the inputs.
        inputs_hash (str): The hash of the inputs.

    Methods:
        get_cached_asset: Abstract method to retrieve a cached Asset.
        create_and_get_asset: Abstract method to create and retrieve an Asset.
        get: Retrieves the cached Asset or creates a new one if needed.
        compute_inputs_hash: Computes a hash based on the inputs.
        is_update_needed: Checks if an update is needed based on the input hash.
        get_cached_hash: Retrieves the cached hash from the file system.
        update_hash: Updates the cached hash with a new hash value.
    """
    
    def __init__(self, inputs: dict):
        
        self.value = None
        self.inputs = inputs
        self.inputs_hash = self.compute_inputs_hash()
        
        for k, v in self.inputs.items():
            setattr(self, k, v)
    
    @abstractmethod
    def get(self):
        pass
        
    def compute_inputs_hash(self) -> str:
        """
        Computes a hash based on the current inputs of the Asset.
    
        Returns:
            A hash string representing the current state of the inputs.
        """
        def serialize(value):
            """
            Recursively serializes a value, handling nested dataclasses and sets.
            """

            if isinstance(value, Asset):
                return value.get_cached_hash()
            
            elif isinstance(value, list) and all(isinstance(v, Asset) for v in value):
                return {i: serialize(v) for i, v in enumerate(value)}
            
            elif is_dataclass(value):
                return {field.name: serialize(getattr(value, field.name)) for field in fields(value)}
            
            elif isinstance(value, dict):
               
               return {k: serialize(v) for k, v in value.items()}
            
            elif isinstance(value, set):
                return list(value)
            
            elif isinstance(value, pathlib.Path):
                return str(value)
            
            elif isinstance(value, gpd.GeoDataFrame):
                geom_hash = hashlib.sha256(b"".join(value.geometry.to_wkb())).hexdigest()
                attr_hash = hash_pandas_object(value.drop(columns="geometry")).sum()
                return hashlib.sha256((geom_hash + str(attr_hash)).encode()).hexdigest()
            
            elif isinstance(value, BaseModel):
                return value.model_dump(mode="json")
            
            else:
                return value
    
        hashable_inputs = {k: serialize(v) for k, v in self.inputs.items()}
        serialized_inputs = json.dumps(hashable_inputs, sort_keys=True).encode('utf-8')
        
        return hashlib.md5(serialized_inputs).hexdigest()

    @staticmethod
    def prepare_parameters(
        parameters: P | None,
        parameters_cls: type[P],
        explicit_args: dict[str, Any],
        required_fields: list[str] | None = None,
        owner_name: str = "Asset",
    ) -> P:
        """Normalize constructor inputs into a pydantic parameters object.

        Args:
            parameters: Pre-built parameters instance, if provided by the caller.
            parameters_cls: Pydantic model class used to build parameters when
                ``parameters`` is not provided.
            explicit_args: Explicit constructor arguments that can be mapped to
                ``parameters_cls`` fields.
            required_fields: Names of explicit arguments that must be provided
                when ``parameters`` is not passed.
            owner_name: Human-readable owner name used in error messages.

        Returns:
            A normalized pydantic parameters instance.

        Raises:
            ValueError: If required explicit arguments are missing when
                ``parameters`` is not provided.
        """
        explicit_provided = {k: v for k, v in explicit_args.items() if v is not None}

        if parameters is not None:
            if not explicit_provided:
                return parameters
            merged = {**parameters.model_dump(mode="python"), **explicit_provided}
            return parameters_cls.model_validate(merged)

        required_fields = required_fields or []
        missing = [field for field in required_fields if explicit_args.get(field) is None]
        if missing:
            fields_str = ", ".join(f"`{field}`" for field in missing)
            raise ValueError(
                f"{owner_name}: missing required explicit argument(s) {fields_str} when `parameters` is not provided."
            )

        return parameters_cls(**explicit_provided)

    def list_parameters(self, recursive: bool = True) -> list[dict[str, Any]]:
        """Collect parameter rows from pydantic models found in asset inputs.

        Args:
            recursive: If ``True``, traverse the full upstream asset DAG.
                Otherwise, only inspect direct inputs of ``self``.

        Returns:
            A flat list of dictionaries, one per model field, including field
            values, defaults, metadata, constraints, and path information.
        """

        rows: list[dict[str, Any]] = []
        visited_assets: set[int] = set()
        asset_paths_by_id: dict[int, set[str]] = {}

        def extract_constraints(field_schema: dict[str, Any]) -> dict[str, Any]:
            keys = [
                "minimum",
                "maximum",
                "exclusiveMinimum",
                "exclusiveMaximum",
                "multipleOf",
                "enum",
                "pattern",
                "minLength",
                "maxLength",
                "minItems",
                "maxItems",
                "uniqueItems",
                "const",
                "format",
            ]
            return {k: field_schema[k] for k in keys if k in field_schema}

        def add_model_rows(model: BaseModel, asset: "Asset", asset_path: str, model_path: str) -> None:
            schema = model.model_json_schema()
            properties = schema.get("properties", {})
            model_name = model.__class__.__name__
            asset_type = asset.__class__.__name__

            for field_name, field_info in model.__class__.model_fields.items():
                field_schema = properties.get(field_name, {})

                if field_info.is_required():
                    default_value = None
                else:
                    default_value = field_info.default

                # model_fields_set tracks fields explicitly provided by the caller.
                if field_name in model.model_fields_set:
                    value_source = "explicit"
                elif field_info.is_required():
                    value_source = "required"
                else:
                    value_source = "default"

                rows.append(
                    {
                        "_asset_id": id(asset),
                        "asset_path": asset_path,
                        "asset_type": asset_type,
                        "model_name": model_name,
                        "field_name": field_name,
                        "field_path": f"{model_path}.{field_name}",
                        "value": getattr(model, field_name),
                        "default": default_value,
                        "value_source": value_source,
                        "title": field_info.title,
                        "description": field_info.description,
                        "unit": (field_info.json_schema_extra or {}).get("unit"),
                        "constraints": extract_constraints(field_schema),
                    }
                )

        def scan_value(value: Any, owner_asset: "Asset", owner_asset_path: str, value_path: str) -> None:
            if isinstance(value, BaseModel):
                add_model_rows(value, owner_asset, owner_asset_path, value_path)
                return

            if isinstance(value, Asset):
                if recursive:
                    walk_asset(value, value_path)
                return

            if isinstance(value, list):
                for i, item in enumerate(value):
                    scan_value(item, owner_asset, owner_asset_path, f"{value_path}[{i}]")
                return

            if isinstance(value, dict):
                for k, item in value.items():
                    scan_value(item, owner_asset, owner_asset_path, f"{value_path}.{k}")
                return

        def walk_asset(asset: "Asset", asset_path: str) -> None:
            asset_id = id(asset)
            asset_paths_by_id.setdefault(asset_id, set()).add(asset_path)

            if id(asset) in visited_assets:
                return
            visited_assets.add(asset_id)

            for input_name, input_value in asset.inputs.items():
                value_path = f"{asset_path}.{input_name}"
                scan_value(input_value, asset, asset_path, value_path)

        walk_asset(self, self.__class__.__name__)

        for row in rows:
            paths = sorted(asset_paths_by_id.get(row["_asset_id"], {row["asset_path"]}))
            row["asset_paths"] = paths

            prefix = row["asset_path"] + "."
            if row["field_path"].startswith(prefix):
                suffix = row["field_path"][len(prefix):]
                row["field_paths"] = [f"{path}.{suffix}" for path in paths]
            else:
                row["field_paths"] = [row["field_path"]]

            del row["_asset_id"]

        return rows

    def parameters_markdown(self, recursive: bool = True) -> str:
        """Render parameters as a human-readable Markdown report.

        Args:
            recursive: If ``True``, include parameters from upstream assets in
                the report.

        Returns:
            Markdown-formatted report text.
        """

        rows = self.list_parameters(recursive=recursive)
        if not rows:
            return "# Parameters Report\n\nNo pydantic parameters found."

        unique_assets = sorted({row["asset_path"] for row in rows})
        unique_models = sorted({row["model_name"] for row in rows})
        lines = [
            "# Parameters Report",
            "",
            "## Summary",
            "",
            f"- Root asset: `{self.__class__.__name__}`",
            f"- Recursive scan: `{recursive}`",
            f"- Assets with parameters: `{len(unique_assets)}`",
            f"- Parameter models found: `{len(unique_models)}`",
            f"- Total parameter fields: `{len(rows)}`",
            "",
        ]

        rows_by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            rows_by_asset.setdefault(row["asset_path"], []).append(row)

        for asset_path in unique_assets:
            asset_rows = rows_by_asset[asset_path]
            lines.append(f"## Asset `{asset_path}`")
            lines.append("")
            lines.append(f"Asset type: `{asset_rows[0]['asset_type']}`")
            lines.append("")

            model_names = sorted({row["model_name"] for row in asset_rows})
            for model_name in model_names:
                lines.append(f"### Model `{model_name}`")
                lines.append("")

                model_rows = sorted(
                    [row for row in asset_rows if row["model_name"] == model_name],
                    key=lambda r: r["field_name"]
                )

                for row in model_rows:
                    lines.append(f"#### `{row['field_name']}`")
                    lines.append(f"- Path: `{row['field_path']}`")
                    if len(row["asset_paths"]) > 1:
                        lines.append(f"- Alternate asset paths: `{row['asset_paths']}`")
                    if len(row["field_paths"]) > 1:
                        lines.append(f"- Alternate field paths: `{row['field_paths']}`")
                    lines.append(f"- Value: `{row['value']}`")
                    lines.append(f"- Default: `{row['default']}`")
                    lines.append(f"- Value source: `{row['value_source']}`")
                    if row["unit"]:
                        lines.append(f"- Unit: `{row['unit']}`")
                    if row["constraints"]:
                        lines.append(f"- Constraints: `{row['constraints']}`")
                    if row["title"]:
                        lines.append(f"- Title: {row['title']}")
                    if row["description"]:
                        lines.append(f"- Description: {row['description']}")
                    lines.append("")

        lines.append("---")
        lines.append(f"Generated from `{self.__class__.__name__}.parameters_markdown()`")

        return "\n".join(lines)

    def parameters_dataframe(self, recursive: bool = True) -> pd.DataFrame:
        """Return parameter rows as a pandas DataFrame.

        Args:
            recursive: If ``True``, include parameters from upstream assets.

        Returns:
            DataFrame containing one row per parameter field.
        """

        rows = self.list_parameters(recursive=recursive)
        columns = [
            "asset_path",
            "asset_type",
            "model_name",
            "field_name",
            "field_path",
            "asset_paths",
            "field_paths",
            "value",
            "default",
            "value_source",
            "title",
            "description",
            "unit",
            "constraints",
        ]
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=columns)
        return df.reindex(columns=columns)
    
    
        
        
