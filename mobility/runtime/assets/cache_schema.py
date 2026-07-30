from __future__ import annotations

import pathlib

import polars as pl


Schema = dict[str, pl.DataType]


def read_cached_parquet(
    cache_path: pathlib.Path,
    *,
    table_name: str,
    required_schema: Schema,
) -> pl.DataFrame:
    """Read one cached parquet table and check its current table contract."""
    table = pl.read_parquet(cache_path)
    validate_cached_table(
        table,
        table_name=table_name,
        required_schema=required_schema,
        cache_path=cache_path,
    )
    return table


def scan_cached_parquet(
    cache_path: pathlib.Path,
    *,
    table_name: str,
    required_schema: Schema,
) -> pl.LazyFrame:
    """Scan one cached parquet table lazily and check its current table contract."""
    table = pl.scan_parquet(cache_path)
    validate_cached_table(
        table,
        table_name=table_name,
        required_schema=required_schema,
        cache_path=cache_path,
    )
    return table


def validate_cached_table(
    table: pl.DataFrame | pl.LazyFrame,
    *,
    table_name: str,
    required_schema: Schema,
    cache_path: pathlib.Path | None = None,
) -> None:
    """Fail when a cached table is missing required columns or key dtypes."""
    actual_schema = table.schema if isinstance(table, pl.DataFrame) else table.collect_schema()
    missing_columns = [
        column
        for column in required_schema
        if column not in actual_schema
    ]
    wrong_types = [
        (column, expected_dtype, actual_schema[column])
        for column, expected_dtype in required_schema.items()
        if column in actual_schema and actual_schema[column] != expected_dtype
    ]

    if not missing_columns and not wrong_types:
        return

    raise RuntimeError(
        _format_cached_schema_error(
            table_name=table_name,
            cache_path=cache_path,
            missing_columns=missing_columns,
            wrong_types=wrong_types,
        )
    )


def _format_cached_schema_error(
    *,
    table_name: str,
    cache_path: pathlib.Path | None,
    missing_columns: list[str],
    wrong_types: list[tuple[str, pl.DataType, pl.DataType]],
) -> str:
    """Build a clear error message for stale cached parquet tables."""
    lines = [
        f"Cached table `{table_name}` does not match the current schema.",
    ]
    if cache_path is not None:
        lines.extend(["", f"Cache path: {cache_path}"])
    if missing_columns:
        lines.append("")
        lines.append("Missing columns:")
        lines.extend(f"- {column}" for column in missing_columns)
    if wrong_types:
        lines.append("")
        lines.append("Wrong column types:")
        lines.extend(
            f"- {column}: expected {expected_dtype}, found {actual_dtype}"
            for column, expected_dtype, actual_dtype in wrong_types
        )
    lines.extend(
        [
            "",
            "This cache was created with an older layout.",
            "Please clear the matching cached files and rerun the model.",
        ]
    )
    return "\n".join(lines)
