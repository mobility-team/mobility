from __future__ import annotations

import pathlib

import polars as pl


def _append_missing_index_entries(
    df: pl.DataFrame,
    index: pl.DataFrame,
    *,
    key_cols: list[str],
    index_col_name: str,
) -> pl.DataFrame:
    """Append unseen keys to an existing index with increasing ids."""
    max_index = index[index_col_name].max()
    if max_index is None:
        max_index = -1

    missing_index = (
        df
        .join(index, on=key_cols, how="anti")
        .select(key_cols)
        .unique()
        .sort(key_cols)
        .with_row_index()
        .with_columns(index=pl.col("index") + max_index + 1)
        .rename({"index": index_col_name})
    )

    return pl.concat([index, missing_index], how="vertical_relaxed")


def _validate_index(index: pl.DataFrame, *, key_cols: list[str], index_col_name: str) -> None:
    """Fail fast when an index contains duplicate keys or ids."""
    if index.select(key_cols).n_unique() != index.height:
        raise ValueError(f"Index '{index_col_name}' contains duplicate keys for columns {key_cols}.")
    if index[index_col_name].n_unique() != index.height:
        raise ValueError(f"Index '{index_col_name}' contains duplicate ids.")


def add_index(df: pl.DataFrame, col: str, index_col_name: str, index_folder: pathlib.Path) -> pl.DataFrame:
    """Ensure a stable integer index exists for one survey-side sequence column.

    Args:
        df: Input dataframe containing the string column to index.
        col: Column name whose unique values should receive stable ids.
        index_col_name: Name of the generated integer id column.
        index_folder: Folder where the persisted parquet index is stored.

    Returns:
        The input dataframe with `index_col_name` joined in.
    """
    index_path = pathlib.Path(index_folder) / f"{index_col_name}.parquet"
    index_path.parent.mkdir(parents=True, exist_ok=True)

    if index_path.exists() is False:
        index = (
            df.select(col)
            .unique()
            .sort(col)
            .with_row_index()
            .rename({"index": index_col_name})
        )
    else:
        index = pl.read_parquet(index_path)
        index = _append_missing_index_entries(
            df,
            index,
            key_cols=[col],
            index_col_name=index_col_name,
        )

    _validate_index(index, key_cols=[col], index_col_name=index_col_name)
    index.write_parquet(index_path)
    return df.join(index, on=col)
