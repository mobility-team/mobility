import pathlib

import polars as pl


def _append_missing_index_entries(
    df: pl.DataFrame,
    index: pl.DataFrame,
    *,
    key_cols: list[str],
    index_col_name: str,
) -> pl.DataFrame:
    """Append unseen keys to an existing index with monotonically increasing ids."""
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


def add_index(df, col, index_col_name, index_folder: pathlib.Path):
    """
    Ensure a stable integer index exists for a categorical or string column.
    
    This function maintains an append-only mapping between unique values in
    `df[col]` and a numeric index column named `index_col_name`. The mapping is
    persisted to a parquet file in the workspace index folder,
    so the same values always map to the same ids across iterations of the 
    population trips sampling.
    
    - If the index file does not yet exist, it is created with all unique values
      from `df[col]` assigned consecutive ids starting at 0.
    - If the index file exists, any new values missing from the index are appended
      with ids continuing after the current maximum.
    - The updated index is written back to disk.
    - The input `df` is returned with the new `index_col_name` joined in.
    
    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe containing the column to index.
    col : str
        Name of the column in `df` whose unique values should be indexed.
    index_col_name : str
        Name of the numeric index column to add to `df`.
    index_folder : pathlib.Path
        Folder where the persisted index parquet should live.
    
    Returns
    -------
    pl.DataFrame
        A copy of `df` with `index_col_name` added.
    """
    
    index_path = pathlib.Path(index_folder) / (index_col_name + ".parquet")
    
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
        
    df = df.join(index, on=col)
        
    return df


def add_plan_index(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    index_folder: pathlib.Path,
    index_col_name: str = "plan_id",
    key_cols: list[str] | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Ensure a stable integer index exists for the full plan-state key."""
    plan_key_cols = key_cols or ["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"]
    df = frame.collect(engine="streaming") if isinstance(frame, pl.LazyFrame) else frame
    if index_col_name in df.columns:
        df = df.drop(index_col_name)
    if df.select(plan_key_cols).null_count().row(0) != tuple([0] * len(plan_key_cols)):
        raise ValueError(f"Plan index keys cannot contain nulls: {plan_key_cols}")

    index_path = pathlib.Path(index_folder) / f"{index_col_name}.parquet"
    if index_path.exists() is False:
        index = (
            df.select(plan_key_cols)
            .unique()
            .sort(plan_key_cols)
            .with_row_index()
            .rename({"index": index_col_name})
        )
    else:
        index = pl.read_parquet(index_path)
        index = _append_missing_index_entries(
            df,
            index,
            key_cols=plan_key_cols,
            index_col_name=index_col_name,
        )

    _validate_index(index, key_cols=plan_key_cols, index_col_name=index_col_name)
    index.write_parquet(index_path)
    result = df.join(index, on=plan_key_cols)
    return result.lazy() if isinstance(frame, pl.LazyFrame) else result
