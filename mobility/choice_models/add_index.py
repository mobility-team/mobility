import polars as pl

def add_index(df, col, index_col_name, tmp_folders):
    """
    Ensure a stable integer index exists for a categorical or string column.
    
    This function maintains an append-only mapping between unique values in
    `df[col]` and a numeric index column named `index_col_name`. The mapping is
    persisted to a parquet file in the workspace (`tmp_folders["sequences-index"]`),
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
    tmp_folders : dict[str, pathlib.Path]
        Dictionary of workspace folders; must contain "sequences-index".
    
    Returns
    -------
    pl.DataFrame
        A copy of `df` with `index_col_name` added.
    """
    
    index_path = tmp_folders["sequences-index"] / (index_col_name + ".parquet")
    
    if index_path.exists() is False:
        
        index = (
            df.select(col)
            .unique()
            .with_row_index()
            .rename({"index": index_col_name})
        )
        
    else:
        
        index = pl.read_parquet(index_path)
        max_index = index[index_col_name].max()
        
        missing_index = (
            df
            .join(index, on=col, how="anti")
            .select(col)
            .unique()
            .with_row_index()
            .with_columns(
                index=pl.col("index") + max_index + 1
            )
            .rename({"index": index_col_name})
        )
        
        index = pl.concat([index, missing_index])
        
    index.write_parquet(index_path)
        
    df = df.join(index, on=col)
        
    return df