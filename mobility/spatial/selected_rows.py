import pandas as pd


def read_selected_rows(
    asset,
    id_column: str,
    ids: list[str],
    empty_columns: list[str],
) -> pd.DataFrame:
    """Read selected rows from a prepared table, with a simple fallback."""
    selected_ids = sorted(set(str(row_id) for row_id in ids if row_id is not None))
    if len(selected_ids) == 0:
        return pd.DataFrame(columns=empty_columns)

    if asset.is_update_needed():
        rows = asset.get()
    else:
        try:
            rows = pd.read_parquet(
                asset.cache_path,
                filters=[(id_column, "in", selected_ids)],
            )
        except (TypeError, ValueError):
            rows = pd.read_parquet(asset.cache_path)

    return rows[rows[id_column].isin(selected_ids)].copy()
