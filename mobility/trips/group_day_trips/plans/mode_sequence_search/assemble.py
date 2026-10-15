import polars as pl


def assemble_mode_sequence_rows(
    *,
    trip_chains: pl.DataFrame,
    search_rows: pl.DataFrame,
    mode_name_by_id: dict[int, str],
) -> pl.DataFrame:
    """Join search results back to grouped trips and map mode ids back to mode names."""
    context_columns = ["home_zone_id", "utility_profile_id"]
    return (
        trip_chains.select(
            context_columns + ["activity_seq_id", "time_seq_id", "dest_seq_id"]
        )
        .join(search_rows, on=["utility_profile_id", "dest_seq_id"])
        .with_columns(mode=pl.col("mode_index").replace_strict(mode_name_by_id))
    )


def build_mode_sequence_keys(search_rows: pl.DataFrame) -> pl.DataFrame:
    """Build one row per mode sequence before assigning the stable mode sequence id."""
    context_columns = ["home_zone_id", "utility_profile_id"]
    candidate_columns = context_columns + [
        "activity_seq_id",
        "time_seq_id",
        "dest_seq_id",
        "mode_seq_index",
    ]
    return (
        search_rows
        .group_by(candidate_columns)
        .agg(mode_sequence_key=pl.col("mode_index").sort_by("seq_step_index").cast(pl.Utf8()))
        .with_columns(mode_sequence_key=pl.col("mode_sequence_key").list.join("-"))
        .sort(candidate_columns + ["mode_sequence_key"])
    )


def finalize_mode_sequence_rows(
    *,
    iteration: int,
    search_rows: pl.DataFrame,
    sequence_keys: pl.DataFrame,
    mode_enum_values: list[str],
) -> pl.DataFrame:
    """Attach stable mode-sequence ids and cast the persisted output schema."""
    context_columns = ["home_zone_id", "utility_profile_id"]
    candidate_columns = context_columns + [
        "activity_seq_id",
        "time_seq_id",
        "dest_seq_id",
        "mode_seq_index",
    ]
    return (
        search_rows
        .join(
            sequence_keys.select(
                candidate_columns + ["mode_seq_id"]
            ),
            on=candidate_columns,
        )
        .drop("mode_seq_index")
        .select(context_columns + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "seq_step_index", "mode"])
        .with_columns(
            seq_step_index=pl.col("seq_step_index").cast(pl.UInt8),
            mode=pl.col("mode").cast(pl.Enum(mode_enum_values)),
            iteration=pl.lit(iteration, dtype=pl.UInt16()),
        )
    )
