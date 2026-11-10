"""Separate shared plan content from local variants and run history."""

from dataclasses import dataclass

import polars as pl

from mobility.trips.group_day_trips.core.multi_scenario import CANDIDATE_ID_COLS, PLAN_CONTENT_COLUMNS
from .candidate_plan_steps import CandidatePlanStepsAsset


VARIANT_COLUMNS = [
    "activity_seq_id",
    "time_seq_id",
    "reference_time_seq_id",
    "destination_sequence_key_1",
    "destination_sequence_key_2",
    "mode_sequence_key_1",
    "mode_sequence_key_2",
]
HISTORY_COLUMNS = [
    "iteration",
    "first_seen_iteration",
    "last_seen_iteration",
    "last_active_iteration",
]
TIME_COLUMNS = ["duration_per_pers", "departure_time", "arrival_time", "next_departure_time"]


@dataclass(frozen=True)
class PlanCatalogue:
    """Plan content, sequence variants, and availability in individual runs.

    Candidate IDs identify physical content to the nearest second. Variant IDs
    are local to this catalogue and must not be persisted as global identifiers.
    """

    plans: pl.DataFrame
    steps: pl.DataFrame
    variants: pl.DataFrame
    membership: pl.DataFrame

    def candidates_for_run(
        self, run_id: int, destination_index: pl.DataFrame, mode_index: pl.DataFrame,
    ) -> pl.DataFrame:
        """Rebuild compact candidate steps with one run's sequence IDs and history."""
        destination_keys = ["destination_sequence_key_1", "destination_sequence_key_2"]
        mode_keys = ["mode_sequence_key_1", "mode_sequence_key_2"]
        members = self.membership.filter(pl.col("run_id") == run_id)
        indexed_variants = (
            members.join(self.variants, on="variant_id", how="inner")
            .join(destination_index, on=destination_keys, how="inner")
            .join(mode_index, on=mode_keys, how="inner")
        )
        if indexed_variants.height != members.height:
            raise ValueError("A candidate variant is missing from the run's sequence indexes.")
        return (
            indexed_variants
            .join(self.steps, on=CANDIDATE_ID_COLS, how="inner")
            .with_columns(
                demand_group_id=pl.lit(0, dtype=pl.UInt32),
                demand_subgroup_id=pl.lit(0, dtype=pl.UInt32),
                country=pl.lit(""), csp=pl.lit(""),
            )
            .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS)
            .sort(["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "seq_step_index"])
        )

    @classmethod
    def merge(cls, catalogues: list["PlanCatalogue"]) -> "PlanCatalogue":
        """Combine batches without expanding their plans back into wide rows."""
        if not catalogues:
            raise ValueError("At least one plan catalogue is required.")
        step_keys = CANDIDATE_ID_COLS + ["seq_step_index"]
        # Several runs can already point at the same immutable physical table.
        # Concatenating those references would recreate every step per run.
        physical_tables = list({id(catalogue.steps): catalogue.steps for catalogue in catalogues}.values())
        if len(physical_tables) == 1:
            steps = physical_tables[0]
            plans = catalogues[0].plans
        else:
            steps = pl.concat(physical_tables, how="vertical_relaxed").unique()
            if steps.group_by(step_keys).len().filter(pl.col("len") > 1).height:
                raise ValueError("A candidate ID maps to different physical plan steps.")
            steps = steps.sort(step_keys)
            plans = steps.select(CANDIDATE_ID_COLS).unique().sort(CANDIDATE_ID_COLS)

        variant_keys = CANDIDATE_ID_COLS + VARIANT_COLUMNS
        common_variants = all(catalogue.variants is catalogues[0].variants for catalogue in catalogues)
        if common_variants:
            variants = catalogues[0].variants
            members = [catalogue.membership for catalogue in catalogues]
        else:
            variants = (
                pl.concat([catalogue.variants.select(variant_keys) for catalogue in catalogues])
                .unique().sort(variant_keys).with_row_index("variant_id")
            )
            # Variant IDs only have meaning within their original catalogue.
            members = [
                catalogue.membership.join(catalogue.variants, on="variant_id", how="inner")
                .drop("variant_id")
                .join(variants, on=variant_keys, how="inner", nulls_equal=True)
                for catalogue in catalogues
            ]
        membership = (
            pl.concat(members, how="vertical_relaxed")
            .group_by(["run_id", "replication", "variant_id"])
            .agg(
                pl.col("iteration").min(),
                pl.col("first_seen_iteration").min(),
                pl.col("last_seen_iteration").max(),
                pl.col("last_active_iteration").max(),
            )
            .sort(["run_id", "replication", "variant_id"])
        )
        return cls(plans=plans, steps=steps, variants=variants, membership=membership)

    @classmethod
    def from_candidates(cls, candidates: pl.DataFrame) -> "PlanCatalogue":
        """Build a catalogue from complete, content-keyed candidate steps.

        Each input row must include its source run and replication. Sequence
        keys are content keys, not the local destination and mode IDs.
        """
        required = set(
            CANDIDATE_ID_COLS + PLAN_CONTENT_COLUMNS + VARIANT_COLUMNS
            + HISTORY_COLUMNS + ["run_id", "replication"]
        )
        missing = required.difference(candidates.columns)
        if missing:
            raise ValueError(f"Candidate steps are missing catalogue columns: {sorted(missing)}.")

        # Content identities already use whole seconds; store the same values
        # for all variants, so source ordering cannot select a different time.
        steps = candidates.select(CANDIDATE_ID_COLS + PLAN_CONTENT_COLUMNS).with_columns(
            [
                ((pl.col(column).cast(pl.Float64) * 3600).round(0) / 3600)
                .cast(candidates.schema[column]).alias(column)
                for column in TIME_COLUMNS
            ]
        ).unique()
        step_keys = CANDIDATE_ID_COLS + ["seq_step_index"]
        if steps.group_by(step_keys).len().filter(pl.col("len") > 1).height:
            raise ValueError("A candidate ID maps to different physical plan steps.")
        steps = steps.sort(step_keys)
        plans = steps.select(CANDIDATE_ID_COLS).unique().sort(CANDIDATE_ID_COLS)

        variant_keys = CANDIDATE_ID_COLS + VARIANT_COLUMNS
        variants = candidates.select(variant_keys).unique().sort(variant_keys).with_row_index("variant_id")
        # History belongs to the source run, not to the shared plan. Multiple
        # observations of the same variant retain the oldest and newest dates.
        membership = (
            candidates.select(["run_id", "replication"] + variant_keys + HISTORY_COLUMNS)
            .join(variants, on=variant_keys, how="inner", nulls_equal=True)
            .group_by(["run_id", "replication", "variant_id"])
            .agg(
                pl.col("iteration").min(),
                pl.col("first_seen_iteration").min(),
                pl.col("last_seen_iteration").max(),
                pl.col("last_active_iteration").max(),
            )
            .sort(["run_id", "replication", "variant_id"])
        )
        return cls(plans=plans, steps=steps, variants=variants, membership=membership)
