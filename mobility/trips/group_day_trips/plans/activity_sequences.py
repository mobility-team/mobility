from typing import Any

import numpy as np
import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from .demand_subgroups import DEMAND_UNIT_COLS
from .activity_schedule_pool import (
    ActivitySchedulePoolAsset,
    prepare_activity_schedule_pool,
)


class ActivitySequences(InMemoryAsset):
    """Build timed schedules shared by home zone on the sequential path."""

    REQUIRED_SCHEMA = {
        "home_zone_id": pl.Int32,
        "activity_draw_id": pl.UInt32,
        "activity_seq_id": pl.UInt32,
        "time_seq_id": pl.UInt32,
        "seq_step_index": pl.UInt8,
    }

    SCHEDULE_STEP_COLUMNS = [
        "home_zone_id",
        "activity_draw_id",
        "activity_seq_id",
        "time_seq_id",
        "activity",
        "is_anchor",
        "seq_step_index",
        "step_count",
        "departure_time",
        "arrival_time",
        "next_departure_time",
    ]

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        previous_state: Asset | None = None,
        seed_asset: Asset | None = None,
        activity_schedule_pool: ActivitySchedulePoolAsset | None = None,
        current_plans: pl.DataFrame | None = None,
        survey_plans: pl.DataFrame | None = None,
        survey_plan_steps: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        parameters: Any = None,
        seed: int | None = None,
    ) -> None:
        self.previous_state = previous_state
        self.is_weekday = is_weekday
        self.seed_asset = seed_asset
        self.activity_schedule_pool = activity_schedule_pool
        self.current_plans = current_plans
        self.survey_plans = survey_plans
        self.survey_plan_steps = survey_plan_steps
        self.demand_groups = demand_groups
        self.parameters = parameters
        self.seed = seed
        inputs = {
            "version": 8,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_state": previous_state,
            "seed_asset": seed_asset,
            "activity_schedule_pool": activity_schedule_pool,
            "activity_sequence_parameters": (
                parameters.activity_sequences if parameters is not None else None
            ),
            "behavior_change_scope": (
                parameters.behavior_change.scope_at(iteration)
                if parameters is not None
                else None
            ),
            "seed": seed,
        }
        super().__init__(inputs)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute admitted activity-sequence rows for one iteration."""
        try:
            get_group_day_trips_progress().iteration_step(self.iteration, "activity sequences")
            self._load_missing_runtime_inputs_from_previous_state()
            if self.seed is None and self.seed_asset is not None:
                self.seed = self.seed_asset.get()["activity_sequences"]

            if self.current_plans is None:
                raise ValueError("Cannot build activity sequences without current plans.")
            if self.survey_plans is None:
                raise ValueError("Cannot build activity sequences without survey plans.")
            if self.survey_plan_steps is None:
                raise ValueError("Cannot build activity sequences without survey plan steps.")
            if self.demand_groups is None:
                raise ValueError("Cannot build activity sequences without demand groups.")
            if self.parameters is None:
                raise ValueError("Cannot build activity sequences without parameters.")
            if self.seed is None:
                raise ValueError("Cannot build activity sequences without a seed.")

            return self._build_activity_sequences_for_scope()
        finally:
            if self.previous_state is not None:
                self.current_plans = None
                self.survey_plans = None
                self.survey_plan_steps = None
                self.demand_groups = None

    def _load_missing_runtime_inputs_from_previous_state(self) -> None:
        """Fill runtime dataframes from the cached state produced by the previous iteration."""
        if self.previous_state is None:
            return

        state = self.previous_state.get()
        if self.current_plans is None:
            self.current_plans = state.current_plans
        if self.survey_plans is None:
            self.survey_plans = state.survey_plans
        if self.survey_plan_steps is None:
            self.survey_plan_steps = state.survey_plan_steps
        if self.demand_groups is None:
            self.demand_groups = state.demand_groups

    def _build_activity_sequences_for_scope(self) -> pl.DataFrame:
        """Return admitted timed activity-sequence rows for this iteration."""
        scope = self.parameters.behavior_change.scope_at(self.iteration)

        if scope == BehaviorChangeScope.FULL_REPLANNING:
            return self._sample_shared_activity_sequences_by_zone()

        if scope in (
            BehaviorChangeScope.DESTINATION_REPLANNING,
            BehaviorChangeScope.MODE_REPLANNING,
            BehaviorChangeScope.NO_TRANSITIONS,
        ):
            return self._select_active_activity_sequences()

        raise ValueError(f"Unsupported behavior change scope: {scope}")

    def _sample_shared_activity_sequences_by_zone(self) -> pl.DataFrame:
        """Draw one broad schedule sample per home zone before demand-unit attachment."""
        if self.activity_schedule_pool is None:
            prepared_pool = prepare_activity_schedule_pool(
                survey_plans=self.survey_plans,
                survey_plan_steps=self.survey_plan_steps,
                demand_groups=self.demand_groups,
                is_weekday=self.is_weekday,
            )
        else:
            prepared_pool = self.activity_schedule_pool.get()

        pool_schedules = prepared_pool["pool_schedules"]
        profile_schedules = prepared_pool["profile_schedules"]
        zone_profiles = prepared_pool["zone_profiles"]
        zone_pools = zone_profiles.select(
            ["country", "home_zone_id", "pool_id"]
        ).unique()
        profiles_without_schedules = zone_profiles.join(
            profile_schedules.select(["country", "activity_profile_id"]).unique(),
            on=["country", "activity_profile_id"],
            how="anti",
        )
        if profiles_without_schedules.height:
            raise ValueError(
                "No survey schedule is compatible with one or more activity profiles. "
                f"Profiles: {profiles_without_schedules.head(10).to_dicts()}"
            )
        if pool_schedules.height == 0:
            if zone_profiles.height == 0:
                return self._empty_activity_sequences()
            raise ValueError("No survey schedules are eligible for the demand zones.")

        sampled_schedules = self._sample_schedules_with_replacement(
            pool_schedules,
            zone_pools,
        )
        draw_count = self.parameters.activity_sequences.k_activity_sequences
        if draw_count is not None:
            sampled_profile_coverage = (
                sampled_schedules
                .select(["country", "home_zone_id", "activity_seq_id", "time_seq_id"])
                .unique()
                .join(
                    profile_schedules,
                    on=["country", "activity_seq_id", "time_seq_id"],
                    how="inner",
                )
                .join(
                    zone_profiles,
                    on=["country", "home_zone_id", "activity_profile_id"],
                    how="inner",
                )
                .select(["country", "home_zone_id", "activity_profile_id"])
                .unique()
            )
            missing_profiles = zone_profiles.join(
                sampled_profile_coverage,
                on=["country", "home_zone_id", "activity_profile_id"],
                how="anti",
            )
            if missing_profiles.height:
                profile_schedule_probabilities = profile_schedules.with_columns(
                    p_plan=pl.col("plan_weight_mass")
                    / pl.col("plan_weight_mass").sum().over(
                        ["country", "activity_profile_id"]
                    )
                )
                missing_profile_schedules = (
                    missing_profiles
                    .join(
                        profile_schedule_probabilities,
                        on=["country", "activity_profile_id"],
                        how="inner",
                    )
                    .with_columns(
                        context_seed=pl.struct(
                            ["country", "home_zone_id", "activity_profile_id"]
                        ).hash(seed=self.seed),
                    )
                    .sort(
                        [
                            "country",
                            "home_zone_id",
                            "activity_profile_id",
                            "activity_seq_id",
                            "time_seq_id",
                        ]
                    )
                    .group_by(["country", "home_zone_id", "activity_profile_id"])
                    .agg(
                        pl.col("activity_seq_id"),
                        pl.col("time_seq_id"),
                        pl.col("p_plan"),
                        pl.col("context_seed").first(),
                    )
                    .sort(["country", "home_zone_id", "activity_profile_id"])
                )
                topup_rows = []
                topup_schedules_by_zone: dict[tuple[Any, Any], set[tuple[int, int]]] = {}
                topup_counts_by_zone: dict[tuple[Any, Any], int] = {}
                for row in missing_profile_schedules.iter_rows(named=True):
                    probabilities = np.asarray(row["p_plan"], dtype=np.float64)
                    probabilities /= probabilities.sum()
                    context_hash = int(row["context_seed"])
                    rng = np.random.default_rng(
                        np.random.SeedSequence(
                            [self.seed, context_hash & 0xFFFFFFFF, context_hash >> 32]
                        )
                    )
                    selected_index = int(
                        rng.choice(len(probabilities), size=1, replace=True, p=probabilities)[0]
                    )
                    zone_key = (row["country"], row["home_zone_id"])
                    schedule_key = (
                        row["activity_seq_id"][selected_index],
                        row["time_seq_id"][selected_index],
                    )
                    zone_topups = topup_schedules_by_zone.setdefault(zone_key, set())
                    if schedule_key in zone_topups:
                        continue
                    zone_topups.add(schedule_key)
                    topup_count = topup_counts_by_zone.get(zone_key, 0)
                    activity_draw_id = draw_count + topup_count
                    topup_counts_by_zone[zone_key] = topup_count + 1
                    topup_rows.append(
                        {
                            "country": row["country"],
                            "home_zone_id": row["home_zone_id"],
                            "activity_seq_id": schedule_key[0],
                            "time_seq_id": schedule_key[1],
                            "activity_draw_id": activity_draw_id,
                        }
                    )
                if topup_rows:
                    topups = pl.DataFrame(topup_rows).with_columns(
                        country=pl.col("country").cast(sampled_schedules.schema["country"]),
                        home_zone_id=pl.col("home_zone_id").cast(
                            sampled_schedules.schema["home_zone_id"]
                        ),
                        activity_seq_id=pl.col("activity_seq_id").cast(
                            sampled_schedules.schema["activity_seq_id"]
                        ),
                        time_seq_id=pl.col("time_seq_id").cast(
                            sampled_schedules.schema["time_seq_id"]
                        ),
                        activity_draw_id=pl.col("activity_draw_id").cast(pl.UInt32),
                    )
                    sampled_schedules = pl.concat(
                        [sampled_schedules, topups],
                        how="vertical_relaxed",
                    )
        return (
            sampled_schedules
            .join(
                self.survey_plan_steps,
                on=["activity_seq_id", "time_seq_id"],
                how="inner",
            )
            .select(self.SCHEDULE_STEP_COLUMNS)
        )

    def _sample_schedules_with_replacement(
        self,
        pool_schedules: pl.DataFrame,
        zone_pools: pl.DataFrame,
    ) -> pl.DataFrame:
        """Draw schedule indices in batches from each shared support pool."""
        pool_cols = ["country", "pool_id"]
        zone_cols = ["country", "home_zone_id", "pool_id"]
        k = self.parameters.activity_sequences.k_activity_sequences
        if k is None:
            return (
                zone_pools
                .join(pool_schedules, on=pool_cols, how="inner")
                .select(["country", "home_zone_id", "activity_seq_id", "time_seq_id"])
                .unique()
                .with_columns(activity_draw_id=pl.lit(0, dtype=pl.UInt32))
            )

        grouped_schedules = pool_schedules.sort(
            pool_cols + ["activity_seq_id", "time_seq_id"]
        ).with_columns(
            p_plan=pl.col("plan_weight_mass")
            / pl.col("plan_weight_mass").sum().over(pool_cols),
            context_seed=pl.struct(pool_cols).hash(seed=self.seed),
        ).group_by(pool_cols).agg(
            pl.col("activity_seq_id"),
            pl.col("time_seq_id"),
            pl.col("p_plan"),
            pl.col("context_seed").first(),
        )
        zones_by_pool = (
            zone_pools
            .sort(zone_cols)
            .group_by(pool_cols)
            .agg(pl.col("home_zone_id"))
        )
        grouped_schedules = grouped_schedules.join(
            zones_by_pool,
            on=pool_cols,
            how="inner",
        )
        sampled_rows = []
        for row in grouped_schedules.iter_rows(named=True):
            probabilities = np.asarray(row["p_plan"], dtype=np.float64)
            probabilities /= probabilities.sum()
            context_hash = int(row["context_seed"])
            rng = np.random.default_rng(
                np.random.SeedSequence([self.seed, context_hash & 0xFFFFFFFF, context_hash >> 32])
            )
            zone_ids = row["home_zone_id"]
            selected_indexes = rng.choice(
                len(probabilities),
                size=len(zone_ids) * k,
                replace=True,
                p=probabilities,
            )
            for zone_index, home_zone_id in enumerate(zone_ids):
                start = zone_index * k
                for draw_id, schedule_index in enumerate(selected_indexes[start : start + k]):
                    sampled_rows.append(
                        {
                            "country": row["country"],
                            "home_zone_id": home_zone_id,
                            "activity_seq_id": row["activity_seq_id"][schedule_index],
                            "time_seq_id": row["time_seq_id"][schedule_index],
                            "activity_draw_id": draw_id,
                        }
                    )
        if not sampled_rows:
            return pl.DataFrame(
                schema={
                    "country": pool_schedules.schema["country"],
                    "home_zone_id": zone_pools.schema["home_zone_id"],
                    "activity_seq_id": pool_schedules.schema["activity_seq_id"],
                    "time_seq_id": pool_schedules.schema["time_seq_id"],
                    "activity_draw_id": pl.UInt32,
                }
            )
        sampled = pl.DataFrame(sampled_rows).with_columns(
            country=pl.col("country").cast(pool_schedules.schema["country"]),
            home_zone_id=pl.col("home_zone_id").cast(zone_pools.schema["home_zone_id"]),
            activity_seq_id=pl.col("activity_seq_id").cast(
                pool_schedules.schema["activity_seq_id"]
            ),
            time_seq_id=pl.col("time_seq_id").cast(pool_schedules.schema["time_seq_id"]),
            activity_draw_id=pl.col("activity_draw_id").cast(pl.UInt32),
        )
        return sampled

    def _select_active_activity_sequences(self) -> pl.DataFrame:
        """Return the currently occupied non-stay-home activity sequences."""
        active_activity_sequences = (
            self.current_plans
            .filter(pl.col("time_seq_id") != 0)
            .select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"])
            .unique()
        )

        active_activity_sequences = active_activity_sequences.join(
            self.demand_groups.select(DEMAND_UNIT_COLS + ["home_zone_id"]),
            on=DEMAND_UNIT_COLS,
        ).select(["home_zone_id", "activity_seq_id", "time_seq_id"]).unique()
        active_activity_sequences = active_activity_sequences.with_columns(
            activity_draw_id=pl.lit(0, dtype=pl.UInt32)
        )

        if active_activity_sequences.height == 0:
            return self._empty_activity_sequences()

        result = active_activity_sequences.join(
            self.survey_plan_steps,
            on=["activity_seq_id", "time_seq_id"],
            how="inner",
        )
        return result.select(self.SCHEDULE_STEP_COLUMNS)

    def _empty_activity_sequences(self) -> pl.DataFrame:
        """Return an empty activity-sequences dataframe with the expected schema."""
        schema = {
            "home_zone_id": pl.Int32,
            "activity_draw_id": pl.UInt32,
            "activity_seq_id": self.survey_plan_steps.schema["activity_seq_id"],
            "time_seq_id": self.survey_plan_steps.schema["time_seq_id"],
            "activity": self.survey_plan_steps.schema["activity"],
            "is_anchor": self.survey_plan_steps.schema["is_anchor"],
            "seq_step_index": self.survey_plan_steps.schema["seq_step_index"],
            "step_count": self.survey_plan_steps.schema["step_count"],
            "departure_time": self.survey_plan_steps.schema["departure_time"],
            "arrival_time": self.survey_plan_steps.schema["arrival_time"],
            "next_departure_time": self.survey_plan_steps.schema["next_departure_time"],
        }
        return pl.DataFrame(schema=schema)
