from typing import Any

import numpy as np
import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step

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
        generation_requests: pl.DataFrame | None = None,
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
        self.generation_requests = generation_requests
        self.parameters = parameters
        self.seed = seed
        generation_request_zone_ids = (
            tuple(
                int(zone_id)
                for zone_id in generation_requests
                .select("home_zone_id")
                .unique()
                .sort("home_zone_id")["home_zone_id"]
                .to_list()
            )
            if generation_requests is not None
            else None
        )
        inputs = {
            "version": 10,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_state": previous_state,
            "seed_asset": seed_asset,
            "activity_schedule_pool": activity_schedule_pool,
            "generation_request_zone_ids": generation_request_zone_ids,
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

    @trace_simulation_step("activity sampling")
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
            self.survey_plan_steps = state.schedule_steps
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
        """Draw K schedules per activity profile and home zone."""
        if self.activity_schedule_pool is None:
            prepared_pool = prepare_activity_schedule_pool(
                survey_plans=self.survey_plans,
                survey_plan_steps=self.survey_plan_steps,
                demand_groups=self.demand_groups,
                is_weekday=self.is_weekday,
            )
        else:
            prepared_pool = self.activity_schedule_pool.get()

        profile_schedules = prepared_pool["profile_schedules"]
        zone_profiles = prepared_pool["zone_profiles"].select(
            ["country", "home_zone_id", "activity_profile_id"]
        ).unique()
        if self.generation_requests is not None:
            requested_zones = self.generation_requests.select("home_zone_id").unique()
            zone_profiles = zone_profiles.join(
                requested_zones,
                on="home_zone_id",
                how="semi",
            )
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
        # Each profile gets its own weighted draws. Shared schedules remain
        # available to every compatible demand unit when plans are attached.
        sampled_schedules = self._sample_schedules_with_replacement(
            profile_schedules,
            zone_profiles,
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
        profile_schedules: pl.DataFrame,
        zone_profiles: pl.DataFrame,
    ) -> pl.DataFrame:
        """Draw schedules in batches for each activity profile."""
        profile_cols = ["country", "activity_profile_id"]
        zone_cols = ["country", "home_zone_id", "activity_profile_id"]
        k = self.parameters.activity_sequences.k_activity_sequences
        if k is None:
            return (
                zone_profiles
                .join(profile_schedules, on=profile_cols, how="inner")
                .select(["country", "home_zone_id", "activity_seq_id", "time_seq_id"])
                .unique()
                .with_columns(activity_draw_id=pl.lit(0, dtype=pl.UInt32))
            )

        grouped_schedules = profile_schedules.sort(
            profile_cols + ["activity_seq_id", "time_seq_id"]
        ).with_columns(
            p_plan=pl.col("plan_weight_mass")
            / pl.col("plan_weight_mass").sum().over(profile_cols),
            context_seed=pl.struct(profile_cols).hash(seed=self.seed),
        ).group_by(profile_cols).agg(
            pl.col("activity_seq_id"),
            pl.col("time_seq_id"),
            pl.col("p_plan"),
            pl.col("context_seed").first(),
        )
        zones_by_profile = (
            zone_profiles
            .sort(zone_cols)
            .group_by(profile_cols)
            .agg(pl.col("home_zone_id"))
        )
        grouped_schedules = grouped_schedules.join(
            zones_by_profile,
            on=profile_cols,
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
                            "activity_profile_id": row["activity_profile_id"],
                            "activity_seq_id": row["activity_seq_id"][schedule_index],
                            "time_seq_id": row["time_seq_id"][schedule_index],
                            "activity_draw_id": draw_id,
                        }
                    )
        if not sampled_rows:
            return pl.DataFrame(
                schema={
                    "country": profile_schedules.schema["country"],
                    "home_zone_id": zone_profiles.schema["home_zone_id"],
                    "activity_seq_id": profile_schedules.schema["activity_seq_id"],
                    "time_seq_id": profile_schedules.schema["time_seq_id"],
                    "activity_draw_id": pl.UInt32,
                }
            )
        sampled = pl.DataFrame(sampled_rows).with_columns(
            country=pl.col("country").cast(profile_schedules.schema["country"]),
            home_zone_id=pl.col("home_zone_id").cast(zone_profiles.schema["home_zone_id"]),
            activity_seq_id=pl.col("activity_seq_id").cast(
                profile_schedules.schema["activity_seq_id"]
            ),
            time_seq_id=pl.col("time_seq_id").cast(profile_schedules.schema["time_seq_id"]),
            activity_draw_id=pl.col("activity_draw_id").cast(pl.UInt32),
        )
        return sampled.sort(
            ["country", "home_zone_id", "activity_profile_id", "activity_draw_id"]
        ).with_columns(
            activity_draw_id=pl.int_range(pl.len(), dtype=pl.UInt32).over(
                ["country", "home_zone_id"]
            )
        ).drop("activity_profile_id")

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
        if self.generation_requests is not None:
            active_activity_sequences = active_activity_sequences.join(
                self.generation_requests.select("home_zone_id").unique(),
                on="home_zone_id",
                how="semi",
            )
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
