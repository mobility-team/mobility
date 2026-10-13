from __future__ import annotations

import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset

from .person_metrics import RESULT_COLUMNS, SCOPE_COLUMNS, add_demand_group_columns
from .scoped_tables import release_scoped_run_tables


class TripCountByDemandGroup(InMemoryAsset):
    """Compute trip counts by demand group across selected replications."""

    def __init__(self, *, plan_steps: list[Asset], demand_groups: list[Asset]) -> None:
        self.plan_steps = list(plan_steps)
        self.demand_groups = list(demand_groups)
        if len(self.plan_steps) != len(self.demand_groups):
            raise ValueError("Trip-count metrics need one demand-group asset per iteration.")
        super().__init__({
            "version": 3,
            "plan_steps": tuple(self.plan_steps),
            "demand_groups": tuple(self.demand_groups),
        })

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build the metric table for the requested result scope."""
        values = []
        for plan_steps_asset, demand_groups_asset in zip(
            self.plan_steps,
            self.demand_groups,
        ):
            try:
                values.append(
                    self._iteration_values(plan_steps_asset, demand_groups_asset)
                )
            finally:
                for iteration in getattr(plan_steps_asset, "selected_iterations", ()):
                    release_scoped_run_tables(iteration)
        return pl.concat(values, how="vertical_relaxed")

    @staticmethod
    def _iteration_values(plan_steps_asset: Asset, demand_groups_asset: Asset) -> pl.DataFrame:
        """Reduce one iteration before moving to the next state."""
        plan_steps = plan_steps_asset.get()
        demand_groups = demand_groups_asset.get()
        dimensions = _demand_group_dimensions(demand_groups)
        plan_steps = add_demand_group_columns(plan_steps, demand_groups, dimensions)
        group_columns = SCOPE_COLUMNS + dimensions

        trip_count = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .group_by(group_columns)
            .agg(n_trips=pl.col("n_persons").cast(pl.Float64).sum())
        )
        per_replication = (
            demand_groups
            .select(group_columns + ["n_persons"])
            .group_by(group_columns)
            .agg(n_persons=pl.col("n_persons").cast(pl.Float64).sum())
            .join(trip_count, on=group_columns, how="left")
            .with_columns(
                n_trips=pl.col("n_trips").fill_null(0.0),
                n_trips_per_person=pl.col("n_trips") / pl.col("n_persons").clip(1e-12),
            )
        )
        output_columns = RESULT_COLUMNS + dimensions
        metric = (
            per_replication
            .group_by(output_columns)
            .agg(
                pl.col("n_persons").mean().alias("n_persons"),
                pl.col("n_trips").mean().alias("n_trips"),
                pl.col("n_trips").std().alias("n_trips_std"),
                pl.col("n_trips_per_person").mean().alias("n_trips_per_person"),
                pl.col("n_trips_per_person").std().alias("n_trips_per_person_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

        return metric


class Immobility(InMemoryAsset):
    """Compute immobility by country and socio-professional category."""

    def __init__(
        self,
        *,
        plan_steps: list[Asset],
        demand_groups: list[Asset],
        transport_zones: Asset,
        survey_immobility: Asset | pl.DataFrame | None = None,
    ) -> None:
        self.plan_steps = list(plan_steps)
        self.demand_groups = list(demand_groups)
        if len(self.plan_steps) != len(self.demand_groups):
            raise ValueError("Immobility metrics need one demand-group asset per iteration.")
        self.transport_zones = transport_zones
        self.survey_immobility = survey_immobility

        super().__init__(
            {
                "version": 3,
                "plan_steps": tuple(self.plan_steps),
                "demand_groups": tuple(self.demand_groups),
                "transport_zones": transport_zones,
                "survey_immobility": survey_immobility,
            },
        )

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build the immobility table for the requested result scope."""
        survey_immobility = (
            self.survey_immobility.get()
            if isinstance(self.survey_immobility, Asset)
            else self.survey_immobility
        )
        values = []
        for plan_steps_asset, demand_groups_asset in zip(
            self.plan_steps,
            self.demand_groups,
        ):
            try:
                values.append(
                    self._iteration_values(plan_steps_asset, demand_groups_asset)
                )
            finally:
                for iteration in getattr(plan_steps_asset, "selected_iterations", ()):
                    release_scoped_run_tables(iteration)
        if survey_immobility is not None:
            values[0] = self._with_survey_reference(values[0], survey_immobility)
        return pl.concat(values, how="vertical_relaxed")

    def _iteration_values(
        self,
        plan_steps_asset: Asset,
        demand_groups_asset: Asset,
    ) -> pl.DataFrame:
        """Reduce one iteration before moving to the next state."""
        plan_steps = plan_steps_asset.get()
        demand_groups = demand_groups_asset.get()
        zones = _transport_zones_with_country(self.transport_zones)
        demand_keys = _demand_group_dimensions(demand_groups)
        plan_steps = add_demand_group_columns(plan_steps, demand_groups, demand_keys)
        join_keys = SCOPE_COLUMNS + demand_keys
        if "home_zone_id" in demand_keys:
            plan_steps = plan_steps.with_columns(pl.col("home_zone_id").cast(pl.String))
            demand_groups = demand_groups.with_columns(pl.col("home_zone_id").cast(pl.String))

        immobile_population = (
            plan_steps
            .filter(pl.col("activity_seq_id") == 0)
            .group_by(join_keys)
            .agg(n_persons_imm=pl.col("n_persons").cast(pl.Float64).sum())
        )
        per_replication = (
            demand_groups
            .select(join_keys + ["n_persons"])
            .group_by(join_keys)
            .agg(n_persons=pl.col("n_persons").cast(pl.Float64).sum())
            .join(immobile_population, on=join_keys, how="left")
            .with_columns(pl.col("n_persons_imm").fill_null(0.0))
            .join(zones.lazy(), on="home_zone_id", how="inner")
            .with_columns(csp=pl.col("csp").cast(pl.String))
            .group_by(SCOPE_COLUMNS + ["country", "csp"])
            .agg(
                n_persons_imm=pl.col("n_persons_imm").sum(),
                n_persons=pl.col("n_persons").sum(),
            )
            .with_columns(p_immobility=pl.col("n_persons_imm") / pl.col("n_persons").clip(1e-12))
        )
        output_columns = RESULT_COLUMNS + ["country", "csp"]
        return (
            per_replication
            .group_by(output_columns)
            .agg(
                pl.col("n_persons_imm").mean().alias("n_persons_imm"),
                pl.col("n_persons_imm").std().alias("n_persons_imm_std"),
                pl.col("n_persons").mean().alias("n_persons"),
                pl.col("p_immobility").mean().alias("p_immobility"),
                pl.col("p_immobility").std().alias("p_immobility_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .collect(engine="streaming")
            .sort(output_columns)
        )

    @staticmethod
    def _with_survey_reference(
        metric: pl.DataFrame,
        survey_immobility: pl.DataFrame,
    ) -> pl.DataFrame:
        """Add the external survey comparison to the final iteration."""
        return (
            metric
            .join(survey_immobility, on=["country", "csp"], how="left")
            .with_columns(
                reference_source=pl.lit("survey"),
                p_immobility_reference=pl.col("p_immobility_ref"),
                n_persons_imm_reference=pl.col("n_persons") * pl.col("p_immobility_ref"),
            )
            .with_columns(
                gap=pl.col("p_immobility") - pl.col("p_immobility_reference"),
                gap_std=pl.col("p_immobility_std"),
                relative_gap=(
                    (pl.col("p_immobility") - pl.col("p_immobility_reference"))
                    / pl.col("p_immobility_reference")
                ),
            )
            .drop("p_immobility_ref")
        )


class SurveyImmobilityReference(InMemoryAsset):
    """Compute survey immobility probabilities for the selected day type."""

    def __init__(self, *, surveys: list[Asset], is_weekday: bool) -> None:
        self.surveys = list(surveys)
        self.is_weekday = is_weekday
        super().__init__({
            "version": 1,
            "surveys": tuple(self.surveys),
            "is_weekday": is_weekday,
        })

    def create_and_get_asset(self) -> pl.DataFrame:
        """Return survey immobility probabilities by country and CSP."""
        column_name = "immobility_weekday" if self.is_weekday else "immobility_weekend"
        survey_tables = [
            pl.DataFrame(survey.get()["p_immobility"].reset_index()).with_columns(
                country=pl.lit(survey.inputs["parameters"].country, dtype=pl.String)
            )
            for survey in self.surveys
        ]
        if not survey_tables:
            raise ValueError("Immobility needs at least one survey with immobility probabilities.")
        return (
            pl.concat(survey_tables, how="vertical_relaxed")
            .with_columns(
                country=pl.col("country").cast(pl.String),
                csp=pl.col("csp").cast(pl.String),
                p_immobility_ref=pl.col(column_name).cast(pl.Float64),
            )
            .select(["country", "csp", "p_immobility_ref"])
        )


def _demand_group_dimensions(demand_groups: pl.LazyFrame) -> list[str]:
    """Return stable demand group dimensions present in the table."""
    schema = demand_groups.collect_schema().names()
    preferred_columns = ["home_zone_id", "csp", "n_cars"]
    return [column for column in preferred_columns if column in schema]


def _transport_zones_with_country(transport_zones: Asset) -> pl.DataFrame:
    """Return inner transport zones with their country."""
    zones = transport_zones.get()
    if "geometry" in zones.columns:
        zones = zones.drop(columns="geometry")
    study_area = transport_zones.study_area.get()
    if "geometry" in study_area.columns:
        study_area = study_area.drop(columns="geometry")
    return (
        pl.from_pandas(zones)
        .filter(pl.col("is_inner_zone"))
        .join(
            pl.from_pandas(study_area).select(["local_admin_unit_id", "country"]),
            on="local_admin_unit_id",
            how="left",
        )
        .select([
            pl.col("transport_zone_id").cast(pl.String).alias("home_zone_id"),
            pl.col("country").cast(pl.String),
        ])
    )
