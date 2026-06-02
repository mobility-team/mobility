from __future__ import annotations

import os
import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

from .person_metrics import RESULT_COLUMNS, SCOPE_COLUMNS, add_demand_group_columns


class TripCountByDemandGroup(FileAsset):
    """Persist trip counts by demand group across selected replications."""

    def __init__(self, *, plan_steps: FileAsset, demand_groups: FileAsset) -> None:
        self.plan_steps = plan_steps
        self.demand_groups = demand_groups

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / "trip_count_by_demand_group.parquet"
        )
        super().__init__(
            {
                "version": 2,
                "plan_steps": plan_steps,
                "demand_groups": demand_groups,
            },
            cache_path,
        )

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the cached metric table."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the metric table."""
        plan_steps = self.plan_steps.get()
        demand_groups = self.demand_groups.get()
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

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        metric.write_parquet(self.cache_path)
        return metric


class Immobility(FileAsset):
    """Persist immobility by country and socio-professional category."""

    def __init__(
        self,
        *,
        plan_steps: FileAsset,
        demand_groups: FileAsset,
        transport_zones: FileAsset,
        survey_immobility: pl.DataFrame | None = None,
    ) -> None:
        self.plan_steps = plan_steps
        self.demand_groups = demand_groups
        self.transport_zones = transport_zones
        self.survey_immobility = survey_immobility

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = project_folder / "group_day_trips" / "results" / "immobility.parquet"
        survey_immobility_rows = None
        if survey_immobility is not None:
            survey_immobility_rows = (
                survey_immobility
                .sort(["country", "csp"])
                .to_dict(as_series=False)
            )
        super().__init__(
            {
                "version": 2,
                "plan_steps": plan_steps,
                "demand_groups": demand_groups,
                "transport_zones": transport_zones,
                "survey_immobility_rows": survey_immobility_rows,
            },
            cache_path,
        )

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the cached metric table."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the immobility table."""
        plan_steps = self.plan_steps.get()
        demand_groups = self.demand_groups.get()
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
        metric = (
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
        if self.survey_immobility is not None:
            metric = (
                metric
                .join(self.survey_immobility, on=["country", "csp"], how="left")
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

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        metric.write_parquet(self.cache_path)
        return metric


def _demand_group_dimensions(demand_groups: pl.LazyFrame) -> list[str]:
    """Return stable demand group dimensions present in the table."""
    schema = demand_groups.collect_schema().names()
    preferred_columns = ["home_zone_id", "csp", "n_cars"]
    return [column for column in preferred_columns if column in schema]


def _transport_zones_with_country(transport_zones: FileAsset) -> pl.DataFrame:
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
