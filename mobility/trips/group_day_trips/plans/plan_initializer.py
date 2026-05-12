import math

import polars as pl

from mobility.activities.activity import ActivityParameters
from mobility.surveys import SurveyPlanAssets
from mobility.transport.modes.core.mode_values import get_mode_values
from .plan_ids import add_plan_id


class PlanInitializer:
    """Build initial survey-derived plan data and supporting summaries.

    Provides helpers to (1) aggregate population groups and attach survey
    plan probabilities, (2) compute mean activity durations, (3) create
    the stay-home baseline plan, (4) derive destination opportunities,
    and (5) fetch current OD costs.
    """

    def get_survey_plan_data(self, population, survey_plan_assets: SurveyPlanAssets, is_weekday):
        """Build runtime survey plan inputs from survey assets and demand groups."""

        lau_to_city_cat = (
            pl.from_pandas(
                population.transport_zones.study_area.get()
                .drop("geometry", axis=1)[["local_admin_unit_id", "urban_unit_category"]]
                .rename({"urban_unit_category": "city_category"}, axis=1)
            ).with_columns(country=pl.col("local_admin_unit_id").str.slice(0, 2))
        )

        demand_groups = (
            pl.scan_parquet(population.get()["population_groups"])
            .rename(
                {
                    "socio_pro_category": "csp",
                    "transport_zone_id": "home_zone_id",
                    "weight": "n_persons",
                }
            )
            .with_columns(home_zone_id=pl.col("home_zone_id").cast(pl.Int32))
            .join(lau_to_city_cat.lazy(), on=["local_admin_unit_id"])
            .group_by(["country", "home_zone_id", "city_category", "csp", "n_cars"])
            .agg(pl.col("n_persons").sum())
            .collect(engine="streaming")
        )

        countries = demand_groups["country"].unique().sort().to_list()
        survey_plan_steps = survey_plan_assets.get_plan_steps().select(
            [
                "activity_seq_id",
                "time_seq_id",
                "seq_step_index",
                "activity",
                "is_anchor",
                "departure_time",
                "arrival_time",
                "next_departure_time",
                "duration_per_pers",
            ]
        )
        survey_plans = survey_plan_assets.get_plans().select(
            [
                "country",
                "activity_seq_id",
                "time_seq_id",
                "city_category",
                "csp",
                "n_cars",
                "is_weekday",
                "p_plan",
            ]
        )

        def get_col_values(df1, df2, col):
            s = pl.concat([df1.select(col), df2.select(col)]).to_series()
            return s.unique().sort().to_list()

        city_category_values = get_col_values(demand_groups, survey_plans, "city_category")
        csp_values = get_col_values(demand_groups, survey_plans, "csp")
        n_cars_values = get_col_values(demand_groups, survey_plans, "n_cars")
        activity_values = survey_plan_steps["activity"].unique().sort().to_list()
        survey_plans = survey_plans.with_columns(
            country=pl.col("country").cast(pl.Enum(countries)),
            city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
            csp=pl.col("csp").cast(pl.Enum(csp_values)),
            n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
        )
        survey_plan_steps = survey_plan_steps.with_columns(
            activity=pl.col("activity").cast(pl.Enum(activity_values)),
        )

        demand_groups = (
            demand_groups.with_columns(
                country=pl.col("country").cast(pl.Enum(countries)),
                city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
                csp=pl.col("csp").cast(pl.Enum(csp_values)),
                n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
            )
            .sort(["home_zone_id", "country", "city_category", "csp", "n_cars"])
            .with_row_index("demand_group_id")
        )

        survey_plans = (
            survey_plans
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
            .group_by(
                [
                    "country",
                    "city_category",
                    "csp",
                    "n_cars",
                    "activity_seq_id",
                    "time_seq_id",
                ]
            )
            .agg(p_plan=pl.col("p_plan").sum())
            .sort(
                [
                    "country",
                    "city_category",
                    "csp",
                    "n_cars",
                    "activity_seq_id",
                    "time_seq_id",
                ]
            )
        )
        survey_plan_steps = (
            survey_plan_steps
            .unique(subset=["activity_seq_id", "time_seq_id", "seq_step_index"], keep="first")
            .sort(["activity_seq_id", "time_seq_id", "seq_step_index"])
        )

        return survey_plans, survey_plan_steps, demand_groups

    @staticmethod
    def _cast_summary_domains(
        mean_activity_durations: pl.DataFrame,
        mean_home_night_durations: pl.DataFrame,
        activity_demand_per_pers: pl.DataFrame,
        demand_groups: pl.DataFrame,
        activity_dtype,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Align summary-key enums with the runtime demand and activity domains."""

        country_dtype = demand_groups.schema["country"]
        csp_dtype = demand_groups.schema["csp"]

        return (
            mean_activity_durations.with_columns(
                country=pl.col("country").cast(country_dtype),
                csp=pl.col("csp").cast(csp_dtype),
                activity=pl.col("activity").cast(activity_dtype),
            ),
            mean_home_night_durations.with_columns(
                country=pl.col("country").cast(country_dtype),
                csp=pl.col("csp").cast(csp_dtype),
            ),
            activity_demand_per_pers.with_columns(
                country=pl.col("country").cast(country_dtype),
                activity=pl.col("activity").cast(activity_dtype),
            ),
        )

    def get_survey_duration_summaries(
        self,
        population_weighted_plan_steps: pl.LazyFrame,
        demand_groups: pl.DataFrame,
    ):
        """Build survey-derived summaries from the canonical weighted step asset.

        The grouped day-trips model uses three different survey summaries:

        - ``mean_activity_durations`` for step-level activity utility
        - ``mean_home_night_durations`` for the stay-home utility term
        - ``activity_demand_per_pers`` for destination opportunity capacity

        They now all come from the same population-weighted survey reference
        table so diagnostics and runtime opportunity totals use one source of
        truth. The aggregations intentionally differ:

        - mean activity duration is weighted per activity occurrence
        - activity demand per person is weighted by total represented persons
        """
        two_minutes = 120.0 / 3600.0
        plan_segment_keys = [
            "country",
            "home_zone_id",
            "city_category",
            "csp",
            "n_cars",
            "activity_seq_id",
            "time_seq_id",
        ]
        plan_keys_without_home_zone = [key for key in plan_segment_keys if key != "home_zone_id"]

        weighted_plan_steps = population_weighted_plan_steps.with_columns(
            country=pl.col("country").cast(pl.String()),
            csp=pl.col("csp").cast(pl.String()),
            activity=pl.col("activity").cast(pl.String()),
        )
        weighted_plan_step_schema = population_weighted_plan_steps.collect_schema()

        mean_activity_durations = (
            weighted_plan_steps
            .filter(pl.col("seq_step_index") != pl.col("seq_step_index").max().over(plan_keys_without_home_zone))
            .group_by(["country", "csp", "activity"])
            .agg(
                mean_duration_per_pers=pl.max_horizontal(
                    [
                        (pl.col("duration_per_pers") * pl.col("n_persons")).sum()
                        / pl.col("n_persons").sum().clip(1e-18),
                        pl.lit(two_minutes),
                    ]
                )
            )
            .collect(engine="streaming")
        )

        mean_home_night_durations = (
            weighted_plan_steps
            .group_by(plan_segment_keys)
            .agg(
                n_persons=pl.col("n_persons").first(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum(),
            )
            .group_by(["country", "csp"])
            .agg(
                mean_home_night_per_pers=pl.max_horizontal(
                    [
                        (pl.col("home_night_per_pers") * pl.col("n_persons")).sum()
                        / pl.col("n_persons").sum().clip(1e-18),
                        pl.lit(two_minutes),
                    ]
                )
            )
            .collect(engine="streaming")
        )

        country_population = (
            demand_groups
            .with_columns(country=pl.col("country").cast(pl.String()))
            .group_by("country")
            .agg(pl.col("n_persons").sum().alias("country_n_persons"))
        )
        activity_demand_per_pers = (
            weighted_plan_steps
            .group_by(["country", "activity"])
            .agg(total_duration=(pl.col("duration_per_pers") * pl.col("n_persons")).sum())
            .join(country_population.lazy(), on="country", how="inner")
            .with_columns(duration_per_pers=pl.col("total_duration") / pl.col("country_n_persons").clip(1e-18))
            .select(["country", "activity", "duration_per_pers"])
            .collect(engine="streaming")
        )

        return self._cast_summary_domains(
            mean_activity_durations,
            mean_home_night_durations,
            activity_demand_per_pers,
            demand_groups,
            weighted_plan_step_schema["activity"],
        )

    def get_stay_home_state(
        self,
        demand_groups,
        home_night_dur,
        home_activity_parameters: ActivityParameters,
        min_activity_time_constant: float,
        sequence_index_folder,
        modes,
    ):
        """Create the baseline 'stay home all day' state."""

        value_of_time_stay_home = home_activity_parameters.value_of_time_stay_home
        mode_values = get_mode_values(modes, "stay_home")

        stay_home_state = (
            demand_groups.select(["demand_group_id", "country", "csp", "n_persons", "home_zone_id"])
            .with_columns(
                iteration=pl.lit(0, pl.UInt16()),
                activity_seq_id=pl.lit(0, pl.UInt32()),
                time_seq_id=pl.lit(0, pl.UInt32()),
                mode_seq_id=pl.lit(0, pl.UInt32()),
                dest_seq_id=pl.lit(0, pl.UInt32()),
                seq_step_index=pl.lit(0, pl.UInt8()),
                activity=pl.lit("home"),
                from_=pl.col("home_zone_id").cast(pl.UInt16),
                to=pl.col("home_zone_id").cast(pl.UInt16),
                mode=pl.lit("stay_home").cast(pl.Enum(mode_values)),
                duration_per_pers=pl.lit(24.0).cast(pl.Float32),
                departure_time=pl.lit(0.0).cast(pl.Float32),
                arrival_time=pl.lit(0.0).cast(pl.Float32),
                next_departure_time=pl.lit(24.0).cast(pl.Float32),
            )
            .join(home_night_dur, on=["country", "csp"])
            .with_columns(
                utility=(
                    value_of_time_stay_home
                    * pl.col("mean_home_night_per_pers")
                    * (
                        pl.col("mean_home_night_per_pers")
                        / pl.col("mean_home_night_per_pers")
                        / math.exp(-min_activity_time_constant)
                    )
                    .log()
                    .clip(0.0)
                )
            )
            .select(
                [
                    "demand_group_id",
                    "country",
                    "csp",
                    "mean_home_night_per_pers",
                    "iteration",
                    "activity_seq_id",
                    "time_seq_id",
                    "mode_seq_id",
                    "dest_seq_id",
                    "seq_step_index",
                    "activity",
                    pl.col("from_").alias("from"),
                    "to",
                    "mode",
                    "duration_per_pers",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                    "utility",
                    "n_persons",
                ]
            )
        )
        current_states = (
            stay_home_state.select(
                [
                    "demand_group_id",
                    "iteration",
                    "activity_seq_id",
                    "time_seq_id",
                    "mode_seq_id",
                    "dest_seq_id",
                    "utility",
                    "n_persons",
                ]
            )
        )
        current_states = add_plan_id(current_states, index_folder=sequence_index_folder).clone()

        return stay_home_state, current_states

    def get_opportunities(self, activity_demand_per_pers, demand_groups, activities, transport_zones):
        """Compute destination opportunities per activity and zone."""

        demand = (
            demand_groups
            .group_by("country")
            .agg(n_persons=pl.col("n_persons").sum())
            .join(activity_demand_per_pers, on="country", how="inner")
            .with_columns(duration=pl.col("duration_per_pers") * pl.col("n_persons"))
            .group_by("activity")
            .agg(pl.col("duration").sum())
        )

        activity_names = activity_demand_per_pers.schema["activity"].categories

        opportunities = (
            pl.concat(
                [
                    activity.get_opportunities(transport_zones).with_columns(
                        activity=pl.lit(activity.name),
                        sink_saturation_coeff=pl.lit(activity.inputs["parameters"].sink_saturation_coeff),
                    )
                    for activity in activities
                    if activity.has_opportunities is True
                ]
            )
            .filter(pl.col("n_opp") > 0.0)
            .with_columns(
                activity=pl.col("activity").cast(pl.Enum(activity_names)),
                to=pl.col("to").cast(pl.Int32),
            )
            .join(demand, on="activity")
            .with_columns(
                opportunity_capacity=(
                    pl.col("n_opp") / pl.col("n_opp").sum().over("activity")
                    * pl.col("duration")
                    * pl.col("sink_saturation_coeff")
                ),
                k_saturation_utility=pl.lit(1.0, dtype=pl.Float64()),
            )
            .select(["to", "activity", "opportunity_capacity", "k_saturation_utility"])
        )

        return opportunities
