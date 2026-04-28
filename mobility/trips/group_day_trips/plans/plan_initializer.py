import math

import polars as pl

from mobility.activities.activity import ActivityParameters
from mobility.surveys import MobilitySurveyPlans, MobilitySurveyPlanSteps, MobilitySurveyPlanSummaries
from .plan_ids import add_plan_id


class PlanInitializer:
    """Build initial survey-derived plan data and supporting summaries.

    Provides helpers to (1) aggregate population groups and attach survey
    plan probabilities, (2) compute mean activity durations, (3) create
    the stay-home baseline plan, (4) derive destination opportunities,
    and (5) fetch current OD costs.
    """

    def get_survey_plan_data(self, population, surveys, activities, modes, is_weekday):
        """Build runtime survey plan inputs from survey assets and demand groups."""

        lau_to_city_cat = (
            pl.from_pandas(
                population.transport_zones.study_area.get()
                .drop("geometry", axis=1)[["local_admin_unit_id", "urban_unit_category"]]
                .rename({"urban_unit_category": "city_category"}, axis=1)
            ).with_columns(country=pl.col("local_admin_unit_id").str.slice(0, 2))
        )

        countries = lau_to_city_cat["country"].unique().to_list()

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

        surveys = [s for s in surveys if s.inputs["parameters"].country in countries]

        survey_plan_step_assets = [
            MobilitySurveyPlanSteps(
                survey=survey,
                activities=activities,
                modes=modes,
            )
            for survey in surveys
        ]
        survey_plan_assets = [
            MobilitySurveyPlans(plan_steps=plan_steps_asset)
            for plan_steps_asset in survey_plan_step_assets
        ]
        survey_plan_steps = pl.concat(
            [plan_steps_asset.get() for plan_steps_asset in survey_plan_step_assets],
            how="vertical_relaxed",
        ).select(
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
        survey_plans = pl.concat(
            [plan_asset.get() for plan_asset in survey_plan_assets],
            how="vertical_relaxed",
        ).select(
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
        survey_plan_steps: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Align summary-key enums with the runtime demand and activity domains."""

        country_dtype = demand_groups.schema["country"]
        csp_dtype = demand_groups.schema["csp"]
        activity_dtype = survey_plan_steps.schema["activity"]

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
        surveys,
        activities,
        modes,
        is_weekday,
        demand_groups: pl.DataFrame,
        survey_plan_steps: pl.DataFrame,
    ):
        """Load precomputed survey-weighted summaries for one day type."""

        summary_assets = [
            MobilitySurveyPlanSummaries(
                plans=MobilitySurveyPlans(
                    plan_steps=MobilitySurveyPlanSteps(
                        survey=survey,
                        activities=activities,
                        modes=modes,
                    )
                ),
                plan_steps=MobilitySurveyPlanSteps(
                    survey=survey,
                    activities=activities,
                    modes=modes,
                ),
            )
            for survey in surveys
        ]
        summary_tables = [summary_asset.get() for summary_asset in summary_assets]

        mean_activity_durations = (
            pl.concat(
                [tables["mean_activity_durations"] for tables in summary_tables],
                how="vertical_relaxed",
            )
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
        )

        mean_home_night_durations = (
            pl.concat(
                [tables["mean_home_night_durations"] for tables in summary_tables],
                how="vertical_relaxed",
            )
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
        )

        activity_demand_per_pers = (
            pl.concat(
                [tables["activity_demand_per_pers"] for tables in summary_tables],
                how="vertical_relaxed",
            )
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
        )

        return self._cast_summary_domains(
            mean_activity_durations,
            mean_home_night_durations,
            activity_demand_per_pers,
            demand_groups,
            survey_plan_steps,
        )

    def get_stay_home_state(
        self,
        demand_groups,
        home_night_dur,
        home_activity_parameters: ActivityParameters,
        min_activity_time_constant: float,
        sequence_index_folder,
    ):
        """Create the baseline 'stay home all day' state."""

        value_of_time_stay_home = home_activity_parameters.value_of_time_stay_home

        stay_home_state = (
            demand_groups.select(["demand_group_id", "country", "csp", "n_persons", "home_zone_id"])
            .with_columns(
                iteration=pl.lit(0, pl.UInt32()),
                activity_seq_id=pl.lit(0, pl.UInt32()),
                time_seq_id=pl.lit(0, pl.UInt32()),
                mode_seq_id=pl.lit(0, pl.UInt32()),
                dest_seq_id=pl.lit(0, pl.UInt32()),
                seq_step_index=pl.lit(0, pl.UInt32()),
                activity=pl.lit("home"),
                from_=pl.col("home_zone_id"),
                to=pl.col("home_zone_id"),
                mode=pl.lit("stay_home"),
                duration_per_pers=pl.lit(24.0),
                departure_time=pl.lit(0.0),
                arrival_time=pl.lit(0.0),
                next_departure_time=pl.lit(24.0),
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
