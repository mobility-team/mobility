import math

import polars as pl

from mobility.activities.activity import ActivityParameters
from .plan_ids import add_plan_id


class PlanInitializer:
    """Builds initial chain demand, averages, and opportunities for the model.

    Provides helpers to (1) aggregate population groups and attach survey
    chain probabilities, (2) compute mean activity durations, (3) create
    the stay-home baseline plan, (4) derive destination opportunities,
    and (5) fetch current OD costs.
    """

    def get_chains(self, population, surveys, activities, modes, is_weekday):
        """Aggregate demand groups and attach survey chain probabilities."""

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

        plans_probability = (
            pl.concat(
                [
                    survey.get_plans_probability(activities, modes).with_columns(
                        country=pl.lit(survey.inputs["parameters"].country),
                        survey_source=pl.lit(survey.inputs["parameters"].survey_name),
                    )
                    for survey in surveys
                ]
            )
        )

        def get_col_values(df1, df2, col):
            s = pl.concat([df1.select(col), df2.select(col)]).to_series()
            return s.unique().sort().to_list()

        city_category_values = get_col_values(demand_groups, plans_probability, "city_category")
        csp_values = get_col_values(demand_groups, plans_probability, "csp")
        n_cars_values = get_col_values(demand_groups, plans_probability, "n_cars")
        activity_values = plans_probability["activity"].unique().sort().to_list()
        mode_values = plans_probability["mode"].unique().sort().to_list()

        plans_probability = plans_probability.with_columns(
            country=pl.col("country").cast(pl.Enum(countries)),
            city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
            csp=pl.col("csp").cast(pl.Enum(csp_values)),
            n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
            activity=pl.col("activity").cast(pl.Enum(activity_values)),
            mode=pl.col("mode").cast(pl.Enum(mode_values)),
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

        activity_seq_index = (
            plans_probability
            .select(
                pl.format(
                    "{}|{}|{}",
                    pl.col("survey_source"),
                    pl.col("country").cast(pl.String),
                    pl.col("survey_plan_id").cast(pl.String),
                ).alias("survey_plan_key")
            )
            .unique()
            .sort("survey_plan_key")
            .with_row_index("activity_seq_id")
            .with_columns(activity_seq_id=(pl.col("activity_seq_id") + 1).cast(pl.UInt32))
        )

        plans_probability = (
            plans_probability
            .with_columns(
                pl.format(
                    "{}|{}|{}",
                    pl.col("survey_source"),
                    pl.col("country").cast(pl.String),
                    pl.col("survey_plan_id").cast(pl.String),
                ).alias("survey_plan_key")
            )
            .join(activity_seq_index, on="survey_plan_key")
            .drop(["activity_seq", "survey_plan_key", "survey_source", "survey_plan_id"])
        )

        anchors = {activity.name: activity.is_anchor for activity in activities}

        chains = (
            demand_groups.join(plans_probability, on=["country", "city_category", "csp", "n_cars"])
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
            .with_columns(n_persons=pl.col("n_persons") * pl.col("p_plan"))
            .with_columns(duration_per_pers=(pl.col("next_departure_time") - pl.col("arrival_time")).clip(0.0, 24.0))
        )

        chains_by_activity = (
            chains.group_by(["demand_group_id", "activity_seq_id", "seq_step_index", "activity"])
            .agg(
                p_plan=pl.col("p_plan").first(),
                n_persons=pl.col("n_persons").sum(),
                duration=(pl.col("n_persons") * pl.col("duration_per_pers")).sum(),
                departure_time=(pl.col("n_persons") * pl.col("departure_time")).sum() / pl.col("n_persons").sum(),
                arrival_time=(pl.col("n_persons") * pl.col("arrival_time")).sum() / pl.col("n_persons").sum(),
                next_departure_time=(
                    (pl.col("n_persons") * pl.col("next_departure_time")).sum() / pl.col("n_persons").sum()
                ),
            )
            .sort(["demand_group_id", "activity_seq_id", "seq_step_index"])
            .with_columns(is_anchor=pl.col("activity").cast(pl.Utf8).replace_strict(anchors))
        )

        demand_groups = demand_groups.drop(["country", "city_category"])

        return chains_by_activity, chains, demand_groups

    def get_mean_activity_durations(self, chains, demand_groups):
        """Compute mean per-person durations for activities and home-night."""

        two_minutes = 120.0 / 3600.0

        chains = (
            chains.join(demand_groups.select(["demand_group_id", "csp"]), on="demand_group_id")
            .with_columns(duration_per_pers=pl.col("duration") / pl.col("n_persons"))
        )

        mean_activity_durations = (
            chains.filter(
                pl.col("seq_step_index")
                != pl.col("seq_step_index").max().over(["demand_group_id", "activity_seq_id"])
            )
            .group_by(["csp", "activity"])
            .agg(
                mean_duration_per_pers=pl.max_horizontal(
                    [
                        (pl.col("duration_per_pers") * pl.col("n_persons")).sum()
                        / pl.col("n_persons").sum(),
                        pl.lit(two_minutes),
                    ]
                )
            )
        )

        mean_home_night_durations = (
            chains.group_by(["demand_group_id", "csp", "activity_seq_id"])
            .agg(
                n_persons=pl.col("n_persons").first(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum(),
            )
            .group_by("csp")
            .agg(
                mean_home_night_per_pers=pl.max_horizontal(
                    [
                        (pl.col("home_night_per_pers") * pl.col("n_persons")).sum()
                        / pl.col("n_persons").sum(),
                        pl.lit(two_minutes),
                    ]
                )
            )
        )

        return mean_activity_durations, mean_home_night_durations

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
            demand_groups.select(["demand_group_id", "csp", "n_persons", "home_zone_id"])
            .with_columns(
                iteration=pl.lit(0, pl.UInt32()),
                activity_seq_id=pl.lit(0, pl.UInt32()),
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
            .join(home_night_dur, on="csp")
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
                    "csp",
                    "mean_home_night_per_pers",
                    "iteration",
                    "activity_seq_id",
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
                    "mode_seq_id",
                    "dest_seq_id",
                    "utility",
                    "n_persons",
                ]
            )
        )
        current_states = add_plan_id(current_states, index_folder=sequence_index_folder).clone()

        return stay_home_state, current_states

    def get_opportunities(self, chains, activities, transport_zones):
        """Compute destination opportunities per activity and zone."""

        demand = (
            chains.filter(pl.col("activity_seq_id") != 0).group_by(["activity"]).agg(pl.col("duration").sum())
        )

        activity_names = chains.schema["activity"].categories

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
