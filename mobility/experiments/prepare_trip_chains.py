import os
import dotenv
import mobility as mobility
import pandas as pd
import polars as pl

from mobility.parsers.mobility_survey.france import EMPMobilitySurvey

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"],
    debug=True
)

# Parse the census and mobility survey data 
emp = EMPMobilitySurvey()
# emp.remove()
# emp.get()


days_trips = pl.from_pandas(emp.get()["days_trip"].reset_index())
short_trips = pl.from_pandas(emp.get()["short_trips"].reset_index())

motive_groups = [
    {"group": "home", "motive": ["1.1"]},
    {"group": "work", "motive": ["9.91"]},
    {"group": "shopping", "motive": ["2.20", "2.21"]},
    {"group": "leisure", "motive": ["7.71", "7.72", "7.73", "7.74", "7.75", "7.76", "7.77", "7.78"]}
]

motive_groups = pd.DataFrame(motive_groups)
motive_groups = motive_groups.explode("motive")
motive_groups = motive_groups.set_index("motive").to_dict()["group"]

sequences = (
    
    days_trips.select(["day_id", "day_of_week"])
    .join(short_trips, on="day_id")
    .rename({"daily_trip_index": "seq_step_index"})
    .with_columns(
        is_weekday=pl.col("day_of_week") < 5,
        max_daily_trip_index=pl.col("seq_step_index").max().over("day_id")
    )

    # Map detailed motives to grouped motives
    .with_columns(pl.col("motive").replace(motive_groups))
    .with_columns(
        motive=pl.when(pl.col("motive").is_in(["home", "work", "shopping", "leisure"]))
        .then(pl.col("motive"))
        .otherwise(pl.lit("other"))
    )
    
    # Cast columns to efficient types
    .with_columns(
        city_category=pl.col("city_category").cast(pl.Enum(["C", "B", "R", "I"])),
        csp=pl.col("csp").cast(pl.Enum(["1", "2", "3", "4", "5", "6", "7", "8", "no_csp"])),
        n_cars=pl.col("n_cars").cast(pl.Enum(["0", "1", "2+"])),
        motive=pl.col("motive").cast(pl.Enum(["work", "shopping", "leisure", "other", "home"]))
    )
    
    # Build the sequence of motives of each sequence and subsequence, by breaking up
    # each sequence at "return to home" motives
    .with_columns(
        subseq_index=(
            pl.col("motive")
            .eq("home")
            .shift(1, fill_value=False)
            .cum_sum()
            .over(["day_id"])
        )
    )
    
    .with_columns(
        motive_seq=( 
            pl.col("motive")
            .str.join("-")
            .over("day_id")
        ),
        motive_subseq=( 
            pl.col("motive")
            .str.join("-")
            .over(["day_id", "subseq_index"])
        )
    )
    
    .with_columns(
        motive_subseq_id=( 
            pl.col("motive_subseq")
            .cast(pl.Categorical)
            .to_physical()
        ),
        subseq_step_index=(
            pl.col("motive")
            .cum_count().over(["day_id", "subseq_index"])
        )
    )
    
    
    # Remove sequences that do not end up at home for now because they break
    # the current sequence logic.
    # This is a simplification as we lose approx. 5 % of the trips !
    # We should treat this as a special case.
    .filter(pl.col("motive_seq").str.slice(-5) == "-home")

    # Compute the average departure and arrival time of each trip in each sequence
    .group_by([
        "is_weekday", "city_category", "csp", "n_cars", "motive_seq",
        "motive_subseq", "motive_subseq_id", "motive",
        "seq_step_index", "subseq_step_index"
    ])
    .agg(
        pondki=pl.col("pondki").first(),
        departure_time=(pl.col("departure_time")*pl.col("pondki")).sum()/pl.col("pondki").sum()/3600.0,
        arrival_time=(pl.col("arrival_time")*pl.col("pondki")).sum()/pl.col("pondki").sum()/3600.0,
    )
    .sort(["seq_step_index"])
    
    # Add 24 hours to the times that are after midnight
    .with_columns(
        prev_arrival_time=pl.col("departure_time").shift(n=1).over(["is_weekday", "city_category", "csp", "n_cars", "motive_seq"])
    )
    .with_columns(
        departure_time=pl.when(pl.col("departure_time") < pl.col("prev_arrival_time"))
        .then(pl.col("departure_time") + 24.0)
        .otherwise(pl.col("departure_time"))
    )
    .with_columns(
        arrival_time=pl.when(pl.col("departure_time") > 24.0)
        .then(pl.col("arrival_time") + 24.0)
        .otherwise(pl.col("arrival_time"))
    )
    
    # Compute the overlap of each activity with the periods morning / midday / evening
    # with a minimum activity time of 5 min (some activities in the survey = 0 min, especially "other" activities ?)
    .with_columns(
        next_departure_time=( 
            pl.col("departure_time")
            .shift(n=-1)
            .over(["is_weekday", "city_category", "csp", "n_cars", "motive_seq"])
            .fill_null(pl.col("arrival_time"))
        )
    )
    .with_columns(
        duration_morning=(
            (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(10.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(0.0)]))
            .clip(0.0, 10.0)
            +
            (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0+10.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(24.0)]))
            .clip(0.0, 10.0)
        ).clip(5.0/60.0, 10.0),
        duration_midday=(
            (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(16.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(10.0)]))
            .clip(5.0/60.0, 6.0)
            +
            (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0+16.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(24.0+10.0)]))
            .clip(0.0, 6.0)
        ).clip(5.0/60.0, 6.0),
        duration_evening=(
            (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(16.0)]))
            .clip(0.0, 8.0)
            +
            (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0+24.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(24.0+16.0)]))
            .clip(5.0/60.0, 8.0)
        ).clip(5.0/60.0, 8.0)
    )
    
    .select([
        "is_weekday", "city_category", "csp", "n_cars",
        "motive_seq", "motive_subseq", "motive_subseq_id", "motive",
        "seq_step_index", "subseq_step_index",
        "duration_morning", "duration_midday", "duration_evening",
        "pondki"
    ])
    
)

p_seq = (
    
    # Compute the probability of each sequence, given day status, city category, 
    # csp and number of cars in the household
    sequences
    .group_by(["is_weekday", "city_category", "csp", "n_cars", "motive_seq"])
    .agg(pl.col("pondki").sum())
    .with_columns(
        p_seq=(
            pl.col("pondki")
            /
            pl.col("pondki").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
        )
    )
    .drop("pondki")
    
)

sequences = (

    sequences.drop("pondki")
    .join(p_seq, on=["is_weekday", "city_category", "csp", "n_cars", "motive_seq"])    
    
)
    

# trips.filter(pl.col("is_weekday") == True).filter(pl.col("csp") == "3").group_by("motive").agg(
#     morning=(pl.col("duration_morning")*pl.col("pondki")).sum(),
#     midday=(pl.col("duration_midday")*pl.col("pondki")).sum(),
#     evening=(pl.col("duration_evening")*pl.col("pondki")).sum()
# )

t = sequences.filter(pl.col("city_category") == "C").filter(pl.col("csp") == "3").filter(pl.col("n_cars") == "0").filter(pl.col("is_weekday") == True).to_pandas()
# t = trips.filter(pl.col("city_category") == "B").filter(pl.col("csp") == "3").filter(pl.col("n_cars") == "2+").filter(pl.col("is_weekday") == True).to_pandas()


sequences.write_parquet("d:/data/mobility/sequences.parquet")
