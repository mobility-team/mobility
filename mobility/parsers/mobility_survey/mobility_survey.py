import os
import pathlib
import polars as pl
import pandas as pd

from mobility.file_asset import FileAsset

class MobilitySurvey(FileAsset):
    """
    A class for managing and processing mobility survey data for the EMP-2019 and ENTD-2008 surveys.
    
    Attributes:
        source (str): The source of the mobility survey data (e.g., "fr-EMP-2019", "fr-ENTD-2008", "ch-MRMT-2021").
        cache_path (dict): A dictionary mapping data identifiers to their file paths in the cache.
    
    Methods:
        get_cached_asset: Returns the cached asset data as a dictionary of pandas DataFrames.
    """
    
    def __init__(self, inputs):
        
        folder_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "mobility_surveys" / inputs["survey_name"]
        
        files = {
            "short_trips": "short_dist_trips.parquet",
            "days_trip": "days_trip.parquet",
            "long_trips": "long_dist_trips.parquet",
            "travels": "travels.parquet",
            "n_travels": "long_dist_travel_number.parquet",
            "p_immobility": "immobility_probability.parquet",
            "p_car": "car_ownership_probability.parquet",
            "p_det_mode": "insee_modes_to_entd_modes.parquet"
        }
        
        cache_path = {k: folder_path / file for k, file in files.items()}

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> dict[str, pd.DataFrame]:
        """
        Fetches the cached survey data.
        
        Returns:
            dict: A dictionary where keys are data identifiers and values are pandas DataFrames of the cached data.
        """
        return {k: pd.read_parquet(path) for k, path in self.cache_path.items()}
    

    def get_chains_probability(self, motives):
        
        motive_mapping = [{"group": m.name, "motive": m.survey_ids} for m in motives]
        motive_mapping = pd.DataFrame(motive_mapping)
        motive_mapping = motive_mapping.explode("motive")
        motive_mapping = motive_mapping.set_index("motive").to_dict()["group"]
        
        motive_names = [m.name for m in motives]

        days_trips = pl.from_pandas(self.get()["days_trip"].reset_index())
        short_trips = pl.from_pandas(self.get()["short_trips"].reset_index())

        sequences = (
            
            days_trips.select(["day_id", "day_of_week", "pondki"])
            .join(short_trips, on="day_id")
            .rename({"daily_trip_index": "seq_step_index"})
            .with_columns(
                is_weekday=pl.col("day_of_week") < 5,
                max_daily_trip_index=pl.col("seq_step_index").max().over("day_id")
            )

            # Map detailed motives to grouped motives
            .with_columns(pl.col("motive").replace(motive_mapping))
            .with_columns(
                motive=pl.when(pl.col("motive").is_in(motive_names))
                .then(pl.col("motive"))
                .otherwise(pl.lit("other"))
            )
            
            # Cast columns to efficient types
            .with_columns(
                city_category=pl.col("city_category").cast(pl.Enum(["C", "B", "R", "I"])),
                csp=pl.col("csp").cast(pl.Enum(["1", "2", "3", "4", "5", "6", "7", "8", "no_csp"])),
                n_cars=pl.col("n_cars").cast(pl.Enum(["0", "1", "2+"])),
                motive=pl.col("motive").cast(pl.Enum(motive_names))
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
                pondki=pl.col("pondki").sum(),
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

        p_subseq = (
            
            # Compute the probability of each sequence, given day status, city category, 
            # csp and number of cars in the household
            sequences
            .group_by(["is_weekday", "city_category", "csp", "n_cars", "motive_seq", "motive_subseq"])
            .agg(
                pondki=pl.col("pondki").first()
            )
            
            .with_columns(
                p_subseq=(
                    pl.col("pondki")
                    /
                    pl.col("pondki").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
                )
            )
            .select(["is_weekday", "city_category", "csp", "n_cars", "motive_seq", "motive_subseq", "p_subseq"])
            
        )
        
        
        # p = p_subseq.filter(pl.col("is_weekday") == True).filter(pl.col("city_category") == "C").filter(pl.col("csp") == "3").filter(pl.col("n_cars") == "0").to_pandas()

        sequences = (

            sequences.drop("pondki")
            .join(p_subseq, on=["is_weekday", "city_category", "csp", "n_cars", "motive_seq", "motive_subseq"])    
            
        )
        
        # s = sequences.filter(pl.col("is_weekday") == True).filter(pl.col("city_category") == "C").filter(pl.col("csp") == "3").filter(pl.col("n_cars") == "0").to_pandas()
        # sequences.group_by(["is_weekday", "city_category", "csp", "n_cars"]).agg(pl.col("p_subseq").sum()).filter(pl.col("is_weekday") == False)["p_subseq"].hist()
        
        return sequences
            
            
        