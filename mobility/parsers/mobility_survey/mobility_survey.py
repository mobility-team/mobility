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
    
    def __init__(self, inputs, seq_prob_cutoff: float = 0.95):
        
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
        
        self.seq_prob_cutoff = seq_prob_cutoff

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> dict[str, pd.DataFrame]:
        """
        Fetches the cached survey data.
        
        Returns:
            dict: A dictionary where keys are data identifiers and values are pandas DataFrames of the cached data.
        """
        return {k: pd.read_parquet(path) for k, path in self.cache_path.items()}
    

    def get_chains_probability(self, motives, modes):
        
        motive_mapping = [{"group": m.name, "motive": m.survey_ids} for m in motives]
        motive_mapping = pd.DataFrame(motive_mapping)
        motive_mapping = motive_mapping.explode("motive")
        motive_mapping = motive_mapping.set_index("motive").to_dict()["group"]
        
        motive_names = [m.name for m in motives]
        
        mode_mapping = [{"group": m.name, "mode": m.survey_ids} for m in modes]
        mode_mapping = pd.DataFrame(mode_mapping)
        mode_mapping = mode_mapping.explode("mode")
        mode_mapping = mode_mapping.set_index("mode").to_dict()["group"]
        
        mode_names = [m.name for m in modes] + ["other"]

        days_trips = pl.from_pandas(self.get()["days_trip"].reset_index())
        short_trips = pl.from_pandas(self.get()["short_trips"].reset_index())
        
        # About 3 % of the sequences for the swiss MRMT survey are incomplete 
        # (missing steps). For now they are filtered out to avoid bugs in 
        # further processing steps.
        # TO DO : check if the issue comes from the survey parsing or from the 
        # raw survey data.
        incomplete_sequences = (
            short_trips
            .group_by(["individual_id", "day_id"])
            .agg(
                min_step = pl.col("daily_trip_index").min(),
                max_step = pl.col("daily_trip_index").max(),
                n_steps = pl.col("daily_trip_index").n_unique(),
            )
            .with_columns(
                dense_ok=(pl.col("min_step")==1) & (pl.col("max_step")==pl.col("n_steps"))
            )
            .filter(~pl.col("dense_ok"))
            .select(["individual_id","day_id"])
        )

        sequences_by_motives_and_modes = (
            
            days_trips.select(["day_id", "day_of_week", "pondki"])
            .join(short_trips, on="day_id")
            .join(incomplete_sequences, on=["individual_id","day_id"], how="anti")
            .rename({"daily_trip_index": "seq_step_index"})
            .sort("seq_step_index")
            .with_columns(
                is_weekday=pl.col("day_of_week") < 5
            )

            # Map detailed motives to grouped motives
            .with_columns(
                pl.col("motive").replace(motive_mapping)
            )
            .with_columns(
                motive=pl.when(pl.col("motive").is_in(motive_names))
                .then(pl.col("motive"))
                .otherwise(pl.lit("other"))
            )
            
            # Map detailed modes to grouped modes
            .with_columns(
                mode=pl.col("mode_id").replace(mode_mapping)
            )
            .with_columns(
                mode=pl.when(pl.col("mode").is_in(mode_names))
                .then(pl.col("mode"))
                .otherwise(pl.lit("other"))
            )
            
            # Cast columns to efficient types   
            .with_columns(
                city_category=pl.col("city_category").cast(pl.Enum(["C", "B", "R", "I"])),
                csp=pl.col("csp").cast(pl.Enum(["1", "2", "3", "4", "5", "6", "7", "8", "no_csp"])),
                n_cars=pl.col("n_cars").cast(pl.Enum(["0", "1", "2+"])),
                motive=pl.col("motive").cast(pl.Enum(motive_names)),
                mode=pl.col("mode").cast(pl.Enum(mode_names)),
                max_seq_step_index=( 
                    pl.col("seq_step_index")
                    .max().over(["individual_id", "day_id"])
                )
            )
            
            # Remove motive sequences that are longer than 10 motives to speed 
            # up further processing steps. We lose approx. 1 % of the travelled 
            # distance.
            # TO DO : break up these motive sequences into smaller ones.
            .filter(pl.col("max_seq_step_index") < 11)
            
            # Force motive sequences to end up at home because further processing
            # steps can only work on such sequences. Approx. 5 % of the trips 
            # are affected.
            # TO DO : handle this special case in the destination / mode choice 
            # algos.
            .with_columns(
                motive=pl.when(
                    (pl.col("seq_step_index") == pl.col("max_seq_step_index")) & 
                    (pl.col("motive") != "home")
                ).then(
                    pl.lit("home")
                ).otherwise(
                    pl.col("motive")
                )
            )
                    
            # Add 24 hours to the times that are after midnight
            .with_columns(
                prev_arrival_time=pl.col("departure_time").shift(n=1).over(["day_id", "individual_id"])
            )
            .with_columns(
                departure_time=pl.when(pl.col("departure_time") < pl.col("prev_arrival_time"))
                .then(pl.col("departure_time") + 24.0*3600.0)
                .otherwise(pl.col("departure_time"))
            )
            .with_columns(
                arrival_time=pl.when(pl.col("arrival_time") < pl.col("departure_time"))
                .then(pl.col("arrival_time") + 24.0*3600.0)
                .otherwise(pl.col("arrival_time"))
            )
            # .with_columns(
            #     arrival_time=pl.when((pl.col("departure_time") >= 24.0*3600.0) & (pl.col("departure_time") > pl.col("arrival_time")))
            #     .then(pl.col("arrival_time") + 24.0*3600.0)
            #     .otherwise(pl.col("arrival_time"))
            # )

                
            # Combine motives within each sequence to identify unique sequences
            .with_columns(
                motive_seq=( 
                    pl.col("motive")
                    .str.join("-")
                    .over(["individual_id", "day_id"])
                ),
                mode_seq=( 
                    pl.col("mode")
                    .str.join("-")
                    .over(["individual_id", "day_id"])
                ),
                travel_time=(pl.col("arrival_time") - pl.col("departure_time"))
            )

            # Compute the average departure and arrival time of each trip in each sequence
            .group_by([
                "is_weekday", "city_category", "csp", "n_cars", "motive_seq",
                "mode_seq", "motive", "mode", "max_seq_step_index", "seq_step_index"
            ])
            .agg(
                pondki=pl.col("pondki").sum(),
                departure_time=(pl.col("departure_time")*pl.col("pondki")).sum()/pl.col("pondki").sum()/3600.0,
                arrival_time=(pl.col("arrival_time")*pl.col("pondki")).sum()/pl.col("pondki").sum()/3600.0,
                travel_time=(pl.col("travel_time")*pl.col("pondki")).sum()/pl.col("pondki").sum()/3600.0,
                distance=(pl.col("distance")*pl.col("pondki")).sum()/pl.col("pondki").sum()
            )
            .sort(["seq_step_index"])
            

            # Compute the overlap of each activity with the periods morning / 
            # midday / evening. Some sequences span more than one day, so the 
            # values can sum to more than 24 hours.
            # TO DO : do some research about how to handle this better, because
            # for now we should model only one day of activities (but multi day 
            # sequences should still be accounted for somehow).
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
                ).clip(0.0, 10.0),
                duration_midday=(
                    (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(16.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(10.0)]))
                    .clip(0.0, 6.0)
                    +
                    (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0+16.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(24.0+10.0)]))
                    .clip(0.0, 6.0)
                ).clip(0.0, 6.0),
                duration_evening=(
                    (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(16.0)]))
                    .clip(0.0, 8.0)
                    +
                    (pl.min_horizontal([pl.col("next_departure_time"), pl.lit(24.0+24.0)]) - pl.max_horizontal([pl.col("arrival_time"), pl.lit(24.0+16.0)]))
                    .clip(0.0/60.0, 8.0)
                ).clip(0.0, 8.0)
            )
            
            # Some activities are reported to have zero duration in the surveys,
            # which is weird (maybe the effect of some threshold ?) and breaks 
            # our logic that uses time to model opportunities saturation at
            # destination.
            # For now we replace the zeros by the average duration for the 
            # activity, for each day type, motive and step index.
            # Approx. 3 % of the activities still have near zero durations after
            # correction (less than 10 second durations).
            # TO DO : check the surveys to see why these cases appear and find 
            # a better way to handle them.
            .with_columns(
               zero_duration=(
                   pl.col("duration_morning") + pl.col("duration_midday") + pl.col("duration_evening") < 1e-3
               ) 
            )
            
            .with_columns(
                
                duration_morning=pl.when(
                    pl.col("zero_duration") == True
                ).then(
                    (pl.when(pl.col("duration_morning") < 1e-3).then(None).otherwise(pl.col("duration_morning"))*pl.col("pondki")).sum().over(["is_weekday", "motive", "max_seq_step_index"])/pl.col("pondki").sum().over(["is_weekday", "motive", "max_seq_step_index"])
                ).otherwise(
                    pl.col("duration_morning")
                ),
                    
                duration_midday=pl.when(
                    pl.col("zero_duration") == True
                ).then(
                    (pl.when(pl.col("duration_midday") < 1e-3).then(None).otherwise(pl.col("duration_morning"))*pl.col("pondki")).sum().over(["is_weekday", "motive", "max_seq_step_index"])/pl.col("pondki").sum().over(["is_weekday", "motive", "max_seq_step_index"])
                ).otherwise(
                    pl.col("duration_midday")
                ),
                    
                duration_evening=pl.when(
                    pl.col("zero_duration") == True
                ).then(
                    (pl.when(pl.col("duration_evening") < 1e-3).then(None).otherwise(pl.col("duration_morning"))*pl.col("pondki")).sum().over(["is_weekday", "motive", "max_seq_step_index"])/pl.col("pondki").sum().over(["is_weekday", "motive", "max_seq_step_index"])
                ).otherwise(
                    pl.col("duration_evening")
                )
            )
                    
            .with_columns(
                
                duration_morning=pl.when(
                    pl.col("seq_step_index") == pl.col("max_seq_step_index")
                ).then(
                    0.0
                ).otherwise(
                    pl.col("duration_morning")
                ),
                    
                duration_midday=pl.when(
                    pl.col("seq_step_index") == pl.col("max_seq_step_index")
                ).then(
                    0.0
                ).otherwise(
                    pl.col("duration_midday")
                ),
                    
                duration_evening=pl.when(
                    pl.col("seq_step_index") == pl.col("max_seq_step_index")
                ).then(
                    0.0
                ).otherwise(
                    pl.col("duration_evening")
                )
                    
            )
            
            
            .select([
                "is_weekday", "city_category", "csp", "n_cars",
                "motive_seq", "motive",
                "mode_seq", "mode",
                "seq_step_index", 
                "duration_morning", "duration_midday", "duration_evening",
                "distance",
                "travel_time",
                "pondki"
            ])
            
        )
        
        # Compute the probability of each subsequence, keeping only the first 
        # x % of the contribution to the average distance for each population 
        # group
        cutoff = self.seq_prob_cutoff
        
        p_seq = (
            
            # Compute the probability of each sequence, given day status, city category, 
            # csp and number of cars in the household
            sequences_by_motives_and_modes
            .group_by(["is_weekday", "city_category", "csp", "n_cars", "motive_seq", "mode_seq"])
            .agg(
                pondki=pl.col("pondki").first(),
                distance=pl.col("distance").sum()
            )
            
            .with_columns(
                p_seq=(
                    pl.col("pondki")
                    /
                    pl.col("pondki").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
                )
            )
            
            .with_columns(
                distance_p=pl.col("distance")*pl.col("p_seq")
            )
            
            .sort("distance_p", descending=True)
            
            .with_columns(
                distance_p_cum_share=(
                    pl.col("distance_p").cum_sum().over(["is_weekday", "city_category", "csp", "n_cars"])
                    /
                    pl.col("distance_p").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
                )
            )
            
            # Filter subsequences
            .with_columns(
                group_count=pl.len().over(["is_weekday", "city_category", "csp", "n_cars"]),
                cross_threshold=(
                    (pl.col("distance_p_cum_share") >= cutoff) & 
                    (pl.col("distance_p_cum_share").shift(1).over(["is_weekday", "city_category", "csp", "n_cars"]) < cutoff)
                )
            )
            
            .filter(
                (pl.col("distance_p_cum_share") < cutoff)
                | (pl.col("cross_threshold"))
                | (pl.col("group_count") == 1)
            )
            
            # Rescale probabilities
            .with_columns(
                p_seq=pl.col("p_seq")/pl.col("p_seq").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
            )
            
            .select([
                "is_weekday", "city_category", "csp", "n_cars", "motive_seq", "mode_seq", "p_seq"
            ])
            
        )

        
        sequences_by_motives_and_modes = (
            sequences_by_motives_and_modes.drop("pondki")
            .join(p_seq, on=["is_weekday", "city_category", "csp", "n_cars", "motive_seq", "mode_seq"])    
        )
        
        return sequences_by_motives_and_modes
            
            
        