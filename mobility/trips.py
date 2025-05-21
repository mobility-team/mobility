import os
import pathlib
import logging
import shortuuid
import pandas as pd
import geopandas as gpd
import numpy as np

from rich.progress import Progress
from mobility.file_asset import FileAsset

from mobility.safe_sample import safe_sample, filter_database
from mobility.sample_travels import sample_travels
from mobility.parsers.mobility_survey import MobilitySurveyAggregator
from mobility.parsers.mobility_survey.france import EMPMobilitySurvey


from typing import Callable

class Trips(FileAsset):
    """
    A class to model and generate trips based on a population asset and mobility survey data.
    
    Attributes:
        population (Asset): The population for which trips will be generated.
        source (str): The source of the mobility survey data (default is "EMP-2019").
        cache_path (pathlib.Path): Path to cache the generated trips data.
    
    Methods:
        get_cached_asset: Returns the cached trips data as a pandas DataFrame.
        create_and_get_asset: Generates trips for the population and caches the data.
        prepare_survey_data: Prepares the necessary mobility survey data for trip generation.
        get_population_trips: Generates trips for each individual in the population.
        get_individual_trips: Samples trips for an individual based on their profile.
    """
    
    def __init__(
            self,
            population: FileAsset,
            surveys={"fr": EMPMobilitySurvey},
            filter_population: Callable[[pd.DataFrame], pd.DataFrame] = None
        ):
        
        mobility_survey = MobilitySurveyAggregator(population, surveys)
        
        inputs = {"population": population, "mobility_survey": mobility_survey}
        
        self.filter_population = filter_population

        file_name = "trips.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:
        """
        Fetches the cached trips data.
        
        Returns:
            pd.DataFrame: The cached trips data as a pandas DataFrame.
        """

        logging.info("Trips already prepared. Reusing the file : " + str(self.cache_path))
        trips = pd.read_parquet(self.cache_path)

        return trips
    
    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Generates trips for each individual in the population based on the mobility survey data, then caches the data.
        
        Returns:
            pd.DataFrame: The generated trips for the population.
        """
        
        logging.info("Generating trips for each individual in the population...")

        transport_zones = self.inputs["population"].inputs["transport_zones"].get()
        study_area = self.inputs["population"].inputs["transport_zones"].study_area.get()
        
        population = self.inputs["population"].get()
        
        if self.filter_population is not None:
            population = self.filter_population(population)
        
        mobility_survey = self.inputs["mobility_survey"].get()
        self.short_trips_db = mobility_survey["short_trips"]
        self.days_trip_db = mobility_survey["days_trip"]
        self.long_trips_db = mobility_survey["long_trips"]
        self.travels_db = mobility_survey["travels"]
        self.n_travels_db = mobility_survey["n_travels"]
        self.p_immobility = mobility_survey["p_immobility"]
        self.p_car = mobility_survey["p_car"]
        
        trips = self.get_population_trips(population, transport_zones, study_area)

        trips.to_parquet(self.cache_path)

        return trips
    
        
    def get_population_trips(
            self,
            population: pd.DataFrame,
            transport_zones: gpd.GeoDataFrame,
            study_area: gpd.GeoDataFrame,
        ) -> pd.DataFrame:
        """
        Generates trips for the entire population by merging population data with transport zone data and then individually generating trips for each person.
        
        Args:
            population (pd.DataFrame): The population data for which trips are to be generated.
            transport_zones (gpd.GeoDataFrame): Geographic data for transport zones.
        
        Returns:
            pd.DataFrame: A DataFrame containing generated trips for the population.
        """
        
        urban_unit_categories = pd.merge(
            transport_zones[["transport_zone_id", "local_admin_unit_id"]],
            study_area[["local_admin_unit_id", "urban_unit_category"]],
            on="local_admin_unit_id"
        )
        
        population = pd.merge(
            population,
            urban_unit_categories[["transport_zone_id", "urban_unit_category"]],
            on="transport_zone_id",
            how="left"
        )
        
        individuals = population.to_dict(orient="records")
        all_trips = []
        
        year = 2025
        dates = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31', freq='D')
        df_days = pd.DataFrame({'date': dates})
        df_days['month'] = df_days['date'].dt.month
        df_days['weekday'] = df_days['date'].dt.weekday
        df_days['day_of_year'] = df_days['date'].dt.dayofyear
        
        with Progress() as progress:
            
            task = progress.add_task("[green]Generating trips...", total=len(individuals))
        
            for individual in individuals:
                
                trips = self.get_individual_trips(
                    csp=individual["socio_pro_category"],
                    csp_household=individual["ref_pers_socio_pro_category"],
                    urban_unit_category=individual["urban_unit_category"],
                    n_pers=individual["n_pers_household"],
                    n_cars=individual["n_cars"],
                    country=individual["country"],
                    df_days=df_days
                )
                
                trips["individual_id"] = individual["individual_id"]
                
                all_trips.append(trips)
                
                progress.update(task, advance=1)
            
        trips = pd.concat(all_trips)
        
        # Replace trip_ids by unique values
        trips["trip_id"] = [shortuuid.uuid() for _ in range(trips.shape[0])]
        
        return trips
        
        
    def get_individual_trips(
        self, csp, csp_household, urban_unit_category, n_pers, n_cars, country, df_days
    ) -> pd.DataFrame:
        """
        Samples long distance trips and short distance trips from survey data (prepared with prepare_survey_data),
        for a specific person's profile (CSP, urban unit category, number of persons and cars of the household).

        Determines the number of cars using the repartition for this urban unit category, CSP and number of persons.
        If data is not sufficient for that triplet, only uses urban unit category and CSP.

        Computes the number of travels (n_travels) during the n_years thanks to the travels_db.
        n_travels are sampled.
        The long_trips associated to these travels are added to the data.

        Thanks to these travels, the number of professional and personal days of travel may be known.
        Local trips made at the travel destination are not included in the source data, so they are estimated:
        for each day at destination, local trips are produced (weekdays for professional travel, week-end days for personal).
        The urban unit category of the destination is used, but the same number or cars is kept.

        The number of days without travels is then deduced (using a 364 days year):
            Week days without travel : 52*5 - number of days of professional travels
            Week-end days without travel : 52*2 - number of days of personal travels
            Doing that, we assume that all professonal trips are made during weekdays and all personal trips during week-ends.
        This assumption is obviously incorrect, and can lead to negative values. This is a point to fix.

        The number of days of immobility is computed using the number without travels and the probability of immobility.
        For the days with mobility, short trips are sampled (separately for week and week-end days).


        Args:
            csp (str):
                The socio-professional category of the person ("1" to "8", or "no_csp").
            csp_household (str):
                The socio-professional category of the household ("1" to "8", or "no_csp").
            n_pers (str) :
                The number of persons of the household ("1", "2" or "3+")
            n_cars (str):
                The number of cars of the household ("0", "1", or "2+").
            urban_unit_category (str):
                The urban unit category ("C", "B", "I", "R").
            source (str) :
                The source of the travels and trips data ("ENTD-2008" or "EMP-2019", the default).

        Returns:
            pd.DataFrame: a dataframe with one row per sampled trip.
            Contains long trips (from travels), short trips made during the travels, and short trips.

                Columns:
                    id (int):
                        The unique id of the trip.
                    mode (str):
                        The mode used for the trip.
                    previous_motive (str):
                        the motive of the previous trip.
                    motive (str):
                        the motive for the trip.
                    distance (float):
                        the distance travelled, in km.
                    n_other_passengers (int):
                        the number of passengers accompanying the person.
        """

        # ---------------------------------------
        # Create new filtered databases according to the socio-pro category, the urban category
        # and the number of persons in the household.
        # If there is no data for this combination, only urban unit category and CSP are used.

        filtered_p_immobility = ( 
            self.p_immobility
            .xs(country, level="country")
            .xs(csp)
        )

        all_trips = []

        # === TRAVELS ===
        # 1/ ---------------------------------------
        # Compute the number of travels during n_years given the socio-pro category.
        n_travel = (
            self.n_travels_db
            .xs(country, level="country")
            .xs(csp)
            .squeeze().astype(int)
        )

        # 2/ ---------------------------------------
        # Sample n_travel travels.
        travels_db = filter_database(
            self.travels_db.xs(country, level="country"),
            csp=csp,
            n_cars=n_cars,
            city_category=urban_unit_category
        )
        
        travels_db = pd.merge(travels_db, df_days, on=["month", "weekday"])
        
        # Sample travels 
        travels_index = sample_travels(
            travels_db,
            start_col="day_of_year",
            length_col="n_nights",
            weight_col="pondki",
            burnin=100,
            k=n_travel,
            num_samples=1
        )
        
        sampled_travels = travels_db.iloc[travels_index[0]].copy()
        
        # 3/ ---------------------------------------
        # Compute the number of days spent in travel, for professional reasons and personal reasons.
        sampled_travels["n_nights"] = sampled_travels["n_nights"].fillna(0)

        # 4/ ---------------------------------------
        # Get the long trips corresponding to the travels sampled.

        travels_id = sampled_travels["travel_id"].to_numpy()
        
        sampled_long_trips = ( 
            self.long_trips_db
            .xs(country, level="country")
            .loc[travels_id]
            .reset_index()
        )
        
        sampled_long_trips["n_nights_at_destination"] = sampled_long_trips["n_nights_at_destination"].fillna(0)
        sampled_long_trips["n_days_from_departure"] = sampled_long_trips.groupby("travel_id")["n_nights_at_destination"].cumsum()
        sampled_long_trips["n_days_from_departure"] = sampled_long_trips.groupby("travel_id")["n_days_from_departure"].shift(1, fill_value=0)
        
        # Filter the columns.
        sampled_long_trips = sampled_long_trips.loc[
            :,
            [
                "travel_id",
                "previous_motive",
                "motive",
                "mode_id",
                "distance",
                "n_other_passengers",
                "n_days_from_departure"
            ],
        ]
        
        # Associate each trip with a date given the departure date of the travels
        sampled_long_trips = pd.merge(
            sampled_long_trips,
            sampled_travels[["travel_id", "date"]],
            on="travel_id"
        )
        
        sampled_long_trips["date"] = sampled_long_trips["date"] + pd.to_timedelta(sampled_long_trips['n_days_from_departure'], unit='D')
        
        # Remove days that do not occur within the modelled year
        sampled_long_trips = sampled_long_trips[sampled_long_trips["date"] < pd.Timestamp('2026-01-01')]

        sampled_long_trips.rename({"travel_id": "trip_id"}, axis=1, inplace=True)
        sampled_long_trips["trip_type"] = "long"
        all_trips.append(sampled_long_trips)

        # 5/ ---------------------------------------
        # Sample days of short trips to simulate the local mobility within a travel :
        #   5.1/ If the travel is for professional reasons
        #   then sample the number of days of the travel from the week days
        #   filtered by the urban category of the destination of the travel.
        #   5.2/ If the travel is for personal reasons
        #   then sample the number of days of the travel from the week-end days
        #   filtered by the urban category of the destination of the travel.

        days_id = []
        
        for i in range(sampled_travels.shape[0]):
            
            n_days_in_travel = int(sampled_travels.iloc[i]["n_nights"] + 1)
            
            weekday = sampled_travels.iloc[i]["motive"][0:1] == "9"
        
            destination_city_category = sampled_travels.iloc[i][
                "destination_city_category"
            ]
            
            sampled_days_in_travel_pro = safe_sample(
                self.days_trip_db.xs(country, level="country"),
                n_days_in_travel,
                weights="pondki",
                csp=csp,
                n_cars=n_cars,
                weekday=weekday,
                city_category=destination_city_category,
            )
            
            dates = pd.date_range(
                start=sampled_travels.iloc[i]["date"],
                periods=n_days_in_travel,
                freq='D'
            )
            
            days_id.append(
                pd.DataFrame({
                    "date": dates,
                    "day_id": sampled_days_in_travel_pro["day_id"].reset_index(drop=True)
                })
            )
                
        days_id = pd.concat(days_id)
        
        # Remove days that do not occur within the modelled year
        days_id = days_id[days_id["date"] < pd.Timestamp('2026-01-01')]

        # 6/ ---------------------------------------
        # Get the short trips corresponding to the days sampled.

        sampled_short_trips_in_travel = pd.merge( 
            days_id,
            self.short_trips_db.xs(country, level="country"),
            on="day_id"
        )

        # Filter the columns.
        sampled_short_trips_in_travel = sampled_short_trips_in_travel.reset_index().loc[
            :,
            [
                "day_id",
                "previous_motive",
                "motive",
                "mode_id",
                "distance",
                "n_other_passengers",
                "date"
            ],
        ]
        
        sampled_short_trips_in_travel.rename(
            {"day_id": "trip_id"}, axis=1, inplace=True
        )
        sampled_short_trips_in_travel["trip_type"] = "short"
        all_trips.append(sampled_short_trips_in_travel)
        
        all_trips = pd.concat(all_trips)

        # === DAILY MOBILITY ===
        # 7/ ---------------------------------------
        # Compute the number of immobility days during the week and during the week-end.

        # Compute the number of days where there is no travel.
        n_week_days_travel = np.sum(pd.to_datetime(all_trips["date"].unique()).weekday < 5)
        n_weekend_days_travel = np.sum(pd.to_datetime(all_trips["date"].unique()).weekday > 4)
        
        n_week_day = 52 * 5 - n_week_days_travel
        n_weekend_day = 52 * 2 - n_weekend_days_travel

        n_immobility_week_day = np.round(
            n_week_day * filtered_p_immobility["immobility_weekday"]
        ).astype(int)
        n_immobility_weekend = np.round(
            n_weekend_day * filtered_p_immobility["immobility_weekend"]
        ).astype(int)

        # Compute the number of days where the person is not in travel nor immobile.
        n_mobile_week_day = max(
            0, 52 * 5 - n_week_days_travel - n_immobility_week_day
        )
        n_mobile_weekend = max(
            0, 52 * 2 - n_weekend_days_travel - n_immobility_weekend
        )

        # 8/ ---------------------------------------
        # Sample n_mob_week_day week days and n_mob_weekend week-end days.
        remaining_days = df_days[
            (~df_days["date"].isin(sampled_long_trips["date"])) & 
            (~df_days["date"].isin(sampled_short_trips_in_travel["date"]))
        ].copy()
        
        remaining_days["is_weekday"] = remaining_days["weekday"] < 5
        
        sampled_week_days = safe_sample(
            self.days_trip_db.xs(country, level="country"),
            n_mobile_week_day,
            weights="pondki",
            csp=csp,
            n_cars=n_cars,
            weekday=True,
            city_category=urban_unit_category,
        )
        
        week_days_dates = remaining_days.loc[remaining_days["is_weekday"] == True].sample(n_mobile_week_day, replace=False)["date"]
        
        sampled_week_days = pd.DataFrame({
            "date": week_days_dates.reset_index(drop=True),
            "day_id": sampled_week_days["day_id"].reset_index(drop=True)
        })

        sampled_weekend_days = safe_sample(
            self.days_trip_db.xs(country, level="country"),
            n_mobile_weekend,
            weights="pondki",
            csp=csp,
            n_cars=n_cars,
            weekday=False,
            city_category=urban_unit_category,
        )
        
        weekend_days_dates = remaining_days.loc[remaining_days["is_weekday"] == False].sample(n_mobile_weekend, replace=False)["date"]
        
        sampled_weekend_days = pd.DataFrame({
            "date": weekend_days_dates.reset_index(drop=True),
            "day_id": sampled_weekend_days["day_id"].reset_index(drop=True)
        })

        # 9/ ---------------------------------------
        # Get the short trips corresponding to the days sampled.

        days_id = pd.concat(
            [
                sampled_week_days,
                sampled_weekend_days
            ]
        )
        
        sampled_short_trips = pd.merge( 
            days_id,
            self.short_trips_db.xs(country, level="country"),
            on="day_id"
        )
        
        # Filter the columns.
        sampled_short_trips = sampled_short_trips.reset_index().loc[
            :,
            [
                "day_id",
                "previous_motive",
                "motive",
                "mode_id",
                "distance",
                "n_other_passengers",
                "date"
            ],
        ]
        sampled_short_trips.rename({"day_id": "trip_id"}, axis=1, inplace=True)
        sampled_short_trips["trip_type"] = "short"

        all_trips = pd.concat([all_trips, sampled_short_trips])

        return all_trips
    
    
   