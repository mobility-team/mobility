import numpy as np
import pandas as pd

from .get_survey_data import get_survey_data
from .safe_sample import safe_sample


class TripSampler:
    def __init__(self, source="EMP-2019"):
        """
        Create a TripSampler object for a given survey.

        Args:
            source (str) : The source of the travels and trips data ("ENTD-2008" or "EMP-2019", the default).

        Returns:
            TripSampler2: the TripSampler object, ready for sampling.
        """

        # Charger les dataframes nécessaires pour l'échantillonage avec get_survey_data(source)

        survey_data = get_survey_data(source=source)
        self.short_trips_db = survey_data["short_trips"]
        self.days_trip_db = survey_data["days_trip"]
        self.long_trips_db = survey_data["long_trips"]
        self.travels_db = survey_data["travels"]
        self.n_travels_db = survey_data["n_travels"]
        self.p_immobility = survey_data["p_immobility"]
        self.p_car = survey_data["p_car"]

    def get_trips(
        self, csp, csp_household, urban_unit_category, n_pers, n_cars=None, n_years=1
    ):
        """
        Sample long distance trips and short distance trips from survey data (prepared with prepare_survey_data),
        for a specific person profile.

        Args:
            csp (str): The socio-professional category of the person ("1" to "8", or "no_csp").
            csp_household (str): The socio-professional category of the household ("1" to "8", or "no_csp").
            n_pers (str) : The number of persons of the household ("1", "2" or "3+")
            n_cars (str): The number of cars of the household ("0", "1", or "2+").
            urban_unit_category (str): The urban unit category ("C", "B", "I", "R").
            n_years (int): The number of years of trips to sample (1 to N, defaults to 1).
            source (str) : The source of the travels and trips data ("ENTD-2008" or "EMP-2019", the default).

        Returns:
            pd.DataFrame: a dataframe with one row per sampled trip.
                Columns:
                    id (int): The unique id of the trip.
                    mode (str): The mode used for the trip.
                    previous_motive (str): the motive of the previous trip.
                    motive (str): the motive for the trip.
                    distance (float): the distance travelled, in km.
                    n_other_passengers (int): the number of passengers accompanying the person.
        """

        # Echantilloner les voyages en fonction de la catégorie d'unité urbaine, la CSP et le nombre de voitures du ménage

        # Les données des voyages n'incluent pas les déplacements une fois à destination, donc il faut les estimer
        # Proposition :
        #   1. Calculer le nombre de jours passés en voyage, pour le travail (n1) et pour raisons personnelles (n2).
        #   2. Echantilloner n1 jours de semaine et n2 jours de weekend dans les données des déplacements courte distance
        #      (en fonction de la catagorie d'unité urbaine de la destination, de la CSP et du nombre de voitures du ménage).

        # Echantilloner les déplacements courte distance
        # Calculer le nombre de jours de semaine passés au domicile (52*5 - n1)
        # Calculer le nombre de jours de weekends passés au domicile (52*2 - n2)

        # ---------------------------------------
        # Create new filtered databases according to the socio-pro category, the urban category
        # and the number of persons in the householde

        try:
            filtered_p_car = (
                self.p_car.xs(urban_unit_category)
                .xs(csp_household)
                .xs(n_pers)
                .squeeze(axis=1)
            )
        except KeyError:
            filtered_p_car = self.p_car.reset_index(level="n_pers", drop=True)
            filtered_p_car = (
                filtered_p_car.xs(urban_unit_category).xs(csp_household).squeeze(axis=1)
            )
            filtered_p_car /= filtered_p_car.sum()

        filtered_p_immobility = self.p_immobility.xs(csp)

        # ---------------------------------------
        # Compute the number of cars based on the city category,
        # the CSP of the reference person and the number of persons in the household
        if n_cars is None:
            n_cars = np.random.choice(
                filtered_p_car.index.to_numpy(), 1, p=filtered_p_car
            )[0]

        all_trips = []

        # === TRAVELS ===
        # 1/ ---------------------------------------
        # Compute the number of travels during n_years given the socio-pro category

        n_travel = n_years * self.n_travels_db.xs(csp).squeeze().astype(int)

        # 2/ ---------------------------------------
        # Sample n_travel travels

        sampled_travels = safe_sample(
            self.travels_db,
            n_travel,
            weights="pondki",
            csp=csp,
            n_cars=n_cars,
            city_category=urban_unit_category,
        )

        # 3/ ---------------------------------------
        # Compute the number of days spent in travel, for professionnal reasons and personnal reasons

        travel_pro_bool = sampled_travels["motive"].str.slice(0, 1) == "9"
        travel_perso_bool = np.logical_not(travel_pro_bool)

        sampled_travels["n_nights"] = sampled_travels["n_nights"].fillna(0)

        # Number of days spent in travel = number of nights + one day per travel
        n_days_travel_pro = int(
            sampled_travels.loc[travel_pro_bool]["n_nights"].sum()
            + travel_pro_bool.sum()
        )
        n_days_travel_perso = int(
            sampled_travels.loc[travel_perso_bool]["n_nights"].sum()
            + travel_perso_bool.sum()
        )

        # 4/ ---------------------------------------
        # Get the long trips corresponding to the travels sampled

        travels_id = sampled_travels["travel_id"].to_numpy()
        sampled_long_trips = self.long_trips_db.loc[travels_id].reset_index()

        # Filter the columns
        sampled_long_trips = sampled_long_trips.loc[
            :,
            [
                "travel_id",
                "previous_motive",
                "motive",
                "mode_id",
                "distance",
                "n_other_passengers",
            ],
        ]
        sampled_long_trips.rename({"travel_id": "trip_id"}, axis=1, inplace=True)
        all_trips.append(sampled_long_trips)

        # 5/ ---------------------------------------
        # Sample days of short trips to simulate the local mobility within a travel :
        #   5.1/ If the travel is for professional reasons
        #   then sample the number of days of the travel from the week days
        #   filtered by the urban category of the destination of the travel
        #   5.2/ If the travel is for personal reasons
        #   then sample the number of days of the travel from the week-end days
        #   filtered by the urban category of the destination of the travel

        days_id = []
        for i in range(sampled_travels.shape[0]):
            # Travel for professionnal reasons
            if sampled_travels.iloc[i]["motive"] == "9":
                n_days_in_travel_pro = int(sampled_travels.iloc[i]["n_nights"] + 1)
                destination_city_category = sampled_travels.iloc[i][
                    "destination_city_category"
                ]
                sampled_days_in_travel_pro = safe_sample(
                    self.days_trip_db,
                    n_days_in_travel_pro,
                    weights="pondki",
                    csp=csp,
                    n_cars=n_cars,
                    weekday=True,
                    city_category=destination_city_category,
                )

                days_id.append(sampled_days_in_travel_pro["day_id"])

            # Travel for personal reasons
            else:
                n_days_in_travel_perso = int(sampled_travels.iloc[i]["n_nights"] + 1)
                destination_city_category = sampled_travels.iloc[i][
                    "destination_city_category"
                ]
                sampled_days_in_travel_perso = safe_sample(
                    self.days_trip_db,
                    n_days_in_travel_perso,
                    weights="pondki",
                    csp=csp,
                    n_cars=n_cars,
                    weekday=False,
                    city_category=destination_city_category,
                )

                days_id.append(sampled_days_in_travel_perso["day_id"])
        days_id = pd.concat(days_id)

        # 6/ ---------------------------------------
        # Get the short trips corresponding to the days sampled

        sampled_short_trips_in_travel = self.short_trips_db.loc[days_id]

        # Filter the columns
        sampled_short_trips_in_travel = sampled_short_trips_in_travel.reset_index().loc[
            :,
            [
                "day_id",
                "previous_motive",
                "motive",
                "mode_id",
                "distance",
                "n_other_passengers",
            ],
        ]
        sampled_short_trips_in_travel.rename(
            {"day_id": "trip_id"}, axis=1, inplace=True
        )
        all_trips.append(sampled_short_trips_in_travel)

        # === DAILY MOBILITY ===
        # 7/ ---------------------------------------
        # Compute the number of immobility days during the week and during the week-end

        # Compute the number of days where there is no travel
        n_week_day = n_years * (52 * 5 - n_days_travel_pro)
        n_weekend_day = n_years * (52 * 2 - n_days_travel_perso)

        n_immobility_week_day = np.round(
            n_week_day * filtered_p_immobility["immobility_weekday"]
        ).astype(int)
        n_immobility_weekend = np.round(
            n_weekend_day * filtered_p_immobility["immobility_weekend"]
        ).astype(int)

        # Compute the number of days where the person is not in travel nor immobile
        n_mobile_week_day = max(
            0, n_years * (52 * 5 - n_days_travel_pro - n_immobility_week_day)
        )
        n_mobile_weekend = max(
            0, n_years * (52 * 2 - n_days_travel_perso - n_immobility_weekend)
        )

        # 8/ ---------------------------------------
        # Sample n_mob_week_day week days and n_mob_weekend week-end days

        sampled_week_days = safe_sample(
            self.days_trip_db,
            n_mobile_week_day,
            weights="pondki",
            csp=csp,
            n_cars=n_cars,
            weekday=True,
            city_category=urban_unit_category,
        )

        sampled_weekend_days = safe_sample(
            self.days_trip_db,
            n_mobile_weekend,
            weights="pondki",
            csp=csp,
            n_cars=n_cars,
            weekday=False,
            city_category=urban_unit_category,
        )

        # 9/ ---------------------------------------
        # Get the short trips corresponding to the days sampled

        days_id = pd.concat(
            [sampled_week_days["day_id"], sampled_weekend_days["day_id"]]
        )
        sampled_short_trips = self.short_trips_db.loc[days_id]
        # Filter the columns
        sampled_short_trips = sampled_short_trips.reset_index().loc[
            :,
            [
                "day_id",
                "previous_motive",
                "motive",
                "mode_id",
                "distance",
                "n_other_passengers",
            ],
        ]
        sampled_short_trips.rename({"day_id": "trip_id"}, axis=1, inplace=True)
        all_trips.append(sampled_short_trips)

        all_trips = pd.concat(all_trips)

        return all_trips
