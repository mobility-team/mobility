import numpy as np
import pandas as pd

from get_survey_data import get_survey_data

class TripSampler2:
    
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
        
        self.travels_db["nb_nights"] = self.travels_db["nb_nights"].astype(float)


    def get_trips(self, csp, csp_ref_pers, urban_unit_category, n_pers, n_cars=None, n_years=1):
        """
        Sample long distance trips and short distance trips from survey data (prepared with prepare_survey_data),
        for a specific person profile.
        
        Args:
            csp (str): The socio-professional category of the person ("1" to "8", or "no_csp").
            csp_ref_pers (str): The socio-professional category of the household ("1" to "8", or "no_csp").
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
                    previous_trip_motive (str): the motive of the previous trip.
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
        # Create new filter databases according to the socio-pro category, the urban category
        pers_p_car = self.p_car.xs(urban_unit_category).xs(csp_ref_pers).xs(n_pers).squeeze(axis=1)
        pers_p_immobility = self.p_immobility.xs(csp)
        


        # ---------------------------------------
        # Compute the number of cars based on the city category,
        # the CSP of the reference person and the number of persons in the household
        if n_cars is None :
            try :
                n_cars = np.random.choice(pers_p_car.index.to_numpy(), 1, p=pers_p_car)[0]
            except AttributeError :     # if pers_p_car has been squeezed to a float
                pass
        # ---------------------------------------
        # Create new filter databases according to the socio-pro category, the urban category and the motorization
        pers_days_trip_db = self.days_trip_db.xs(urban_unit_category).xs(csp).xs(n_cars)
        pers_travels_db = self.travels_db.xs(urban_unit_category).xs(csp).xs(n_cars)

        all_trips = []
        
        # === TRAVELS ===
        # 1/ ---------------------------------------
        # Compute the number of travels during n_years given the socio-pro category
        n_travel = n_years * np.round(13 * self.n_travels_db.xs(csp)).squeeze().astype(int)
        
                
        # 2/ ---------------------------------------
        # Sample n_travel travels
        try :
            pers_travels = pers_travels_db.reset_index().sample(
                n_travel, weights="pondki", replace=True)
        except KeyError :
            # This part handles the case where pers_travels_db is pd.Series instead of a pd.DataFrame
            pers_travels_db = pd.DataFrame([pers_travels_db])
            pers_travels = pers_travels_db.reset_index().sample(
                n_travel, weights="pondki", replace=True)

        # 3/ ---------------------------------------
        # Compute the number of days spent in travel, for professionnal reasons and personnal reasons
        travel_pro_bool = pers_travels['travel_mot_id'].str.slice(0,1)=='9'
        travel_perso_bool = np.logical_not(travel_pro_bool)
        
        # Number of days spent in professionnal travel = nb of nights + one day per pro travel
        n_days_travel_pro = int(pers_travels.loc[travel_pro_bool]['nb_nights'].sum() + travel_pro_bool.sum())
        n_days_travel_perso = int(pers_travels.loc[travel_perso_bool]['nb_nights'].sum() + travel_perso_bool.sum())        
        
        # 4/ ---------------------------------------
        # Get the long trips corresponding to the travels sampled
        travels_id = pers_travels["travel_id"].to_numpy()
        long_trips = self.long_trips_db.loc[travels_id].reset_index(drop=True)
        
        # Filter the columns
        long_trips = long_trips.loc[:, ["ori_loc_mot_id", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions"]]
        all_trips.append(long_trips)
        
        # 5/ ---------------------------------------
        # Sample n_days_travel_pro and n_days_travel_perso days of short trips
        # to simulate the local mobility in travel
        
        # Sample n_days_travel_pro week days
        pers_days_travel_pro = pers_days_trip_db.xs(True).reset_index(drop=True).sample(
            n_days_travel_pro, weights="pondki", replace=True)
        
        # Sample n_days_travel_perso week-end days
        pers_days_travel_perso = pers_days_trip_db.xs(False).reset_index(drop=True).sample(
            n_days_travel_perso, weights="pondki", replace=True)
        
        # 6/ ---------------------------------------
        # Get the short trips corresponding to the days sampled
        days_id = pd.concat( [pers_days_travel_pro['day_id'], pers_days_travel_perso['day_id']] )
        short_trips_travel = self.short_trips_db.loc[days_id]
        
        # Filter the columns
        short_trips_travel = short_trips_travel.loc[:, ["ori_loc_mot_id", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions"]]
        all_trips.append(short_trips_travel)
        
        # === DAILY MOBILITY ===
        # 7/ ---------------------------------------
        # Compute the number of immobility days during the week and during the week-end
        
        # Compute the number of days where there is no travel
        n_week_day = n_years * (52*5 - n_days_travel_pro)
        n_weekend_day = n_years * (52*2 - n_days_travel_perso)
        
        n_immobility_week_day = np.round(n_week_day * pers_p_immobility['immobility_weekday']).astype(int)
        n_immobility_weekend = np.round(n_weekend_day * pers_p_immobility['immobility_weekend']).astype(int)
        
        # Compute the number of days where the person is not in travel nor immobile
        n_mob_week_day = max(0, n_years * (52*5 - n_days_travel_pro - n_immobility_week_day))
        n_mob_weekend = max(0, n_years * (52*2 - n_days_travel_perso - n_immobility_weekend))
        
        # 8/ ---------------------------------------
        # Sample n_mob_week_day week days and n_mob_weekend week-end days
        pers_week_days = pers_days_trip_db.xs(True).reset_index(drop=True).sample(
            n_mob_week_day, weights="pondki", replace=True)
        pers_weekend_days = pers_days_trip_db.xs(False).reset_index(drop=True).sample(
            n_mob_weekend, weights="pondki", replace=True)
        
        # 9/ ---------------------------------------
        # Get the short trips corresponding to the days sampled
        days_id = pd.concat( [pers_week_days['day_id'], pers_weekend_days['day_id']] )
        short_trips = self.short_trips_db.loc[days_id]
        
        # Filter the columns
        short_trips = short_trips.loc[:, ["ori_loc_mot_id", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions"]]
        all_trips.append(short_trips)
        
        all_trips = pd.concat(all_trips)
        
        return all_trips

ts = TripSampler2(source="EMP-2019")
csp="3"
all_trips = ts.get_trips(csp, csp, "B", "2", n_cars=None, n_years=1)

