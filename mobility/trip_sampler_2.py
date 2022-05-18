
class TripSampler2:
    
    def __init__(self, source="EMD-2018-2019"):
        """
        Create a TripSampler object for a given survey.
        
        Args:
            source (str) : The source of the travels and trips data ("ENTD-2008" or "EMD-2018-2019", the default).

        Returns:
            TripSampler2: the TripSampler object, ready for sampling.
        """

        # Charger les dataframes nécessaires pour l'échantillonage avec get_survey_data(source)


    def get_trips(socio_professional_category, n_cars, urban_unit_category, n_years=1):
        """
        Sample long distance travels and short distance trips from survey data (prepared with prepare_survey_data),
        # for a specific person profile.
        
        Args:
            socio_professional_category (str): The socio-professional category of the person ("1" to "8", or "no_csp").
            n_cars (str): The number of cars of the household ("0", "1", or "2+").
            urban_unit_category (str): The urban unit category ("C", "B", "I", "R").
            n_years (int): The number of years of trips to sample (1 to N, defaults to 1).
            source (str) : The source of the travels and trips data ("ENTD-2008" or "EMD-2018-2019", the default).

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




