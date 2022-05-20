#ttest

def prepare_entd_2008():

    # Préparer la dataframe des déplacements locaux
    # On va essayer de changer de logique d'échantillonage :
    # Au lieu d'échantilloner des déplacements, on va échantilloner des journées entières de déplacements
    # On aura de cette manière plus d'informations sur les chaines de déplacements, qui sont déterminantes dans le choix de modes notamment
    # On évite aussi d'avoir à calculer un nombre de déplacements par jour

    # Préparer la dataframe des voyages

    # Préparer la dataframe du nombre de voyages par année


def prepare_emd_2018_2019():

    # Même démarche que pour l'ENTD 2008


def get_survey_data(source):
    """
    This function transforms raw survey data into dataframes needed for the sampling procedures.
    
    Args:
        source (str) : The source of the travels and trips data ("ENTD-2008" or "EMD-2018-2019", the default).

    Returns:
        survey_data (dict) : a dict of dataframes.
            "short_trips" (pd.DataFrame)
            "travels" (pd.DataFrame)
            "n_travels" (pd.DataFrame)
    """
    
    # Tester si les fichiers parquet existent déjà pour la source demandée
    # Si oui, charger les parquet dans un dict
    # Si non, utiliser les fonctions de préparation pour les créer avant de les charger dans un dict