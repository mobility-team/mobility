import requests
import pandas as pd
import pathlib
import os
import time
from ratelimiter import RateLimiter

def prepare_emissions_factors():
    """
        Query the ADEME API to get the emissions factor for the transportation 
            modes listed in data/ademe/mapping.csv, and write the result in
            data/ademe/ef.csv.

        Returns:
            None
    """

    data_folder_path = pathlib.Path(os.path.dirname(__file__)).parents[0] / "data"

    emissions_factors = pd.read_csv(
        data_folder_path / "ademe/mapping.csv",
        usecols=["mobility_ef_name", "ademe_id"],
        dtype=str
    )

    emissions_factors["ef"] = emissions_factors["ademe_id"].apply(get_emissions_factor)
    emissions_factors["database"] = "ADEME - Base carbone"

    emissions_factors.to_csv(data_folder_path / "ademe/ef.csv", index=False)

    return None


@RateLimiter(max_calls=1, period=0.1)
def get_emissions_factor(element_id: str, proxies: dict = {}) -> float:
    """
        Query the ADEME API to get the emissions factor for one element. The 
        API call is throttled to respect the 10 req/s limit.

        Args:
            - element_id (str): the id of the element in the Base Carbone.
            - proxies (dict): an optional dict providing http_proxy and
                https_proxy values for the HTTP GET request.

        Returns:
            - emissions_factor (float): the emissions factor in kgCO2e, per unit
                of element.
    """

    print(
        "Getting emissions factor from ADEME Base carbone for the element with id : "
        + element_id
    )

    request_url = (
        "https://data.ademe.fr/data-fair/api/v1/datasets/base-carbone(r)/"
        "lines?page=1&after=1&size=12&sort=&select=&highlight=&format=json&"
        "html=false&q_mode=simple&qs="
    )

    query = (
        "Identifiant_de_l'élément:{element_id} AND "
        "Type_Ligne:Elément"
    ).format(element_id=element_id)

    request_url += query

    r = requests.get(request_url, proxies=proxies).json()

    emissions_factor = r["results"][0]["Total_poste_non_décomposé"]
    
    return emissions_factor

