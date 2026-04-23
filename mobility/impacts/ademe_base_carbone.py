import requests
import time


def get_emissions_factor(element_id: str, proxies: dict = {}) -> float:
    """
        Queries the ADEME API to get the emissions factor for one element. The
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
        "https://data.ademe.fr/data-fair/api/v1/datasets/base-carboner/"
        "lines?page=1&after=1&size=12&sort=&select=&highlight=&format=json&"
        "html=false&q_mode=simple&qs="
    )

    query = (
        "Identifiant_de_l'élément:{element_id} AND "
        "Type_Ligne:Elément"
    ).format(element_id=element_id)

    request_url += query

    r = requests.get(request_url, proxies=proxies).json()
    time.sleep(0.1)

    emissions_factor = r["results"][0]["Total_poste_non_décomposé"]

    return emissions_factor
