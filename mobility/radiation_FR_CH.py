from mobility.get_insee_data import get_insee_data
from mobility.get_swiss_data import get_swiss_data
from mobility.parsers import download_work_home_flows
from pyproj import Transformer
import numpy as np
import pandas as pd
import os
from pathlib import Path


COMMUNES_COORDINATES_CSV = "donneesCommunesFrance.csv"
COMMUNES_SURFACES_CSV = "donneesCommunesFrance.csv"
WORK_HOME_FLUXES_CSV = download_work_home_flows()

# incomplete list, should be expanded
CODES_CANTON = {"NE": ["64", "65"], "VD": ["54", "55", "56", "57", "58", "59"]}
SWISS_WAGES_COEF = 2.0


# FUNCTIONS
def get_franco_swiss_data_for_model(
    territories_list,
    purpose="jobs",
    work_home_fluxes_csv=WORK_HOME_FLUXES_CSV,
    communes_coordinates_csv=COMMUNES_COORDINATES_CSV,
    communes_surfaces_csv=COMMUNES_SURFACES_CSV,
    alpha=0,
    beta=1,
    test=False,
):
    """
    Gets data for the given territories list, including French departments
    and Swiss cantons.
    Currently only configured for the cantons of Neuchâtel and Vaud. To add
    more cantons, it would be needed to find the corresponding prefixes in
    their geogrophical codes.

    Uses get_insee_data function for active population and jobs in France
    Uses get_swiss_data function for similar data in Switzerland
    Uses French and Swiss local data for communes' superficies,
    intra-communal distance and work-home mobility
    Todo:
     * improve the get_insee_data function for work-home data, coordinates
       and superficies, to not have to deal with local data
     * compute the internal distance within the code instead of using a CSV
     * add all Swiss cantons
     * better match between years (in case of changes in the list of communes)

    Parameters
    ----------
    territories_list : list
        List of French partements and Swiss cantons for which to get the data.
    work_home_fluxes_csv : string, optional
        Which CSV to use for work-home data.
        The default is WORK_HOME_FLUXES_CSV defined at the top of the file.
    communes_coordinates_csv : string, optional
        Which CSV to use for coordinates data.
        The default is COMMUNES_COORDINATES_CSV.
    communes_surfaces_csv : string, optional
        Which CSV to use for surfaces data.
        The default is COMMUNES_SURFACES_CSV.
    alpha : float, optional
        The default is 0.
    beta : float, optional
        The default is 1.

    Returns sources_territory, sinks_territory, costs_territory,
            coordinates, raw_flowDT
            (all of them are pandas dataframes)

    """
    # ===================
    # IMPORT AND PROCESS THE DATA

    departements = []
    swiss_codes = []
    for code in territories_list:
        print(code)
        if code.isnumeric():
            departements.append(code)
        else:
            swiss_codes += CODES_CANTON[code]
    print(departements, swiss_codes)

    # Import the data (active population and jobs)
    # > France
    insee_data = get_insee_data(test=test)
    db_sources = insee_data["active_population"]
    db_opportunities = insee_data["jobs"]

    db_opportunities["EMPLT"] = db_opportunities[
        [
            "n_jobs_CS1",
            "n_jobs_CS2",
            "n_jobs_CS3",
            "n_jobs_CS4",
            "n_jobs_CS5",
            "n_jobs_CS6",
        ]
    ].sum(axis=1)
    db_opportunities.reset_index(inplace=True)

    # Only keep the sinks in the chosen departements
    sinks_territory = db_opportunities.loc[:, ["CODGEO", "EMPLT"]]
    sinks_territory["DEP"] = sinks_territory["CODGEO"].str.slice(0, 2)
    mask = sinks_territory["DEP"].apply(lambda x: x in departements)
    sinks_territory = sinks_territory.loc[mask]

    sinks_territory["CODGEO"] = "87" + sinks_territory["CODGEO"]
    sinks_territory = sinks_territory.set_index("CODGEO")
    sinks_territory.rename(columns={"EMPLT": "sink_volume"}, inplace=True)
    sinks_territory = sinks_territory.drop(columns=["DEP"])

    db_sources["ACT"] = db_sources[
        [
            "active_pop_CS1",
            "active_pop_CS2",
            "active_pop_CS3",
            "active_pop_CS4",
            "active_pop_CS5",
            "active_pop_CS6",
        ]
    ].sum(axis=1)
    db_sources.reset_index(inplace=True)

    # Only keep the sources in the chosen departements
    french_sources = db_sources.loc[:, ["CODGEO", "ACT"]]
    french_sources["DEP"] = french_sources["CODGEO"].str.slice(0, 2)
    mask = french_sources["DEP"].apply(lambda x: x in departements)
    french_sources = french_sources.loc[mask]

    french_sources["CODGEO"] = "87" + french_sources["CODGEO"]
    french_sources = french_sources.set_index("CODGEO")
    french_sources = french_sources.drop(columns=["DEP"])
    french_sources.rename(columns={"ACT": "source_volume"}, inplace=True)

    # > Switzerland
    swiss_data = get_swiss_data()
    swiss_db_sources = swiss_data["active_population"]
    swiss_db_opportunities = swiss_data["jobs"]

    # Only keep the sinks in the chosen swiss cantons
    swiss_opportunities = swiss_db_opportunities.loc[:, ["Code", "Emplois"]]
    swiss_opportunities["COD2"] = swiss_opportunities["Code"].str.slice(0, 2)
    mask = swiss_opportunities["COD2"].apply(lambda x: x in swiss_codes)
    swiss_opportunities = swiss_opportunities.loc[mask]
    # Hard fix: do not work for communes with 3-digit codes
    # However works for the entire Suisse romande
    mask2 = swiss_opportunities["Code"].apply(lambda x: len(x) > 3)
    swiss_opportunities = swiss_opportunities.loc[mask2]
    swiss_opportunities["sink_volume"] = pd.to_numeric(swiss_opportunities["Emplois"]) * SWISS_WAGES_COEF
    swiss_opportunities_volume = swiss_opportunities.loc[:, ["Code", "sink_volume"]]
    swiss_opportunities_volume["Code"] = "85" + swiss_opportunities_volume["Code"]
    swiss_opportunities_volume.set_index('Code', inplace=True)

    sinks_territory = pd.concat([sinks_territory, swiss_opportunities_volume])

    # Only keep the sources in the chosen swiss cantons
    swiss_sources = swiss_db_sources.loc[:, ["Territoire", "ACT"]]
    swiss_sources["COD2"] = swiss_sources["Territoire"].str.slice(6, 8)
    mask = swiss_sources["COD2"].apply(lambda x: x in swiss_codes)
    swiss_sources = swiss_sources.loc[mask]
    swiss_sources["Commune"] = swiss_sources["Territoire"].str.slice(11, 42)
    swiss_sources["CODGEO"] = "85" + swiss_sources["Territoire"].str.slice(6, 10)
    swiss_sources_volume = swiss_sources.loc[:, ["CODGEO", "ACT"]]
    swiss_sources_volume.rename(columns={"ACT": "source_volume"}, inplace=True)
    swiss_sources_volume.set_index('CODGEO', inplace=True)

    sources_territory = pd.concat([french_sources, swiss_sources_volume])

    data_folder_path = Path(os.path.dirname(__file__)).joinpath("data").joinpath("insee").joinpath("territories")
    ch_data_folder_path = Path(os.path.dirname(__file__)) / "data/CH"

    # > France only
    # Import the INSEE data on the work-home mobility
    file_path = os.path.join(data_folder_path, work_home_fluxes_csv)
    raw_flowDT = pd.read_csv(
        file_path,
        sep=";",
        usecols=["COMMUNE", "DCLT", "IPONDI", "TRANS"],
        dtype={"COMMUNE": str, "DCLT": str, "IPONDI": float, "TRANS": int},
    )

    # Only keep the flows in the given departments
    raw_flowDT["DEP"] = raw_flowDT["COMMUNE"].str.slice(0, 2)
    raw_flowDT["DEP2"] = raw_flowDT["DCLT"].str.slice(0, 2)
    mask = raw_flowDT["DEP"].apply(lambda x: x in territories_list)
    mask2 = raw_flowDT["DEP2"].apply(lambda x: x in territories_list)
    raw_flowDT = raw_flowDT.loc[mask]
    raw_flowDT = raw_flowDT.loc[mask2]

    # > France
    # Import coordinates
    coordinates = pd.read_csv(
        data_folder_path / communes_coordinates_csv,
        sep=",",
        usecols=["NOM_COM", "INSEE_COM", "x", "y"],
        dtype={"INSEE_COM": str},
    )
    coordinates["INSEE_COM"] = "87" + coordinates["INSEE_COM"]
    coordinates.set_index("INSEE_COM", inplace=True)
    # The multiplication by 1000 is only for visualization purposes
    coordinates["x"] = coordinates["x"] * 1000
    coordinates["y"] = coordinates["y"] * 1000

    # > Switzerland
    # Import coordinates and convert to Lambert
    swiss_coordinates = pd.read_csv(ch_data_folder_path / "CH-2023-repertoire-localites.csv",
                                    sep=";",
                                    usecols=["BFS-Nr", "E", "N", "Ortschaftsname"])
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154")
    lambert_coords = swiss_coordinates.apply(lambda row: pd.Series(transformer.transform(row['N'], row['E'])),
                                             axis=1, result_type='expand')
    lambert_coords.columns = ['x', 'y']
    swiss_coordinates = swiss_coordinates.merge(lambert_coords, left_index=True, right_index=True)
    swiss_coordinates["BFS-Nr"] = "85" + swiss_coordinates["BFS-Nr"].astype("str")
    swiss_coordinates.rename(columns={"Ortschaftsname": "NOM_COM", "BFS-Nr": "INSEE_COM"}, inplace=True)
    swiss_coordinates.drop_duplicates(subset=["INSEE_COM"], inplace=True)
    # Limitation here: keeps only the same value, some coords could be slightly different, name could be totally different

    swiss_coordinates.set_index("INSEE_COM", inplace=True)

    # Merge coordinates
    coordinates = pd.concat([coordinates, swiss_coordinates])

    # > France
    # Import surfaces
    surfaces = pd.read_csv(
        data_folder_path / communes_surfaces_csv,
        sep=",",
        usecols=["INSEE_COM", "distance_interne"],
        dtype={"INSEE_COM": str},
    )
    surfaces["INSEE_COM"] = "87" + surfaces["INSEE_COM"]
    surfaces.set_index("INSEE_COM", inplace=True)

    # > Switzerland
    # Import surfaces
    swiss_surfaces = pd.read_csv(
        ch_data_folder_path / "CH-2016-superficie-communes.csv",
        sep=";",
        usecols=["CODE_REGION", "distance_interne"],
        )
    swiss_surfaces["CODE_REGION"] = "85" + swiss_surfaces["CODE_REGION"]
    swiss_surfaces.rename(columns={"CODE_REGION": "INSEE_COM"}, inplace=True)
    swiss_surfaces.set_index("INSEE_COM", inplace=True)

    # Merge coordinates
    surfaces = pd.concat([surfaces, swiss_surfaces])

    # Compute the distance between cities
    #    distance between i and j = (x_i - x_j)**2 + (y_i - y_j)**2
    lst_communes = sources_territory.index.to_numpy()
    idx_from_to = np.array(np.meshgrid(lst_communes, lst_communes)).T.reshape(-1, 2)
    idx_from = idx_from_to[:, 0]
    idx_to = idx_from_to[:, 1]
    costs_territory = pd.DataFrame(
        {"from": idx_from, "to": idx_to, "cost": np.zeros(idx_to.shape[0])}
    )

    costs_territory = pd.merge(
        costs_territory, coordinates, left_on="from", right_index=True
    )
    costs_territory.rename(columns={"x": "from_x", "y": "from_y"}, inplace=True)
    costs_territory = pd.merge(
        costs_territory, coordinates, left_on="to", right_index=True
    )
    costs_territory.rename(columns={"x": "to_x", "y": "to_y"}, inplace=True)

    costs_territory = pd.merge(
        costs_territory, surfaces, left_on="from", right_index=True
    )

    costs_territory["cost"] = np.sqrt(
        (costs_territory["from_x"] / 1000 - costs_territory["to_x"] / 1000) ** 2
        + (costs_territory["from_y"] / 1000 - costs_territory["to_y"] / 1000) ** 2
    )

    # distance if the origin and the destination is the same city
    # is internal distance = 128*r / 45*pi
    # where r = sqrt(surface of the city)/pi
    mask = costs_territory["from"] != costs_territory["to"]
    costs_territory["cost"].where(
        mask, other=costs_territory["distance_interne"], inplace=True
    )

    return (
        sources_territory,
        sinks_territory,
        costs_territory,
        coordinates,
        raw_flowDT,
    )
