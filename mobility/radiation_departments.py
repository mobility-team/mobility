from mobility.get_insee_data import get_insee_data
from mobility.parsers import download_work_home_flows
import mobility.radiation_model as rm
import numpy as np
import pandas as pd
import os
from pathlib import Path



COMMUNES_COORDINATES_CSV = "donneesCommunesFrance.csv"
COMMUNES_SURFACES_CSV = "donneesCommunesFrance.csv"
WORK_HOME_FLUXES_CSV = download_work_home_flows()


# FUNCTIONS
def get_data_for_model(
    lst_departments,
    work_home_fluxes_csv=WORK_HOME_FLUXES_CSV,
    communes_coordinates_csv=COMMUNES_COORDINATES_CSV,
    communes_surfaces_csv=COMMUNES_SURFACES_CSV,
    alpha=0,
    beta=1,
    test=False,
):
    """
    Gets data for the given departments.

    Uses the get_insee_data function for active population ans jobs
    Uses local data for communes' superficies, intra-communal distance
    and work-home mobility
    (by default, will use the national CSVs in the directory)
    Todo:
     * improve the get_insee_data function for work-home data, coordinates
       and superficies, to not have to deal with local data
     * compute the internal distance within the code instead of using a CSV

    Parameters
    ----------
    lst_departments : list
        List of departements for which to get the data.
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

    # Import the data (active population and jobs)
    insee_data = get_insee_data(test=test)
    db_actifs = insee_data["active_population"]
    db_emplois = insee_data["jobs"]

    db_emplois["EMPLT"] = db_emplois[
        [
            "n_jobs_CS1",
            "n_jobs_CS2",
            "n_jobs_CS3",
            "n_jobs_CS4",
            "n_jobs_CS5",
            "n_jobs_CS6",
        ]
    ].sum(axis=1)
    db_emplois.reset_index(inplace=True)

    db_actifs["ACT"] = db_actifs[
        [
            "active_pop_CS1",
            "active_pop_CS2",
            "active_pop_CS3",
            "active_pop_CS4",
            "active_pop_CS5",
            "active_pop_CS6",
        ]
    ].sum(axis=1)
    db_actifs.reset_index(inplace=True)

    # Only keep the sinks in the chosen departements
    sinks_territory = db_emplois.loc[:, ["CODGEO", "EMPLT"]]
    sinks_territory["DEP"] = sinks_territory["CODGEO"].str.slice(0, 2)
    mask = sinks_territory["DEP"].apply(lambda x: x in lst_departments)
    sinks_territory = sinks_territory.loc[mask]

    sinks_territory = sinks_territory.set_index("CODGEO")
    sinks_territory.rename(columns={"EMPLT": "sink_volume"}, inplace=True)
    sinks_territory = sinks_territory.drop(columns=["DEP"])

    # Only keep the sinks in the chosen departements
    sources_territory = db_actifs.loc[:, ["CODGEO", "ACT"]]
    sources_territory["DEP"] = sources_territory["CODGEO"].str.slice(0, 2)
    mask = sources_territory["DEP"].apply(lambda x: x in lst_departments)
    sources_territory = sources_territory.loc[mask]

    sources_territory = sources_territory.set_index("CODGEO")
    sources_territory = sources_territory.drop(columns=["DEP"])
    sources_territory.rename(columns={"ACT": "source_volume"}, inplace=True)

    data_folder_path = Path(os.path.dirname(__file__)).joinpath("data").joinpath("insee").joinpath("territories")

    # Import the INSEE data on the work-home mobility on Millau
    print("data folder path:", data_folder_path, "csv:", work_home_fluxes_csv)
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
    mask = raw_flowDT["DEP"].apply(lambda x: x in lst_departments)
    mask2 = raw_flowDT["DEP2"].apply(lambda x: x in lst_departments)
    raw_flowDT = raw_flowDT.loc[mask]
    raw_flowDT = raw_flowDT.loc[mask2]

    # Import the geographic data on the work-home mobility on Millau

    coordinates = pd.read_csv(
        data_folder_path / communes_coordinates_csv,
        sep=",",
        usecols=["NOM_COM", "INSEE_COM", "x", "y"],
        dtype={"INSEE_COM": str},
    )
    coordinates.set_index("INSEE_COM", inplace=True)
    # The multiplication by 1000 is only for visualization purposes
    coordinates["x"] = coordinates["x"] * 1000
    coordinates["y"] = coordinates["y"] * 1000

    surfaces = pd.read_csv(
        data_folder_path / communes_surfaces_csv,
        sep=",",
        usecols=["INSEE_COM", "distance_interne"],
        dtype={"INSEE_COM": str},
    )
    surfaces.set_index("INSEE_COM", inplace=True)

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


def run_model_for_territory(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordinates,
    raw_flowDT,
    alpha=0,
    beta=1,
    subset=None,
):
    """
    Runs the model and visualises output

    Runs the modal with the given sources, sinks and costs
    Provides 4 maps:
        * Volume of active people per commune
        * Volume of jobs per commune
        * Flows generated by the radiation model
        * Flows estimated by INSEE

    Parameters
    ----------
    sources_territory, sinks_territory, costs_territory,
        coordinates, raw_flowDT
        all of them are pandas dataframes defined in the get_data function
    alpha : float, optional
        The default is 0.
    beta : float, optional
        The default is 1.
    subset:
        List of communal codes.
        Flows originating from these communes will be plotted in a separate figure
        Default is None

    Returns
    -------
    flowsRM : pandas dataframe
        Flows between communes produced by the radiation model.
    flowDT : pandas dataframe
        Flows between communes estimated by INSEE.
    coordinates : pandas dataframe
        Coordinates of the communes.
    plot_sources : pandas dataframe
        Modified version of the sources dataframe.

    """

    print(
        "Model running with {} sources, {} sinks and {} costs".format(
            len(sources_territory), len(sinks_territory), len(costs_territory)
        )
    )

    # COMPUTE THE MODEL
    (total_flows, source_rest_volume, sink_rest_volume) = rm.iter_radiation_model(
        sources_territory,
        sinks_territory,
        costs_territory,
        alpha=alpha,
        beta=beta,
        plot=False,
    )

    # PLOT THE SOURCES AND THE SINKS
    plot_sources = sources_territory.rename(columns={"source_volume": "volume"})
    rm.plot_volume(plot_sources, coordinates, n_locations=10, title="Volume d'actifs")

    plot_sinks = sinks_territory.rename(columns={"sink_volume": "volume"})
    rm.plot_volume(plot_sinks, coordinates, n_locations=10, title="Volume d'emplois")

    # PLOT THE FLOWS COMPUTED BY THE MODEL

    plot_flows = total_flows.reset_index()
    plot_sources = sources_territory

    rm.plot_flow(
        plot_flows,
        coordinates,
        sources=None,
        n_flows=500,
        n_locations=20,
        size=10,
        title=(
            "(1) Flux domicile-travail générés par le modèle"
            " - alpha = {} - beta = {}"
        ).format(alpha, beta),
    )

    # PLOT SUBSET FLOWS

    if subset is not None:
        print("Visualisation for the chosen subset")
        mask = plot_flows["from"].apply(lambda x: x in subset)
        plot_subset_flows = plot_flows.loc[mask]
        rm.plot_flow(
            plot_subset_flows,
            coordinates,
            sources=None,
            n_flows=500,
            n_locations=len(subset)//5,
            size=10,
            title=(
                "(1) Flux domicile-travail générés par le modèle dans l'échantillon"
                " - alpha = {} - beta = {}"
            ).format(alpha, beta),
        )

    # PLOT THE FLOWS FROM THE INSEE DATA

    plot_flowDT = raw_flowDT.groupby(["COMMUNE", "DCLT"])["IPONDI"].sum().reset_index()
    plot_flowDT.rename(
        columns={"IPONDI": "flow_volume", "COMMUNE": "from", "DCLT": "to"}, inplace=True
    )

    rm.plot_flow(
        plot_flowDT,
        coordinates,
        sources=plot_sources,
        n_flows=500,
        n_locations=20,
        size=10,
        title="(2) Flux domicile-travail mesurés par l'INSEE",
    )

    # EXPORT THE MODEL AND THE INSEE DATA

    flowDT = raw_flowDT.rename(
        columns={"IPONDI": "flow_volume", "COMMUNE": "from", "DCLT": "to"}
    )
    flowDT = flowDT.groupby(["from", "to"])["flow_volume"].sum()
    flowDT = pd.DataFrame(flowDT)

    flowsRM = pd.DataFrame(total_flows)

    print("Model flow of {} and empirical flow of {}".format(len(flowsRM), len(flowDT)))

    return flowsRM, flowDT, coordinates, plot_sources


def compare_thresholds(
    predicted_flux, empirical_flux, thresholds=[400, 200, 100, 50, 25, 20, 15, 10, 5]
):
    """
    Shows the SSI for different thresholds.

    The SSI should be higher for high thresholds.
    INSEE says:
    « The sample sizes below 200 must be handled with caution because,
    due to the imprecision associated with sampling,
    https://www.insee.fr/fr/information/2383290

    Parameters
    ----------
    predicted_flux : pd.DataFrame with a flow_volume column
                     and a MultiIndex with two INSEE codes
        Contains all the fluxes computed by the model
    empirical_flux : pd.DataFrame with a flow_volume column
                     and a MultiIndex with two INSEE codes
        Contains all the empirical fluxes.
    thresholds : list of int
        Different thresholds you want to test.
        By default a list of values from 400 to 5
    Returns
    -------
    None.

    """
    return {t: compute_similarity_index(predicted_flux, empirical_flux, t) for t in thresholds}


def compute_similarity_index(predicted_flux, empirical_flux, threshold=200):
    """
    Computes a modified Sørensen similarity index n (SSI) between the fluxes

    Uses the formula from "A universal opportunity model for human mobility",
    developped by Liu and Yan
    In their datasets, their SSI range between 0.5 and 0.8

    If the pair exists only in the empirical flow (INSEE),
    it is not taken into account in the SSI and is counted as an exception.
    If you have a high number of exceptions, check the data.

    Parameters
    ----------
    predicted_flux : pd.DataFrame with a flow_volume column
                     and a MultiIndex with two INSEE codes
                     Contains all the fluxes computed by the model
    empirical_flux : pd.DataFrame with a flow_volume column
                     and a MultiIndex with two INSEE codes
                     Contains all the empirical fluxes.
    threshold : int
        Under this threshold, the fluxes won't be considered for the index.
        This reflects that under 200, INSEE data is less precise.

    Returns
    -------
    Float with the similarity index
    """
    ssi = 0
    n = 0
    under_threshold = 0
    exc = 0
    for pair in empirical_flux.iterrows():
        pair_ids = pair[0]
        t2_ij = float(pair[1])
        if t2_ij >= threshold:
            try:
                t_ij = float(predicted_flux.loc[pair_ids])
                if t_ij + t2_ij > 0:
                    ssi += 2 * min(t_ij, t2_ij) / (t_ij + t2_ij)
                    n += 1
            except KeyError:
                exc += 1
                pass
        else:
            under_threshold += 1
    ssi = ssi / n
    ssi_print = f"{ssi:.3f}"
    print(
        "Similarity index:",
        ssi_print,
        "after",
        n,
        "steps.",
        under_threshold,
        "pairs under a threshold of",
        threshold,
        "--",
        exc,
        "exceptions",
    )
    return ssi


def optimise_parameters(
    sources_territory,
    sinks_territory,
    costs_territory,
    coordinates,
    raw_flowDT,
    coef=1,
    threshold=20,
    test=False,
):
    """
    Finds the values of α and β giving the best SSI for the given coef.

    Tries all the combinations of α and β by 0.1 steps
    (with 0 <= α,β <= 1, and α+β <= 1 )

    Parameters
    ----------
    coef : float, optional
        Correction coefficient.
        All the model outputs are multiplied by this coef.
        It can increase the similarity. The default is 1.

    Returns
    -------
    best_pair : alpha (float), beta(float)
        Best α,β pair found. α+β <= 1

    """
    best_score = 0
    best_pair = [np.nan, np.nan]
    print("Finding the best α,β pair")
    for alpha in np.arange(0, 1.1, 0.1):
        for beta in np.arange(0, 1.1, 0.1):
            if alpha + beta < 1.05:
                if not test:
                    print(f"\n\nα = {alpha:.1f}, β ={beta:.1f}")
                    (
                        predicted_flux,
                        empirical_flux,
                        coordinates,
                        plot_sources,
                    ) = run_model_for_territory(
                        sources_territory.copy(),
                        sinks_territory,
                        costs_territory,
                        coordinates,
                        raw_flowDT,
                        alpha=alpha,
                        beta=beta,
                    )
                    ssi = compute_similarity_index(
                    coef * predicted_flux, empirical_flux, threshold
                    )
                else:
                    ssi=0
                if ssi > best_score:
                    best_score = ssi
                    best_pair = [alpha, beta]
    print("Best α,β pair found is", best_pair)
    return best_pair


def compare_insee_and_model(predicted_flux, empirical_flux, coordinates, plot_sources):
    """
    Compares INSEE data and the model output, showing common share of the flow

    Also provides maps of flows from both models

    Parameters
    ----------
    predicted_flux : pd.DataFrame with a flow_volume column
                     and a MultiIndex with two INSEE codes
        Contains all the fluxes computed by the model
    empirical_flux : pd.DataFrame with a flow_volume column
                     and a MultiIndex with two INSEE codes
        Contains all the empirical fluxes.
    coordinates : pd.DataFrame with x,y coordinates of each territory
    plot_sources : pd.DataFrame with the source volume of each territory

    Returns
    -------
    Float with the repartition error
    """
    # Join on the couple origin destinations
    # how = 'inner' to keep only the couples that are in both dataframes
    flow_join = empirical_flux.join(
        predicted_flux, how="inner", lsuffix="DT", rsuffix="RM"
    )
    flow_join.reset_index(inplace=True)

    # Compare visually model and INSEE data
    fig = flow_join.plot(x="flow_volumeDT", y="flow_volumeRM", style="o")
    # fig.set_xlim(0,2500) # Use this to set the limit of the figure if needed
    # fig.set_ylim(0,2500)

    # Compare the total flow
    print("The 2 dataframes have {} OD in common\n".format(flow_join.shape[0]))

    sum_flow_DT = flow_join["flow_volumeDT"].sum()
    intra_flow_mask = flow_join["from"] == flow_join["to"]
    intra_flow_DT = flow_join.loc[intra_flow_mask, "flow_volumeDT"].sum()
    print(
        "Total flow of the INSEE data :\n"
        "   {:.0f} ({:.0f}% intra-city flow)".format(
            sum_flow_DT, 100 * intra_flow_DT / sum_flow_DT
        )
    )

    sum_flow_RM = flow_join["flow_volumeRM"].sum()
    intra_flow_RM = flow_join.loc[intra_flow_mask, "flow_volumeRM"].sum()
    intra_city_flow = 100 * intra_flow_RM / sum_flow_RM
    print(
        "Total flow of the model :\n"
        "   {:.0f} ({:.0f}% intra-city flow)\n".format(sum_flow_RM, intra_city_flow)
    )

    # Compare the repartition between the ODs
    flow_join["repartitionDT"] = (
        flow_join["flow_volumeDT"] / flow_join["flow_volumeDT"].sum()
    )
    flow_join["repartitionRM"] = (
        flow_join["flow_volumeRM"] / flow_join["flow_volumeRM"].sum()
    )

    error_repartition = np.abs(flow_join["repartitionDT"] - flow_join["repartitionRM"])

    print(
        "The repartitions from the INSEE data and the data"
        "have {:.2f}% in common.".format(100 - 50 * error_repartition.sum())
    )

    # similarity = compute_similarity_index(flowRM,flowDT)
    # print("Similarity between the model and the INSEE data is ", similarity)

    plot_DT = pd.DataFrame(flow_join[["from", "to", "repartitionDT"]])
    plot_DT.rename(columns={"repartitionDT": "flow_volume"}, inplace=True)
    plot_RM = pd.DataFrame(flow_join[["from", "to", "repartitionRM"]])
    plot_RM.rename(columns={"repartitionRM": "flow_volume"}, inplace=True)

    rm.plot_flow(
        plot_DT,
        coordinates,
        sources=plot_sources,
        n_flows=500,
        size=10,
        n_locations=20,
        title="(3) Flux domicile-travail mesurés par l'INSEE",
    )
    rm.plot_flow(
        plot_RM,
        coordinates,
        sources=plot_sources,
        n_flows=500,
        size=10,
        n_locations=20,
        title="(4) Flux domicile-travail générés par le modèle",
    )
    return error_repartition
