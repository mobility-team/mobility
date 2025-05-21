import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def radiation_model(sources, sinks, costs, alpha=0, beta=1):
    """
    This function computes the volume of flows between source and sink nodes,
    according to the radiation model. The model takes into account the volume
    of "demand" and "opportunities" at each node (active persons and jobs for
    example). It takes also into account the cost/benefit delta for a person
    when going from one node to another. The nodes can represent any kind of
    transport zone (neighborhood, city...).

    Args:
        sources (pd.DataFrame):
            Index:
                transport_zone_id (str): unique id of the transport zone.
            Columns:
                source_volume (float): volume of "demand" of the transport zones.
        sinks (pd.DataFrame):
            Index:
                transport_zone_id (str): unique id of the transport zone.
            Columns:
                sink_volume (float): volume of "opportunities" of the transport zones.
        costs (pd.DataFrame):
            Index:
                from (str): trip origin transport zone id.
                to (str): trip destination transport zone id.
            Columns:
                cost (float): cost/benefit to go from the origin (source) to the destination (sink).
        alpha (float):
            Must be in [0,1] s.t alpha+beta<=1.
            Parameter of the radiation model: reflects the behavior of the individual's tendency
            to choose the destination whose benefit is higher than the benefits of the origin
            and the intervening opportunities .
            (see "A universal opportunity model for human mobility", developped by Liu and Yan)
        beta (float):
            Must be in [0,1] s.t alpha+beta<=1.
            Parameter of the radiation model: reflects the behavior of the individual’s tendency
            to choose the destination whose benefit is higher than the benefit of the origin,
            and the benefit of the origin is higher than the benefits of the intervening opportunities.
            (see "A universal opportunity model for human mobility", developped by Liu and Yan)
    Returns:
        flows (pd.DataFrame):
            Index:
                from (str): trip origin transport zone id (source).
                to (str): trip destination transport zone id (sink).
            Columns:
                flow_volume (float): flow volume between source and sink nodes.
        source_rest_volume (pd.Series):
            Index:
                from (str): unique id of the transport zone.
            Name:
                source_volume (float): rest of the volum of demand of the transport zone.
        sink_rest_volume (pd.Series):
            Index:
                to (str): unique id of the transport zone.
            Name:
                sink_volume (float): rest of the volume of oopportunities of the transport zone.
    """

    # The pseudo-code is in the documentation. 

    # Epsilon value under which values are set to 0 in order to avoid numerical errors
    # during the successive iterations
    eps = 1e-6

    matrix_origin_destinations = pd.merge(
        sources, costs, left_index=True, right_on="from"
    )
    matrix_origin_destinations = pd.merge(
        matrix_origin_destinations, sinks, left_on="to", right_index=True
    )

    # Compute the number of "intervening opportunities"
    # = total volume of sinks in locations which are less costly than the location at hand
    # To do so for each origin, compute the cumulative sum of the sink volume in ascending ordrer of cost
    matrix_origin_destinations.sort_values(by=["from", "cost"], inplace=True, ascending=False)
    matrix_origin_destinations["s_ij"] = matrix_origin_destinations.groupby("from")[
        "sink_volume"
    ].cumsum()

    matrix_origin_destinations["s_ij"] = np.maximum(
        matrix_origin_destinations["s_ij"] - matrix_origin_destinations["sink_volume"],
        0,
    )

    # Compute the probabilities with the UO model
    matrix_origin_destinations["p_ij"] = (
        matrix_origin_destinations["source_volume"]
        + alpha * matrix_origin_destinations["s_ij"]
    )
    matrix_origin_destinations["p_ij"] *= matrix_origin_destinations["sink_volume"]
    matrix_origin_destinations["p_ij"] /= (
        matrix_origin_destinations["source_volume"]
        + (alpha + beta) * matrix_origin_destinations["s_ij"]
    )
    matrix_origin_destinations["p_ij"] /= (
        matrix_origin_destinations["source_volume"]
        + (alpha + beta) * matrix_origin_destinations["s_ij"]
        + matrix_origin_destinations["sink_volume"]
    )

    # Keep only the first 95% of the distribution ?
    
    # Normalize the probabilities such that the sum for each origin is equal to one
    temp = matrix_origin_destinations.groupby("from")["p_ij"].sum()
    temp.name = "p_i"

    matrix_origin_destinations.reset_index(inplace=True)
    matrix_origin_destinations = pd.merge(
        matrix_origin_destinations, temp, left_on="from", right_index=True
    )
    matrix_origin_destinations.set_index(["from", "to"], inplace=True)

    matrix_origin_destinations["p_ij"] /= matrix_origin_destinations["p_i"]

    matrix_origin_destinations["p_ij"].where(
        matrix_origin_destinations["p_ij"].notna(), 0, inplace=True
    )

    
    # The flow volume is calibrated to respect the total source volume
    # (not the total sink volume)
    matrix_origin_destinations["flow_volume"] = (
        matrix_origin_destinations["source_volume"] * matrix_origin_destinations["p_ij"]
    )

    # Possibility to directly modifity the p_ij to adjust the flow according to the sinks rather than having an iterative process?

    # Set to 0 the small flow volume in order to avoid numerical errors
    # during the next iterations
    matrix_origin_destinations["flow_volume"].where(
        matrix_origin_destinations["flow_volume"] > eps, 0.0, inplace=True
    )

    # Compute the overflow for each sink : sum(flow_volume) - sink_volume
    overflow = (
        matrix_origin_destinations.groupby("to")["flow_volume"].sum()
        - matrix_origin_destinations.groupby("to")["sink_volume"].first()
    )
    overflow = overflow.where(overflow > 0, 0.0)
    overflow.name = "overflow"
    matrix_origin_destinations = pd.merge(
        matrix_origin_destinations, overflow, how="left", left_on="to", right_index=True
    )

    # Substract the overflow to the flow volume
    # so that the flow volume stays samller than the sink volume
    # This overflow is split among the origins according to its contribution

    # Contribution of each origin to every sink
    flow_volume_per_sink = matrix_origin_destinations.groupby("to")["flow_volume"].sum()
    flow_volume_per_sink.name = "flow_volume_per_sink"
    matrix_origin_destinations = pd.merge(
        matrix_origin_destinations, flow_volume_per_sink, left_on="to", right_index=True
    )

    mask = matrix_origin_destinations["flow_volume_per_sink"] > 0

    # Substract the overflow to the flow volume weighted by the contribution of the origin
    matrix_origin_destinations.loc[
        mask, "flow_volume"
    ] = matrix_origin_destinations.loc[mask, "flow_volume"] * (
        1
        - matrix_origin_destinations.loc[mask, "overflow"]
        / matrix_origin_destinations.loc[mask, "flow_volume_per_sink"]
    )

    matrix_origin_destinations["flow_volume"].where(
        matrix_origin_destinations["flow_volume"] != np.inf, 0, inplace=True
    )

    # Compute the rest of the demand volume and the sink volume
    source_rest_volume = (
        matrix_origin_destinations.groupby("from")["source_volume"].first()
        - matrix_origin_destinations.groupby("from")["flow_volume"].sum()
    )
    sink_rest_volume = (
        matrix_origin_destinations.groupby("to")["sink_volume"].first()
        - matrix_origin_destinations.groupby("to")["flow_volume"].sum()
    )
    source_rest_volume.name = "source_volume"
    sink_rest_volume.name = "sink_volume"

    source_rest_volume.where(source_rest_volume > eps, 0, inplace=True)
    sink_rest_volume.where(sink_rest_volume > eps, 0, inplace=True)

    return (
        matrix_origin_destinations[["flow_volume"]],
        source_rest_volume,
        sink_rest_volume,
    )


def iter_radiation_model(
    sources, sinks, costs, alpha=0, beta=1, max_iter=20, plot=False
):
    """
    Iterates the radiation model between source and sink nodes.

    At each iteration, the flows between the sources and the sinks are computed
    as well as the rest of the volume of demand and opportunities,
    according to the radiation model. The next iteration, the rest of the volume
    of demand and opportunities are used.
    The iterations stops after max_iter or when the flow volume computed is small
    compared to the total source volume.

    Args:
        sources (pd.DataFrame):
            Index:
                transport_zone_id (str): unique id of the transport zone.
            Columns:
                source_volume (float): volume of "demand" of the transport zones.
        sinks (pd.DataFrame):
            Index:
                transport_zone_id (str): unique id of the transport zone.
            Columns:
                sink_volume (float): volume of "opportunities" of the transport zones.
        costs (pd.DataFrame):
            Index:
                from (str): trip origin transport zone id.
                to (str): trip destination transport zone id.
            Columns:
                cost (float): cost/benefit to go from the origin (source) to the destination (sink).
        alpha (float):
            Must be in [0,1] s.t alpha+beta<=1.
            Parameter of the radiation model: reflects the behavior of the individual's tendency
            to choose the destination whose benefit is higher than the benefits of the origin
            and the intervening opportunities .
            (see "A universal opportunity model for human mobility", developped by Liu and Yan)
        beta (float):
            Must be in [0,1] s.t alpha+beta<=1.
            Parameter of the radiation model: reflects the behavior of the individual’s tendency
            to choose the destination whose benefit is higher than the benefit of the origin,
            and the benefit of the origin is higher than the benefits of the intervening opportunities.
            (see "A universal opportunity model for human mobility", developped by Liu and Yan)
        max_iter (int):
            Maximum number of iterations of the radiation model.
        plot (boolean):
            Indicates whether the evolution of the demand volume and opportunities volume
            should be plotted.

    Returns:
        total_flows (pd.Series):
            Index:
                from (str): trip origin transport zone id (source).
                to (str): trip destination transport zone id (sink).
                Columns:
                flow_volume (float): flow volume between source and sink nodes.
        source_rest_volume (pd.Series):
            Index:
                from (str): unique id of the transport zone.
            Name:
                source_volume (float): rest of the volum of demand of the transport zone.
        sink_rest_volume (pd.Series):
            Index:
                to (str): unique id of the transport zone.
            Name:
                sink_volume (float): rest of the volume of oopportunities of the transport zone.
    """
    
    # First iteration of the radiation model
    iteration = 1
    flows, source_volume, sink_volume = radiation_model(
        sources, sinks, costs, alpha=alpha, beta=beta
    )
    total_flows = flows["flow_volume"]

    total_source_volume = sources["source_volume"].sum()

    rest_source = []
    rest_sink = []
    rest_source.append(source_volume.sum())
    rest_sink.append(sink_volume.sum())

    # The convergence criteria is when the volume of flow computed at the iteration i
    # is less than 1% of the original source volume
    while (
        iteration < max_iter and flows["flow_volume"].sum() > 0.01 * total_source_volume
    ):
        # logging.info("Iteration n°{} of the radiation model".format(iteration))
        iteration += 1

        # Compute the radiation model with the rest of the demand and sink volume
        flows, source_volume, sink_volume = radiation_model(
            source_volume, sink_volume, costs, alpha=alpha, beta=beta
        )

        total_flows += flows["flow_volume"]

        rest_source.append(source_volume.sum())
        rest_sink.append(sink_volume.sum())

    if iteration == max_iter:
        logging.info("The iterations of the radiation model didn't converge")
    if plot:
        plt.figure(figsize=(18, 5))
        plt.subplot(121)
        plt.plot(np.arange(1, iteration + 1), rest_source)
        plt.xticks(np.arange(1, iteration + 1))
        plt.xlabel("n° itérations")
        plt.ylabel("Demand volume not fulfilled")
        
        print("Total demand volume : {}".format(sources["source_volume"].sum()))
        print(
            "Rest of demand volume after {} iterations : {}".format(
                iteration, rest_source[-1]
            )
        )

        plt.subplot(122)
        plt.plot(np.arange(1, iteration + 1), rest_sink)
        plt.xticks(np.arange(1, iteration + 1))
        plt.xlabel("n° itérations")
        plt.ylabel("Opportunity volume not fulfilled")

        print("Total opportunity volume : {}".format(sinks["sink_volume"].sum()))
        print(
            "Rest of opportunity volume after {} iterations : {}".format(
                iteration, rest_sink[-1]
            )
        )
    
    # Remove possible null flows
    total_flows = total_flows[total_flows > 0.0]

    return total_flows, source_volume, sink_volume


def plot_volume(volume_location, coordinates, n_locations=10, title=""):
    """
    Plot each location whose size is proportional to the volume at the location.
    Display also the label of the location for the biggest n_locations.

    Args:
        volume_location (pd.DataFrame): a dataframe with one row per location
            Index:
                CODGEO (str): geographic code of the location
            Columns:
                volume (float): volume at the location
        coordinates (pd.DataFrame): a dataframe with one row per location
            Index:
                CODGEO (str): geographic code of the location
            Columns:
                NOM_COM (str): name of the location
                x (float): x coordinate of the location
                y (float): y coordinate of the location
        n_locations (int): display the labels of only the biggest n_locations locations
            according to the volume from sources
    """
    plt.figure()
    plt.title(title)
    # Normalization
    volume_location["volume"] = (
        100
        * (volume_location["volume"] - volume_location["volume"].min())
        / volume_location["volume"].max()
    )

    # Plot the locations
    volume_location = pd.merge(
        volume_location, coordinates, left_index=True, right_index=True
    )
    plt.scatter(volume_location["x"], volume_location["y"], s=volume_location["volume"])

    # n_locations biggest location to display
    volume_location.sort_values(by="volume", inplace=True)
    idx_show = volume_location.iloc[-n_locations:].index

    volume_location.sort_index(inplace=True)
    for idx in idx_show:
        plt.text(
            volume_location.loc[idx, "x"],
            volume_location.loc[idx, "y"],
            volume_location.loc[idx, "NOM_COM"][0:14],
        )
    return


def plot_flow(
    flows, coordinates, sources=None, n_flows=100, n_locations=5, size=1, title=""
):
    """
    Plots the flows between the locations.

    The bigger the flow is, the bigger the plot line
    will be. THe points are the locations. If sources=None, then the size of each location
    is proportionnal to the internal flow within the location. Otherwise, the size of the
    location is proportionnal to the source volume from sources.

    Args:
        flows (pd.DataFrame): a dataframe with one row per couple origin/destination
            Columns:
                from (str): geographic code of the flow origin
                to (str): geographic code of the flow destination
                flow_volume (float): flow volume between the origin and the destination
        coordinates (pd.DataFrame): a dataframe with one row per location
            Index:
                CODGEO (str): geographic code of the location
            Columns:
                NOM_COM (str): name of the location
                x (float): x coordinate of the location
                y (float): y coordinate of the location
        sources (pd.DataFrame): a dataframe with one row per location
            If None is passed
            Index:
                CODGEO (str): geographic code of the location
            Columns:
                source_volume (float): the source volume at the location
        n_flows (int): plot only the n_flows biggest flows (to avoid too heavy computationss)
            If n_flows=-1 then plot all the flows
        n_locations (int): display the labels of only the biggest n_locations locations
            according to the volume from sources
        size (int): determines the size of the figure (size>=1). Default is 1.
    """

    # Normalization
    flows["flow_volume"] = (
        size
        * 100
        * (flows["flow_volume"] - flows["flow_volume"].min())
        / flows["flow_volume"].max()
    )

    # Get the coordinates for each origin/destination
    flows = pd.merge(flows, coordinates, left_on="from", right_index=True)
    flows.rename({"x": "from_x", "y": "from_y"}, axis=1, inplace=True)
    flows = pd.merge(flows, coordinates, left_on="to", right_index=True)
    flows.rename({"x": "to_x", "y": "to_y"}, axis=1, inplace=True)
    flows.sort_values(by="flow_volume", ascending=False, inplace=True)

    idx_show = flows.iloc[:n_flows].index

    plt.figure(figsize=(6 * size, 4 * size))
    plt.title(title, fontsize=5 * size)
    # Plot the flows
    for idx in idx_show:
        plt.plot(
            [flows.loc[idx, "from_x"], flows.loc[idx, "to_x"]],
            [flows.loc[idx, "from_y"], flows.loc[idx, "to_y"]],
            linewidth=flows.loc[idx, "flow_volume"],
            color="lightblue",
            zorder=0,
        )

    if sources is None:
        # Plot the locations based on the internal flow
        internal_flows = flows.loc[flows["from"] == flows["to"]]
        # Normalization
        internal_flows.loc[:, "flow_volume"] = (
            size**2
            * 100
            * (
                internal_flows.loc[:, "flow_volume"]
                - internal_flows["flow_volume"].min()
            )
            / internal_flows["flow_volume"].max()
        )
        plt.scatter(
            internal_flows["from_x"],
            internal_flows["from_y"],
            s=internal_flows["flow_volume"],
            zorder=1,
        )

        # n_locations biggest location to display
        internal_flows.sort_values(by="flow_volume", ascending=False, inplace=True)

        temp = internal_flows.iloc[:n_locations].index
        idx_show = internal_flows.loc[temp, "from"].to_numpy()
        for idx in idx_show:
            plt.text(
                coordinates.loc[idx, "x"],
                coordinates.loc[idx, "y"],
                coordinates.loc[idx, "NOM_COM"][0:14],
                fontsize=5 * size,
            )
    else:
        # Normalization
        sources["source_volume"] = (
            size**2
            * 100
            * (sources["source_volume"] - sources["source_volume"].min())
            / sources["source_volume"].max()
        )

        # Plot the locations
        sources = pd.merge(sources, coordinates, left_index=True, right_index=True)
        plt.scatter(sources["x"], sources["y"], s=sources["source_volume"], zorder=1)

        # n_locations biggest location to display
        sources.sort_values(by="source_volume", inplace=True)
        idx_show = sources.iloc[-n_locations:].index

        sources.sort_index(inplace=True)
        for idx in idx_show:
            plt.text(
                sources.loc[idx, "x"],
                sources.loc[idx, "y"],
                sources.loc[idx, "NOM_COM"][0:14],
                fontsize=5 * size,
            )
