import logging
import pandas as pd
import numpy as np
import polars as pl
import pathlib
from scipy.optimize import minimize_scalar
import matplotlib.pyplot as plt
from scipy.linalg import lstsq

from mobility.r_utils.r_script import RScript

# def radiation_model_selection(
#     sources, sinks, costs, utilities, selection_lambda, max_iter=20, plot=False
# ):
    
#     # Convert input DataFrames to Polars DataFrames
#     sources = pl.DataFrame(sources.reset_index()).with_columns([
#         pl.col("from").cast(pl.Int64)
#     ])
    
#     sinks = pl.DataFrame(sinks.reset_index()).with_columns([
#         pl.col("to").cast(pl.Int64)
#     ])
    
    
#     flows = compute_flows(
#         sources,
#         sinks,
#         costs,
#         utilities,
#         selection_lambda
#     )
    
#     flows = flows.to_pandas().set_index(["from", "to"])["flow_volume"]

#     return flows, 0.0, 0.0




def apply_radiation_model(sources, sinks, costs, utilities, selection_lambda):

    eps = 1e-6
    
    # Smooth the opportunities locations and costs to account for uncertainty
    # (for now with a constant cost delta and a gaussian distribution, but 
    # could be estimated from the cost uncertainty specific to each OD)
    def offset_costs(costs, delta, prob):
        return (
            costs
            .with_columns([
                (pl.col("cost") + delta).alias("cost"),
                pl.lit(prob).alias("prob")
            ])
        )
    
    costs = pl.concat([
        offset_costs(costs, -2.0, 0.07),
        offset_costs(costs, -1.0, 0.24),
        offset_costs(costs, +0.0, 0.38),
        offset_costs(costs, +1.0, 0.24),
        offset_costs(costs, +2.0, 0.07)
    ])

    # Merge sources, sinks, costs and utilities
    od = (
        sources
        .join(costs, on="from")
        .join(sinks, on="to")
        .join(utilities, on="to")
        .with_columns([(pl.col("sink_volume")*pl.col("prob")).alias("sink_volume")])
        
    )
    
    # Compute the net utility for each OD
    od = od.with_columns([
        (pl.col("utility") - 2*pl.col("cost")).alias('net_utility')
    ])
    
    # Bin the net utility and compute the probability of each destination within
    # each from -> net utility bin
    od = od.with_columns([pl.col("net_utility").round().alias("net_utility_bin")])
    od = od.with_columns([(pl.col("sink_volume")/pl.col("sink_volume").sum().over(["from", "net_utility_bin"])).alias("p_to_bin")])
    
    # Aggregate opportunities within each bin, for each origin
    od_bin = od.group_by(["from", "net_utility_bin"]).agg(pl.col("sink_volume").sum())

    # Compute the probabilities to choose a destination according to the
    # radiation model with selection
    od_bin = od_bin.sort(['from', 'net_utility_bin'], descending=[False, True])
    
    od_bin = od_bin.with_columns([
        pl.col('sink_volume')
        .cum_sum()
        .over('from')
        .alias('s_ij')
    ])
    
    od_bin = od_bin.with_columns([
        ((1 - selection_lambda**(1+pl.col('s_ij'))) / (1+pl.col('s_ij')) / (1-selection_lambda)).alias('p_a'),
    ])
    
    
    od_bin = od_bin.with_columns([
        pl.col('p_a')
        .shift(fill_value=1.0)
        .over('from')
        .alias('p_a_lag')
    ])
    
    od_bin = od_bin.with_columns([
        (pl.col('p_a_lag') - pl.col('p_a')).alias('p_ij')
    ])

    # Normalize p_ij within each group
    od_bin = od_bin.with_columns([
        (pl.col('p_ij') / pl.col('p_ij').sum().over('from'))
        .alias('p_ij')
    ])
    
    
    # Disagregate the flows from source to destination
    # First step : source -> bin (with p_ij)
    # Second step : bin -> destination (with p_to_bin)
    flows = (
        od_bin
        .select(["from", "net_utility_bin", "p_ij"])
        .join(sources.select(["from", "source_volume"]), on="from")
        .join(od.select(["from", "net_utility_bin", "to", "p_to_bin"]), on=["from", "net_utility_bin"])
    )
    
    flows = flows.with_columns([
        (pl.col('source_volume') * pl.col('p_ij') * pl.col("p_to_bin"))
        .alias('flow_volume')
    ])
    
    # Re-aggregate at OD level
    flows = flows.group_by(['from', 'to']).agg([
        pl.col('source_volume').first(),
        pl.col('flow_volume').sum()
    ])
    
    # Remove small flows and rescale the remaining flows so that the source 
    # volumes stay the same
    flows = (
        flows
        .filter(pl.col('flow_volume') > 0.1)
        .with_columns([(pl.col('flow_volume') / pl.col('flow_volume').sum().over('from')).alias('p_ij')])
        .with_columns((pl.col("source_volume")*pl.col("p_ij")).alias("flow_volume"))
    )
    
    return flows.select(['from', 'to', 'flow_volume'])





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
