import logging
import pandas as pd
import numpy as np
import polars as pl
from scipy.optimize import minimize_scalar
import matplotlib.pyplot as plt

def radiation_model_selection(sources, sinks, costs, selection_lambda):

    eps = 1e-6

    od = pd.merge(sources, costs, left_index=True, right_index=True)
    od = pd.merge(od, sinks, left_index=True, right_index=True)
    
    # Split each OD to account for uncertainty in the cost calculation
    multipliers = np.array([0.05, 0.25, 0.4, 0.25, 0.05])
    adjustments = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    
    od_list = []
    for mult, adj in zip(multipliers, adjustments):
        temp = od.copy()
        temp["net_utility"] += adj
        temp["sink_volume"] *= mult
        od_list.append(temp)
        
    od = pd.concat(od_list)
    
    
    od.sort_values(by=["from", "net_utility"], inplace=True, ascending=False)
    
    od["s_ij"] = od.groupby("from")["sink_volume"].cumsum()
    
    # od["p_a"] = (1 - np.power(selection_lambda, 1+od["s_ij"]))/(1+od["s_ij"])/(1-selection_lambda)
    
    od["p_a"] = (
        1 - np.where(
            od["country_id"] == "fr",
            np.power(selection_lambda["fr"], 1 + od["s_ij"]),
            np.power(selection_lambda["ch"], 1 + od["s_ij"])
        )
    ) / (1 + od["s_ij"]) / (
        1 - np.where(
            od["country_id"] == "fr",
            selection_lambda["fr"],
            selection_lambda["ch"]
        )
    )
    
    od["p_a_lag"] = od.groupby("from")["p_a"].shift(fill_value = 1.0)
    
    od["p_ij"] = (od["p_a_lag"] - od["p_a"])
    od["p_ij"] /= od.groupby("from")["p_ij"].transform("sum")
    
    # The flow volume is calibrated to respect the total source volume
    od["flow_volume"] = od["source_volume"]*od["p_ij"]
    
    # Reagregate at OD level
    od = od.groupby(["from", "to"]).agg({"source_volume": "first", "sink_volume": "sum", "flow_volume": "sum"})

    # Remove very small flows
    od["p_ij"] = od["flow_volume"]/od["source_volume"]
    od = od[od["flow_volume"] > 1.0].copy()
    od["flow_volume"] /= od.groupby("from")["p_ij"].transform("sum")

    # Set to 0 the small flow volume in order to avoid numerical errors
    # during the next iterations
    od["flow_volume"].where(od["flow_volume"] > eps, 0.0, inplace=True)
    
    od = od["flow_volume"]

    return od



def radiation_model_selection_polars(sources, sinks, costs, selection_lambda, country_zone=dict()):

    eps = 1e-6

    # Merge sources, costs, and sinks
    od = sources.join(costs, on="from").join(sinks, on="to")
    
    # Create modified versions of the 'od' DataFrame
    multipliers = [0.05, 0.25, 0.4, 0.25, 0.05]
    adjustments = [-2.0, -1.0, 0.0, 1.0, 2.0]

    # Create and concatenate modified DataFrames
    od_list = [
        od.with_columns([
            (pl.col('net_utility') + adj).alias('net_utility'),
            (pl.col('sink_volume') * mult).alias('sink_volume')
        ])
        for adj, mult in zip(adjustments, multipliers)
    ]
    
    od = pl.concat(od_list)
    
    # Sort values by 'from' and 'net_utility' in descending order
    od = od.sort(['from', 'net_utility'], descending=[False, True])
    
    # Calculate cumulative sum of 'sink_volume'
    od = od.with_columns([
        pl.col('sink_volume').cum_sum().over('from').alias('s_ij')
    ])

    # Calculate probabilities
    od = od.with_columns([
        ((1 - selection_lambda ** (1 + pl.col('s_ij'))) / (1 + pl.col('s_ij')) / (1 - selection_lambda)).alias('p_a'),
    ])
    
    # Calculate probabilities with a differentiated lambda for ch and fr
    # od = od.with_columns([
    #     (
    #         (1 - pl.when(pl.col('country_id') == 'fr')
    #                 .then(selection_lambda['fr'] ** (1 + pl.col('s_ij')))
    #                 .otherwise(selection_lambda['ch'] ** (1 + pl.col('s_ij')))
    #         ) / (1 + pl.col('s_ij')) / 
    #         (1 - pl.when(pl.col('country_id') == 'fr')
    #                 .then(selection_lambda['fr'])
    #                 .otherwise(selection_lambda['ch']))
    #     ).alias('p_a'),
    # ])
    
    
    od = od.with_columns([
        pl.col('p_a').shift(fill_value=1.0).over('from').alias('p_a_lag')
    ])
    
    od = od.with_columns([
        (pl.col('p_a_lag') - pl.col('p_a')).alias('p_ij')
    ])

    # Normalize p_ij within each group
    od = od.with_columns([
        (pl.col('p_ij') / pl.col('p_ij').sum().over('from')).alias('p_ij')
    ])
    
    # Calibrate flow volume to respect the total source volume
    od = od.with_columns([
        (pl.col('source_volume') * pl.col('p_ij')).alias('flow_volume')
    ])
    
    # Re-aggregate at OD level
    od = od.group_by(['from', 'to']).agg([
        pl.col('source_volume').first(),
        pl.col('sink_volume').sum(),
        pl.col('flow_volume').sum()
    ])
    
    # Remove very small flows
    od = od.with_columns([
        (pl.col('flow_volume') / pl.col('source_volume')).alias('p_ij')
    ])
    
    od = od.filter(pl.col('flow_volume') > 1.0).with_columns([
        (pl.col('flow_volume') / pl.col('p_ij').sum().over('from')).alias('flow_volume')
    ])
    
    # Set small flow volumes to 0 to avoid numerical errors during the next iterations
    od = od.with_columns([
        pl.when(pl.col('flow_volume') > eps).then(pl.col('flow_volume')).otherwise(0.0).alias('flow_volume')
    ])
    
    return od.select(['from', 'to', 'flow_volume'])


def calculate_total_utility(flows, costs):
    return flows.join(costs, on=["from", "to"]).with_columns([
        (pl.col("net_utility") * pl.col("flow_volume")).alias("total_flow_utility")
    ]).select(pl.col("total_flow_utility").sum()).item()

def update_costs(sinks, flows, costs):
    
    sinks_occupation = flows.group_by("to").agg(
        pl.col("flow_volume").sum().alias("sink_occupation")
    )
    
    sinks_occupation = sinks.join(
        sinks_occupation,
        on="to",
        how="left",
        coalesce=True
    ).with_columns(
        pl.col("sink_occupation").fill_null(0.0)
    )
    
    sinks_occupation = sinks_occupation.with_columns([
        pl.when(
            pl.col("sink_occupation") > pl.col("sink_volume")
        ).then(
            1.0 / ((pl.col("sink_occupation") / pl.col("sink_volume")) ** 1)
        ).otherwise(
            1.0
        ).alias("k_utility")
    ])
    
    updated_costs = costs.join(sinks_occupation.select(["to", "k_utility"]), on="to")
    updated_costs = updated_costs.with_columns([
        (pl.col("utility") * pl.col("k_utility") - pl.col("cost")).alias("net_utility")
    ])
    
    return updated_costs.select(["from", "to", "cost", "utility", "net_utility"])

def conjugate_frank_wolfe(sources, sinks, costs, selection_lambda, max_iterations=20, tolerance=1e-3):
    
    # Initial flow using radiation model
    flows = radiation_model_selection_polars(sources, sinks, costs, selection_lambda)
    updated_costs = update_costs(sinks, flows, costs)
    total_utility = calculate_total_utility(flows, updated_costs)
    
    # Initialize direction and previous direction
    direction = None
    
    for iteration in range(max_iterations):
        
        logging.info(f"Iteration {iteration + 1} of the Frank-Wolfe method")
            
        # Compute new flows
        new_flows = radiation_model_selection_polars(sources, sinks, updated_costs, selection_lambda)
        
        # Direction finding
        direction = flows.join(
            new_flows,
            on=["from", "to"],
            how="left", coalesce=True
        ).with_columns([
            pl.col("flow_volume_right").fill_null(0.0)
        ]).with_columns([
            (pl.col("flow_volume_right") - pl.col("flow_volume")).alias("direction")
        ]).select(["from", "to", "direction"])
        
        # Line search to find the optimal step size
        def objective_function(g):
            
            adjusted_flows = flows.join(direction, on=["from", "to"]).with_columns([
                (pl.col("flow_volume") + g * pl.col("direction")).alias("flow_volume")
            ]).select(["from", "to", "flow_volume"])
            
            adjusted_costs = update_costs(sinks, adjusted_flows, costs)
            
            total_utility = calculate_total_utility(adjusted_flows, adjusted_costs)
            total_utility *= -1.0
            
            return total_utility
        
        gamma = minimize_scalar(objective_function, bounds=(0, 1), method='bounded').x
        
        # Update flows
        flows = flows.join(direction, on=["from", "to"]).with_columns([
            (pl.col("flow_volume") + gamma * pl.col("direction")).alias("flow_volume")
        ]).select(["from", "to", "flow_volume"])
        
        updated_costs = update_costs(sinks, flows, costs)
        
        new_total_utility = calculate_total_utility(flows, updated_costs)
        
        # Check for convergence
        if np.abs(new_total_utility - total_utility)/total_utility < tolerance:
            break
        
        total_utility = new_total_utility
    
    return flows



def iter_radiation_model_selection(
    sources, sinks, costs, selection_lambda, max_iter=20, plot=False
):
    
    # First iteration of the radiation model
    iteration = 1
    logging.info("Iteration n°{} of the radiation model".format(iteration))
    
    # Convert input DataFrames to Polars DataFrames
    sources = pl.DataFrame(sources.reset_index()).with_columns([
        pl.col("from").cast(pl.Int64)
    ])
    
    sinks = pl.DataFrame(sinks.reset_index()).with_columns([
        pl.col("to").cast(pl.Int64)
    ])
    
    costs = pl.DataFrame(costs.reset_index()).with_columns([
        pl.col("from").cast(pl.Int64),
        pl.col("to").cast(pl.Int64)
    ])
    
    flows = conjugate_frank_wolfe(
        sources,
        sinks,
        costs,
        selection_lambda
    )
    
    flows = flows.to_pandas().set_index(["from", "to"])["flow_volume"]

    return flows, 0.0, 0.0


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
