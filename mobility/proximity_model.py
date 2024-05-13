import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# def proximity_model(data_students, data_schools, costs_territory, data_communes):
    
#     df_origine_dest = pd.DataFrame(index = range(len(data_students["CODGEO"])), columns = ["from", "to", "flow_volume"])
    
#     for i in range(len(data_students["CODGEO"])):
#         dist_min = -1
#         index_dist_min = 0
#         for j in range(len(data_schools["CODGEO"])):
#             if data_schools.at[j, "Tranche_Age"] == Age:
#                 commune_origine = data_students.at[i, "CODGEO"]
#                 commune_dest = data_schools.at[j, "CODGEO"]
                
#                 index_commune_origine = data_communes.index[data_communes['INSEE_COM'] == commune_origine]
#                 index_commune_dest = data_communes.index[data_communes['INSEE_COM'] == commune_dest]
                
#                 x_origine = data_communes.at[index_commune_origine, "x"]
#                 y_origine = data_communes.at[index_commune_origine, "y"]
#                 x_studies = data_communes.at[index_commune_dest, "x"]
#                 y_studies = data_communes.at[index_commune_dest, "y"]
                
                
#                 dist = np.sqrt((x_origine - x_studies)**2 + (y_origine - y_studies)**2)
                    
#                 if dist_min > dist or dist_min < 0:
#                     dist_min = dist
#                     index_dist_min = j

#         df_origine_dest.iloc[i, 0] = commune_origine
#         df_origine_dest.iloc[i, 1] = data_students.at[index_dist_min, "CODGEO"]
#         df_origine_dest.iloc[i, 2] = data_schools.at[i, "Nombre_d_eleves"]
        
        
#     return df_origine_dest

def proximity_model(data_students, data_schools, costs_territory, data_communes):
    
    df_origine_dest = pd.DataFrame(index=range(len(data_students)), columns=["from", "to", "flow_volume"])
    
    for i, commune_origine in enumerate(data_students.index):
        dist_min = -1
        index_dist_min = 0
        
        for j, commune_dest in enumerate(data_schools.index):
                
            x_origine = data_communes.at[commune_origine, "x"]
            y_origine = data_communes.at[commune_origine, "y"]
            x_studies = data_communes.at[commune_dest, "x"]
            y_studies = data_communes.at[commune_dest, "y"]
            
            dist = np.sqrt((x_origine - x_studies)**2 + (y_origine - y_studies)**2)
                
            if dist_min > dist or dist_min < 0:
                dist_min = dist
                index_dist_min = j
                
        df_origine_dest.iloc[i, 0] = commune_origine
        df_origine_dest.iloc[i, 1] = data_schools.index[index_dist_min]
        df_origine_dest.iloc[i, 2] = data_students["source_volume"][commune_origine]
        
    df_origine_dest.set_index(["from", "to"], inplace=True)
    df_origine_dest["flow_volume"] = df_origine_dest["flow_volume"].astype(float)
    
    total_flows = df_origine_dest["flow_volume"]
    
    return total_flows

    


def iter_proximity_model(data_students, data_schools, costs_territory, data_communes, plot=False):
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


    flows = proximity_model(data_students, data_schools, costs_territory, data_communes)
    total_flows = flows["flow_volume"]


    return total_flows


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