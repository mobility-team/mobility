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

    # Pseudo code
    # Calculer la probabilité des déplacements entre sources et puits,
    # en fonction des coûts/bénéfices de transport entre sources et puits,
    # d'après le modèle de radiation

    # Fusionner costs et sources
    # Fusionner le résultat avec sinks
    # -> dataframe from, to, cost, source_volume, sink_volume

    # Classer la dataframe selon l'origin et le cout par ordre croissant
    # Calculer les opportunités cumulées (intervening_opportunities) pour chaque origine,
    # en partant du cout le plus faible

    # Calculer la probabilité de déplacement donnée par le modèle de radiation
    # p_ij = source_volume*sink_volume/(source_volume + intervening_opportunities)/(source_volume + sink_volume + intervening_opportunities)

    # Normaliser les probabilités pour que la somme par origine soit égale à 1.

    # Calculer les flux résultants (flow_volume = source_volume*p_ij)

    # Calculer la part des flux à "renvoyer" à la source parce que le puits est saturé
    # pour chaque puits, overflow = sum(flow_volume) - sink_volume
    # cet overflow est réparti par source en fonction de sa contribution aux flux global
    # pour chaque source, backflow = overflow*flow_volume/sum(flow_volume)

    # Corriger les flux avec le backflow
    # flow_volume = flow_volume - backflow

    # Calculer les sources et les puits non attribués
    # pour chaque source, source_volume - sum(flow_volume)
    # pour chaque puits, sink_volume - sum(flow_volume)

    # Stocker la dataframe des flux dans une liste

    # Itération de la procédure avec les sources et puits non attribués, jusqu'à convergence :
    # les puits sont saturés (somme des volumes non attribués des puits < tolerance)
    # ou les sources sont vides (somme des sources non attribuées < tolerance)

    # Informer l'utilisateur sur l'équilibrage ou non du système :
    # Résidu de volumes de puits non attribués
    # Résidu de volumes de sources non attribués
    # Exemple pour le motif domicile travail : 
    # Emplois non pourvus / Actifs sans emploi

    # Concatener puis retourner la dataframe des flux.

    # Epsilon value under which values are set to 0 in order to avoid numerical errors
    # during the successive iterations
    eps = 1e-6

    matrix_origin_destinations = pd.merge(sources, costs, left_index=True, right_on="from")
    matrix_origin_destinations = pd.merge(matrix_origin_destinations, sinks, left_on="to", right_index=True)

    # Compute the number of "intervening opportunities"
    # = total volume of sinks in locations which are less costly than the location at hand
    # To do so for each origin, compute the cumulative sum of the sink volume in ascending ordrer of cost
    matrix_origin_destinations.sort_values(by=['from', 'cost'], inplace=True)
    matrix_origin_destinations['s_ij'] = matrix_origin_destinations.groupby('from')['sink_volume'].cumsum()

    matrix_origin_destinations["s_ij"] = np.maximum(matrix_origin_destinations["s_ij"] - matrix_origin_destinations["sink_volume"], 0)

    # Compute the probabilities with the UO model
    matrix_origin_destinations["p_ij"] = (matrix_origin_destinations['source_volume'] + alpha*matrix_origin_destinations["s_ij"])
    matrix_origin_destinations["p_ij"] *= matrix_origin_destinations['sink_volume']
    matrix_origin_destinations["p_ij"] /= (matrix_origin_destinations['source_volume'] + (alpha+beta)*matrix_origin_destinations["s_ij"])
    matrix_origin_destinations["p_ij"] /= (matrix_origin_destinations['source_volume'] + (alpha+beta)*matrix_origin_destinations["s_ij"] + matrix_origin_destinations['sink_volume'])

    # Keep only the first 95% of the distribution ?

    # Normalize the probabilities such that the sum for each origin is equal to one
    matrix_origin_destinations["p_ij"] /= matrix_origin_destinations.groupby('from')["p_ij"].sum()

    matrix_origin_destinations["p_ij"].where(matrix_origin_destinations["p_ij"].notna(), 0, inplace=True)

    # The flow volume is calibrated to respect the total source volume
    # (not the total sink volume)
    matrix_origin_destinations['flow_volume'] = matrix_origin_destinations['source_volume'] * matrix_origin_destinations["p_ij"]

    # Possibilité de jouer directement sur les p_ij pour ajuster les flux avec les puits plutôt
    # que de faire un processus itératif par la suite ?

    # Set to 0 the small flow volume in order to avoid numerical errors
    # during the next iterations
    matrix_origin_destinations['flow_volume'].where(matrix_origin_destinations['flow_volume']>eps, 0, inplace=True)

    # Compute the overflow for each sink : sum(flow_volume) - sink_volume
    overflow = matrix_origin_destinations.groupby('to')['flow_volume'].sum() - matrix_origin_destinations.groupby('to')['sink_volume'].first()
    overflow = overflow.where(overflow>0, 0)
    overflow.name='overflow'
    matrix_origin_destinations = pd.merge(matrix_origin_destinations, overflow, how='left', left_on='to', right_index=True)

    # Substract the overflow to the flow volume
    # so that the flow volume stays lesser than the sink volume
    # This overflow is split among the origins according to its contribution

    # Contribution of each origin to every sink
    flow_volume_per_sink = matrix_origin_destinations.groupby('to')['flow_volume'].sum()
    flow_volume_per_sink.name = "flow_volume_per_sink"
    matrix_origin_destinations = pd.merge(matrix_origin_destinations, flow_volume_per_sink, left_on='to', right_index=True)

    mask = matrix_origin_destinations['flow_volume_per_sink']>0

    # Substract the overflow to the flow volume weighted by the contribution of the origin
    matrix_origin_destinations.loc[mask, 'flow_volume'] = matrix_origin_destinations.loc[mask, 'flow_volume'] *(1 - matrix_origin_destinations.loc[mask, 'overflow']/matrix_origin_destinations.loc[mask, 'flow_volume_per_sink'])

    matrix_origin_destinations['flow_volume'].where(matrix_origin_destinations['flow_volume']!=np.inf, 0, inplace=True)

    # Compute the rest of the demand volume and the sink volume
    source_rest_volume = matrix_origin_destinations.groupby('from')['source_volume'].first() - matrix_origin_destinations.groupby('from')['flow_volume'].sum()
    sink_rest_volume = matrix_origin_destinations.groupby('to')['sink_volume'].first() - matrix_origin_destinations.groupby('to')['flow_volume'].sum()
    source_rest_volume.name = "source_volume"
    sink_rest_volume.name = "sink_volume"

    source_rest_volume.where(source_rest_volume>eps, 0, inplace=True)
    sink_rest_volume.where(sink_rest_volume>eps, 0, inplace=True)

    return matrix_origin_destinations[['flow_volume']], source_rest_volume, sink_rest_volume

def iter_radiation_model(sources, sinks, costs, max_iter=20, plot=False):
    """
    This function iterates the radiation model between source and sink nodes.
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
    iteration=1
    # First iteration of the radiation model
    flows, source_volume, sink_volume = radiation_model(sources, sinks, costs)
    total_flows = flows['flow_volume']

    total_source_volume = sources['source_volume'].sum()

    rest_source = []
    rest_sink = []
    rest_source.append(source_volume.sum())
    rest_sink.append(sink_volume.sum())
    
    # The convergence criteria is when the volume of flow computed at the iteration i
    # is less than 1% of the original source volume
    while iteration<max_iter and flows['flow_volume'].sum()>0.01*total_source_volume :
        print("Iteration n°{} of the radiation model".format(iteration))
        iteration+=1

        # Compute the radiation model with the rest of the demand and sink volume
        flows, source_volume, sink_volume = radiation_model(source_volume, sink_volume, costs)

        total_flows += flows['flow_volume']

        rest_source.append(source_volume.sum())
        rest_sink.append(sink_volume.sum())

    if iteration==max_iter:
        print("The iterations of the radiation model didn't converge")
    if plot:
        plt.figure(figsize=(18,5))
        plt.subplot(121)
        plt.plot(np.arange(1, iteration+1), rest_source)
        plt.xticks(np.arange(1, iteration+1))
        plt.xlabel("n° itérations")
        plt.ylabel("Demand volume not fulfilled")

        print("Total demand volume : {}".format(sources['source_volume'].sum()))
        print("Rest of demand volume after {} iterations : {}".format(
            iteration, rest_source[-1]))

        plt.subplot(122)
        plt.plot(np.arange(1, iteration+1), rest_sink)
        plt.xticks(np.arange(1, iteration+1))
        plt.xlabel("n° itérations")
        plt.ylabel("Opportunity volume not fulfilled")

        print("Total opportunity volume : {}".format(sinks['sink_volume'].sum()))
        print("Rest of opportunity volume after {} iterations : {}".format(
            iteration, rest_sink[-1]))

    return total_flows, source_volume, sink_volume