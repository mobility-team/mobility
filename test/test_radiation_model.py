import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import os
from pathlib import Path

import sys
sys.path.append("..")
from radiation_model import iter_radiation_model

#%% Import data for testing

data_folder_path = Path(os.path.dirname(__file__)).parent / "data"

sources = pd.read_csv(
    data_folder_path / "input/mobility/work_home/sources.csv",
    usecols=["location_id", "m_i"],
    dtype={"location_id": str},
)

sinks = pd.read_csv(
    data_folder_path / "input/mobility/work_home/destinations.csv",
    usecols=["location_id", "m_j"],
    dtype={"location_id": str}
)

costs = pd.read_csv(
    data_folder_path / "input/mobility/work_home/trips_average_unit_cost.csv",
    dtype={"from": str, "to": str, "cost_per_km": float}
)

sources.rename({"location_id": "transport_zone_id", "m_i": "source_volume"}, 
               axis=1, inplace=True)

sources = sources.groupby('transport_zone_id').first()

sinks.rename({"location_id": "transport_zone_id", "m_j": "sink_volume"}, 
             axis=1, inplace=True)
sinks = sinks.groupby('transport_zone_id').first()

costs.set_index(['from', 'to'], inplace=True)
costs.rename({"cost_per_km": "cost"}, axis=1, inplace=True)
distances = 10000*np.random.random(costs.shape[0]) # en metres
costs['cost'] = costs['cost'] * distances

#%% Garder seulement les communes du Bouces-du-Rhône + ajout d'un coût pour chaque OD si manquant

locations_id = ["{}".format(i) for i in range (13001, 13120)]
sources = sources.loc[locations_id]
sinks = sinks.loc[locations_id]
costs = costs.loc[locations_id]
costs = costs.loc[locations_id]
costs.reset_index(inplace=True)
costs.set_index(['from', 'to'], inplace=True)

cout_max = costs['cost'].max()
for loc_id_from in locations_id:
    for loc_id_to in locations_id:
        try :
            costs.loc[(loc_id_from, loc_id_to)]
        except KeyError :
            df = pd.DataFrame(data=[[loc_id_from, loc_id_to, cout_max*2]], columns=['from', 'to', 'cost'])
            df.set_index(['from', 'to'], inplace=True)
            costs = pd.concat([costs, df])

costs = costs.loc[locations_id].reset_index(level='from').loc[locations_id]
costs.reset_index(inplace=True)
costs.set_index(['from', 'to'], inplace=True)

#%% Compute and plot the flows between the different cities

#une_source = pd.DataFrame([sources.loc['13004']])
total_flows, source_volume, sink_volume = iter_radiation_model(sources, sinks, costs, max_iter=20, plot=True)

location_names = pd.read_excel(data_folder_path / 'input/mobility/work_home/table-appartenance-geo-communes-20.xlsx',
                               sheet_name=0, header=5,
                               usecols=['CODGEO', 'LIBGEO'])
location_names.set_index('CODGEO', inplace=True)

location_coordinates = pd.read_csv(data_folder_path / 'input/mobility/work_home/locations.csv')
location_coordinates['x'] = location_coordinates['x'] - location_coordinates['x'].min()
location_coordinates['x'] = location_coordinates['x'] / location_coordinates['x'].max()
location_coordinates['y'] = location_coordinates['y'] - location_coordinates['y'].min() 
location_coordinates['y'] = location_coordinates['y'] / location_coordinates['y'].max()
location_coordinates.set_index('location_id', inplace=True)

plot_flows = total_flows.reset_index()
plot_flows = pd.merge(plot_flows, location_coordinates, left_on='from', right_index=True)
plot_flows.rename({'x': 'from_x', 'y': 'from_y'}, axis=1, inplace=True)
plot_flows = pd.merge(plot_flows, location_coordinates, left_on='to', right_index=True)
plot_flows.rename({'x': 'to_x', 'y': 'to_y'}, axis=1, inplace=True)
plot_flows.sort_values(by='flow_volume', ascending=False, inplace=True)
idx_show = plot_flows.iloc[:100].index

plt.figure()
for idx in idx_show:
    plt.plot([plot_flows.loc[idx, 'from_x'], plot_flows.loc[idx, 'to_x']],
             [plot_flows.loc[idx, 'from_y'], plot_flows.loc[idx, 'to_y']],
             linewidth=plot_flows.loc[idx, 'flow_volume']/10,
             color='lightblue')

# Plot the locations (the more active people, the bigger)
temp = sources.join(location_coordinates)
plt.scatter(temp['x'], temp['y'],
            s=temp['source_volume'])
# N_big biggest location to show
N_big = 5
sources.sort_values(by='source_volume', inplace=True)
idx_show = sources.iloc[-N_big:].index

location_coordinates.sort_index(inplace=True)
sources.sort_index(inplace=True)
for idx in idx_show:
    plt.text(location_coordinates.loc[idx, 'x'], location_coordinates.loc[idx, 'y'],
             location_names.loc[idx, 'LIBGEO'])

#%% Attribuer une OD à chaque déplacement, juste à partir du domicile

data = [['13004', '9.91'], ['13004', '2.20'], ['13004', '2.21'], ['13004', '1.1'], 
        ['13001', '9.91'], ['13001', '1.1'],
        ['13004', '2.21'], ['13004', '9.91'], ['13004', '1.1']]
all_trips = pd.DataFrame(data=data, columns=['location_id', 'motive'])

locations_id = all_trips['location_id'].unique()

# Keep only the homes of the population for the sources
sources_ter = sources.loc[locations_id]
# Keep only the destinations at less than XX km from the sources
temp = ["{}".format(i) for i in range (13001, 13120)]
temp.remove('13055')
sinks_ter = sinks.loc[temp]

# Keep only the relevant OD and add a cost to those which don't have one
costs_ter = costs.loc[locations_id]

cout_max = costs_ter['cost'].max()
for loc_id_from in locations_id:
    for loc_id_to in temp:
        try :
            costs_ter.loc[(loc_id_from, loc_id_to)]
        except KeyError :
            df = pd.DataFrame(data=[[loc_id_from, loc_id_to, cout_max*2]], columns=['from', 'to', 'cost'])
            df.set_index(['from', 'to'], inplace=True)
            costs_ter = pd.concat([costs_ter, df])
costs_ter = costs_ter.loc[locations_id].reset_index(level='from').loc[temp]
costs_ter.reset_index(inplace=True)
costs_ter.set_index(['from', 'to'], inplace=True)

total_flows, source_volume, sink_volume = iter_radiation_model(sources_ter, sinks_ter, costs_ter, max_iter=20, plot=True)
total_flows = pd.DataFrame(total_flows)
#%% Methode 1 : deplacement par deplacement
all_trips['destination_id'] = np.nan

work_index = all_trips.loc[all_trips['motive']=='9.91'].index
for idx in work_index:
    all_trips.loc[idx, 'destination_id'] = total_flows.xs(all_trips.loc[idx, 'location_id']).sample(1, weights='flow_volume').index[0]
    
#%% Methode 2 : en groupant par domicile
all_trips['destination_id'] = np.nan
all_trips.reset_index(inplace=True)
all_trips.set_index(['motive', 'location_id'], inplace=True)

for loc_id in locations_id:
    print(all_trips.loc['9.91'].loc[loc_id, 'destination_id'])
    all_trips.loc['9.91'].loc[loc_id, 'destination_id'] = 1
    
