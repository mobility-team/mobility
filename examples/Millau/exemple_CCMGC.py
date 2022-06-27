from get_insee_data import get_insee_data
import radiation_model as rm
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path

"""
This script uses the radiation model on the french city Millau and its surroundings
to compute home-work mobility and then compare the results with the INSEE data.

The territory considered in the radiation model is the french departments of
Aveyron, Lozère, Hérault, Gard, Tarn.
"""

# ===================
# IMPORT AND PROCESS THE DATA

# Import the data (active population and jobs)
insee_data = get_insee_data()
db_actifs = insee_data['active_population']
db_emplois = insee_data['jobs']

db_emplois['EMPLT'] = db_emplois[['C18_EMPLT_CS1', 'C18_EMPLT_CS2', 'C18_EMPLT_CS3',
                                  'C18_EMPLT_CS4', 'C18_EMPLT_CS5', 'C18_EMPLT_CS6']].sum(axis=1)
db_emplois.reset_index(inplace=True)

db_actifs['ACT'] = db_actifs[['C18_ACT1564_CS1', 'C18_ACT1564_CS2', 'C18_ACT1564_CS3',
                              'C18_ACT1564_CS4', 'C18_ACT1564_CS5', 'C18_ACT1564_CS6']].sum(axis=1)
db_actifs.reset_index(inplace=True)

# keep only the cities in Aveyron, Lozère, Hérault, Gard, Tarn
lst_departements = ['12', '48', '34', '30', '81']

sinks_millau = db_emplois.loc[:, ['CODGEO', 'EMPLT']]
sinks_millau['DEP'] = sinks_millau['CODGEO'].str.slice(0,2)
mask = sinks_millau['DEP'].apply(lambda x: x in lst_departements)
sinks_millau = sinks_millau.loc[mask]

sinks_millau = sinks_millau.set_index('CODGEO')
sinks_millau.rename(columns={'EMPLT': 'sink_volume'}, inplace=True)
sinks_millau = sinks_millau.drop(columns=['DEP'])

sources_millau = db_actifs.loc[:, ['CODGEO', 'ACT']]
sources_millau['DEP'] = sources_millau['CODGEO'].str.slice(0,2)
mask = sources_millau['DEP'].apply(lambda x: x in lst_departements)
sources_millau = sources_millau.loc[mask]

sources_millau = sources_millau.set_index('CODGEO')
sources_millau = sources_millau.drop(columns=['DEP'])
sources_millau.rename(columns={'ACT': 'source_volume'}, inplace=True)

data_folder_path = Path(os.path.dirname(__file__))

# Import the INSEE data on the work-home mobility on Millau
raw_flowDT = pd.read_excel(data_folder_path / "fluxDtMillau2017.xlsx",
            dtype={'COMMUNE': str, 'DCLT': str})

# Import the geographic data on the work-home mobility on Millau
coordonnees = pd.read_csv(data_folder_path / "coordonneesCommunesOccitanie.csv",
            sep=',', usecols=['NOM_COM', 'INSEE_COM', 'x', 'y'],
            dtype={'INSEE_COM': str})
coordonnees.set_index('INSEE_COM', inplace=True)

surfaces = pd.read_csv(data_folder_path / "surfacesCommunesOccitanie.csv",
            sep=',', usecols=['INSEE_COM', 'distance_interne'],
            dtype={'INSEE_COM': str})
surfaces.set_index('INSEE_COM', inplace=True)

# Compute the distance between cities
#    distance between i and j = (x_i - x_j)**2 + (y_i - y_j)**2
lst_communes = sources_millau.index.to_numpy()
idx_from_to = np.array(np.meshgrid(lst_communes, lst_communes)).T.reshape(-1, 2)
idx_from = idx_from_to[:, 0]
idx_to = idx_from_to[:, 1]
costs_millau = pd.DataFrame(
    {'from': idx_from, 'to': idx_to, 'cost': np.zeros(idx_to.shape[0])})

costs_millau = pd.merge(costs_millau, coordonnees, left_on='from', right_index=True)
costs_millau.rename(columns={'x': 'from_x', 'y': 'from_y'}, inplace=True)
costs_millau = pd.merge(costs_millau, coordonnees, left_on='to', right_index=True)
costs_millau.rename(columns={'x': 'to_x', 'y': 'to_y'}, inplace=True)

costs_millau = pd.merge(costs_millau, surfaces, left_on='from', right_index=True)

costs_millau['cost'] = np.sqrt((costs_millau['from_x']/1000-costs_millau['to_x']/1000)**2 + (costs_millau['from_y']/1000-costs_millau['to_y']/1000)**2)

# distance if the origin and the destination is the same city
# is internal distance = 128*r / 45*pi where r = sqrt(surface of the city)/pi
mask = costs_millau['from']!=costs_millau['to']
costs_millau['cost'].where(mask, other=costs_millau['distance_interne'], inplace=True)

#%% COMPUTE THE MODEL

total_flows, source_rest_volume, sink_rest_volume = rm.iter_radiation_model(
    sources_millau, sinks_millau, costs_millau, alpha=0, beta=1, plot=True)   

#%% PLOT THE SOURCES AND THE SINKS

plot_sources = sources_millau.rename(columns={'source_volume': 'volume'})
rm.plot_volume(plot_sources, coordonnees, n_locations=10, title="Volume d'actifs")

plot_sinks = sinks_millau.rename(columns={'sink_volume': 'volume'})
rm.plot_volume(plot_sinks, coordonnees, n_locations=10, title="Volume d'emplois")

#%% PLOT THE FLOWS COMPUTED BY THE MODEL

plot_flows = total_flows.reset_index()
plot_sources = sources_millau

rm.plot_flow(plot_flows, coordonnees, sources=plot_sources, n_flows=500, n_locations=20,
             size=10, title="Flux domicile-travail générés par le modèle")

#%% PLOT THE FLOWS FROM THE INSEE DATA

plot_flowDT = raw_flowDT.groupby(['COMMUNE', 'DCLT'])['IPONDI'].sum().reset_index()
plot_flowDT.rename(columns={'IPONDI': 'flow_volume', 'COMMUNE': 'from', 'DCLT': 'to'},
                   inplace=True)

rm.plot_flow(plot_flowDT, coordonnees, sources=plot_sources, n_flows=500, n_locations=20,
             size=10, title="Flux domicile-travail mesuré par l'INSEE")

#%% COMPARE THE MODEL WITH THE INSEE DATA

flowDT = raw_flowDT.rename(columns={'IPONDI': 'flow_volume', 'COMMUNE': 'from', 'DCLT': 'to'})
flowDT = flowDT.groupby(['from', 'to'])['flow_volume'].sum()
flowDT = pd.DataFrame(flowDT)

flowsRM = pd.DataFrame(total_flows)

# Join on the couple origin destinations
# how = 'inner' to keep only the couples that are in both dataframes
flow_join = flowDT.join(flowsRM, how='inner', lsuffix='DT', rsuffix='RM')
   
# Compare the total flow 
print('The 2 dataframes have {} OD in common\n'.format(flow_join.shape[0]))
print('Total flow of the INSEE data :\n   {:.0f}'.format(flow_join['flow_volumeDT'].sum()))
print('Total flow of the model :\n   {:.0f}\n'.format(flow_join['flow_volumeRM'].sum()))
    
# Compare the repartition between the ODs
flow_join['repartitionDT'] = flow_join['flow_volumeDT'] / flow_join['flow_volumeDT'].sum()
flow_join['repartitionRM'] = flow_join['flow_volumeRM'] / flow_join['flow_volumeRM'].sum()

error_repartition = np.abs(flow_join['repartitionDT'] - flow_join['repartitionRM'])
    
print("The repartitions from the INSEE data and the data have {:.2f}% in common.".format(100 - 50*error_repartition.sum()))
    
flow_join.reset_index(inplace=True)
plot_DT = pd.DataFrame(flow_join[['from', 'to', 'repartitionDT']])
plot_DT.rename(columns={'repartitionDT': 'flow_volume'}, inplace=True)
plot_RM = pd.DataFrame(flow_join[['from', 'to', 'repartitionRM']])
plot_RM.rename(columns={'repartitionRM': 'flow_volume'}, inplace=True)
    
rm.plot_flow(plot_DT, coordonnees, sources=plot_sources, n_flows=500, size=10, n_locations=20,
             title="Flux domicile-travail mesuré par l'INSEE")
rm.plot_flow(plot_RM, coordonnees, sources=plot_sources, n_flows=500, size=10, n_locations=20,
             title="Flux domicile-travail générés par le modèle")