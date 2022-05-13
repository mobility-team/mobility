import sys
sys.path.append("..")

from mobility import ShopsMobilityModel

codgeo = "69191"

mob = ShopsMobilityModel(codgeo)

# See the origins of the people coming to the city's shops
source_probabilities = mob.compute_source_probabilities(codgeo)
print(source_probabilities.sort_values().tail(5))

# See the destinations of the residents when they go shopping
sink_probabilities = mob.compute_sink_probabilities(codgeo)
print(sink_probabilities.sort_values().tail(5))

# Add shops capacity to the city
# Capacity = number of consumption units that shops can handle
# (1 m² of shops ~= 1.5 consumption unit)
# see data_preparation/shops_mobility_data.py
# and INSEE's definition of a consumption unit
mob.add_to_sink(codgeo, 10000)

# See how it changes where residents go shopping
sink_probabilities = mob.compute_sink_probabilities(codgeo)
print(sink_probabilities.sort_values().tail(5))


# Add consumption units to the city
mob = ShopsMobilityModel(codgeo)
mob.add_to_source(codgeo, 10000)

# See how it changes where residents go shopping
sink_probabilities = mob.compute_sink_probabilities(codgeo)
print(sink_probabilities.sort_values().tail(5))
