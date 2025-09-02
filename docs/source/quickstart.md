# Quickstart - Local home-work travel behavior

The goal of Mobility is to make it easy to get detailed estimated travel behavior data for a sample population, living in a given region. The main final output is a table of trips taken over a given period by each individual in the population, each trip having a motive, an origin, a destination, a mode or a sequence of modes used, and travel distance and time. Intermediate outputs may also be used to compute aggregated metrics over the whole region or a subset of cities, over a given motive, over an origin - destination pair...

As an example, this page shows you how to :
- Estimate the travel times and modal shares for home to work place trips, by transport zone, in the region of Dijon, France.
- Get a table of trips made by a sample population, in which most trips only depend on individual characteristics but home to work place trips also take the local context into account : spatial distribution of active persons and jobs, and transport infrastructure (only car and bicycle in this example).

We'll go through the example step by step, but you can also get complete script directly here.

## Set up

Once Mobility is installed, as with all python libraries, we can `import mobility` to access its functionnality. We have to call `mobility.set_params()` at the beginning of a script, to make sure all needed dependencies and environment variables are set up. The first call can take some time to run, but subsequent ones should be much faster.

```python
import mobility
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

mobility.set_params()
```

## Transport zones

We first need to define the region we want to study, as well as its spatial subdivisions, that we call transport zones. Mobility uses administrative boundaries at city level as the first level of spatial subdivision, with the possibility to further subdivide them in high density areas, based on building density (by switching the level of detail to 1).

We can define the region by providing an explicit list of city ids, like ["fr-21231", "fr-21355"] (for the cities of Dijon and Longvic, in the "country_code-city_code" format), or select all cities within a circle centered on a given city (with a radius in km).

```python
transport_zones = mobility.TransportZones(
    local_admin_unit_id="fr-21231",
    radius=40,
    level_of_detail=0
)
```

Mobility tries to compute things only when and if they are needed, so at this point our transport_zones object did not create anything. We need to explicitly access its content to first create and then retrieve the pandas.GeoDataFrame, so we can plot or inspect it for example.

```python
tz_gdf = transport_zones.get()
tz_gdf.plot()
tz_gdf.head()
```

## Transport modes

We then need to define the available transport modes for our local population, and their perception : cost of distance (€/km), cost of time (€/h), cost constant (€/trip). For this example we can use only two modes, only differentiate the cost of distance, and use Mobility's default values for the other generalized costs parameters.

```python
car = mobility.CarMode(
    transport_zones=transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=0.1
    )
)

bicycle = mobility.BicycleMode(
    transport_zones=transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=0.0
    )
)

modes = [
    car,
    bicycle
]
```

## Work destination and mode choice models

We can then create two choice models that will estimate the probability of choosing a work place given the transport zone in which an active person lives, and the probability of using a car or a bicycle to get from one transport zone to another. As we saw earlier, don't forget to access the model contents by calling `get`,  to create and retrieve them.


```python
work_choice_model = mobility.WorkDestinationChoiceModel(
    transport_zones,
    modes=modes
)

mode_choice_model = mobility.TransportModeChoiceModel(
    destination_choice_model=work_choice_model
)

work_choice_model.get()
mode_df = mode_choice_model.get()
```

## Comparing to reference data

We can see how well the model matches the home - work place pair counts estimated by INSEE and OFS, by comparing them directly on a log-log plot (so we don't only see big counts) or by computing their Sorensen Similarity Index (SSI) at different minimum count thresholds (the SSI goes from 0 to 1, 1 being a perfect fit).


```python
comparison = work_choice_model.get_comparison()

work_choice_model.plot_model_fit(comparison)

work_choice_model.compute_ssi(comparison, 200)
work_choice_model.compute_ssi(comparison, 400)
work_choice_model.compute_ssi(comparison, 1000)
```

This toy model does OK but does not fit very well, so we could try to tweak the different parameters to improve the fit ! Keep in mind that reference data are also estimates, and that small counts (< 200) are likely to be very uncertain.

## Extracting metrics (to improve)

We can also use data generated by the choice models to compute and then plot maps of different metrics of interest, for example the average time to get to work, from each transport zone.

```python
car_travel_costs = car.travel_costs.get()
car_travel_costs["mode"] = "car"

bicycle_travel_costs = bicycle.travel_costs.get()
bicycle_travel_costs["mode"] = "bicycle"

travel_costs = pd.concat([
    car_travel_costs,
    bicycle_travel_costs]
)


ids = transport_zones.get()[["local_admin_unit_id", "transport_zone_id"]]
ori_dest_counts = pd.merge(comparison, ids, left_on="local_admin_unit_id_from", right_on="local_admin_unit_id")
ori_dest_counts = pd.merge(ori_dest_counts, ids, left_on="local_admin_unit_id_to", right_on="local_admin_unit_id")
ori_dest_counts = ori_dest_counts[["transport_zone_id_x", "transport_zone_id_y", "flow_volume"]]
ori_dest_counts = ori_dest_counts.rename(columns={"transport_zone_id_x": "from", "transport_zone_id_y":"to"})
modal_shares = pd.merge(mode_df, ori_dest_counts, on=["from", "to"])
modal_shares["flow_volume"] = modal_shares["flow_volume"] * modal_shares["prob"]

travel_time_by_ori = pd.merge(modal_shares, travel_costs, on=["from", "to", "mode"])
travel_time_by_ori["tot_time"] = travel_time_by_ori["time"]*travel_time_by_ori["flow_volume"]
travel_time_by_ori = travel_time_by_ori.groupby("from")["tot_time"].sum()/travel_time_by_ori.groupby("from")["flow_volume"].sum()
travel_time_by_ori.name = "average_travel_time"
travel_time_by_ori = pd.merge(transport_zones.get(), travel_time_by_ori.reset_index(), left_on="transport_zone_id", right_on="from")
travel_time_by_ori = gpd.GeoDataFrame(travel_time_by_ori)
travel_time_by_ori.plot(column="average_travel_time", legend=True)
plt.show()

```

## Sample trips (to do)

One last step is needed to get a table of trips made by a sample population. We need to initialize this population by sampling individuals in each transport zone, sample activity programmes and the related trips for each individual, and finally contextualize some of the trips given the work place and mode choice models we have.


## Complete script
```python
# -----------------------------------------------------------------------------
# Set up

import mobility
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

mobility.set_params()

# -----------------------------------------------------------------------------
# Transport modes

transport_zones = mobility.TransportZones(
    local_admin_unit_id="fr-21231",
    radius=40,
    level_of_detail=0
)

# -----------------------------------------------------------------------------
# Transport modes

car = mobility.CarMode(
    transport_zones=transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=0.1
    )
)

bicycle = mobility.BicycleMode(
    transport_zones=transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_of_distance=0.0
    )
)

modes = [
    car,
    bicycle
]

# -----------------------------------------------------------------------------
# Work destination and mode choice models

work_choice_model = mobility.WorkDestinationChoiceModel(
    transport_zones,
    modes=modes
)

mode_choice_model = mobility.TransportModeChoiceModel(
    destination_choice_model=work_choice_model
)

work_choice_model.get()
mode_df = mode_choice_model.get()

# -----------------------------------------------------------------------------
# Comparing to reference data

comparison = work_choice_model.get_comparison()

work_choice_model.plot_model_fit(comparison)

work_choice_model.compute_ssi(comparison, 200)
work_choice_model.compute_ssi(comparison, 400)
work_choice_model.compute_ssi(comparison, 1000)

# -----------------------------------------------------------------------------
# Extracting metrics

# Average travel time by origin
car_travel_costs = car.travel_costs.get()
car_travel_costs["mode"] = "car"

bicycle_travel_costs = bicycle.travel_costs.get()
bicycle_travel_costs["mode"] = "bicycle"

travel_costs = pd.concat([
    car_travel_costs,
    bicycle_travel_costs]
)


ids = transport_zones.get()[["local_admin_unit_id", "transport_zone_id"]]
ori_dest_counts = pd.merge(comparison, ids, left_on="local_admin_unit_id_from", right_on="local_admin_unit_id")
ori_dest_counts = pd.merge(ori_dest_counts, ids, left_on="local_admin_unit_id_to", right_on="local_admin_unit_id")
ori_dest_counts = ori_dest_counts[["transport_zone_id_x", "transport_zone_id_y", "flow_volume"]]
ori_dest_counts = ori_dest_counts.rename(columns={"transport_zone_id_x": "from", "transport_zone_id_y":"to"})
modal_shares = pd.merge(mode_df, ori_dest_counts, on=["from", "to"])
modal_shares["flow_volume"] = modal_shares["flow_volume"] * modal_shares["prob"]

travel_time_by_ori = pd.merge(modal_shares, travel_costs, on=["from", "to", "mode"])
travel_time_by_ori["tot_time"] = travel_time_by_ori["time"]*travel_time_by_ori["flow_volume"]
travel_time_by_ori = travel_time_by_ori.groupby("from")["tot_time"].sum()/travel_time_by_ori.groupby("from")["flow_volume"].sum()
travel_time_by_ori.name = "average_travel_time"
travel_time_by_ori = pd.merge(transport_zones.get(), travel_time_by_ori.reset_index(), left_on="transport_zone_id", right_on="from")
travel_time_by_ori = gpd.GeoDataFrame(travel_time_by_ori)
travel_time_by_ori.plot(column="average_travel_time", legend=True)
plt.show()

```
