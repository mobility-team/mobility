# Public Objects

This page lists the main public objects that a project modeller is expected to use directly.

It lists the objects used in examples, the main arguments to look for, and the pages that explain the modelling choice behind them. Internal assets and helper modules can change more often.

## Setup

### `mobility.set_params(...)`

Use this once at the start of a script or notebook.

Main arguments:

- `package_data_folder_path`: shared folder for downloaded and prepared datasets,
- `project_data_folder_path`: project folder for scenario inputs, cache files, and run outputs,
- `inject_into_ssl`: set to `True` when downloads fail because Python does not use your system certificate store,
- `r_packages_download_method`: use `"wininet"` on some Windows proxy setups,
- `feedback`: use `"progress"`, `"logs"`, or `"debug"` to control run feedback.

See [installation](installation.md) for folder setup and common installation problems.

## Study Area

### `mobility.TransportZones(...)`

Use this for most project scripts. It creates the spatial units used by population, activities, routing, and results.

Common patterns:

```python
transport_zones = mobility.TransportZones("fr-87085", radius=10.0)
```

```python
transport_zones = mobility.TransportZones(
    ["fr-74010", "fr-74133"],
    level_of_detail=1,
)
```

Main arguments:

- `local_admin_unit_id` or a list of ids: the administrative units that define the study area,
- `radius`: a distance around one local administrative unit for a small first area,
- `level_of_detail`: `0` for coarser zones, `1` for more detailed zones,
- `inner_local_admin_unit_id` or `inner_radius`: smaller reporting area inside a larger routing area,
- `cutout_geometries`: areas to remove before zones are created.

See [study area and transport zones](transport_zones.md).

### `mobility.LocalAdminUnits(...)`

Use this only when you need to inspect the administrative-unit input table directly.

### `mobility.spatial.FrenchAdminUnits(...)`

Use this lower-level object when you need French administrative boundaries by level. Supported levels are `"country"`, `"region"`, `"departement"`, `"epci"`, and `"commune"`.

It returns `admin_level`, `admin_id`, `admin_name`, `country`, `geometry`, and longitude-latitude bbox columns.

### `mobility.spatial.SwissAdminUnits(...)`

Use this lower-level object when you need Swiss administrative boundaries. Supported levels are `"country"` and `"municipality"`.

It returns the same normalized columns as `FrenchAdminUnits`.

### `mobility.StudyArea(...)`

Use this when you need a lower-level study-area object before transport zones are built. Most user scripts can start directly with `TransportZones`.

## Population And Surveys

### `mobility.Population(...)`

Use this to create a synthetic resident population for the study area.

Common pattern:

```python
population = mobility.Population(transport_zones, sample_size=1000)
```

Main arguments:

- `transport_zones`: the zones where the population is located,
- `sample_size`: number of sampled people used by the simulation.

Result metrics use represented-person weights, so a `sample_size` of 1000 does not mean the final trip count is 1000 trips. See [population and surveys](population.md).

### `mobility.EMPMobilitySurvey(...)`

Use this for French survey-based behaviour patterns from EMP 2018-2019.

Project-specific survey objects can also be passed to `PopulationGroupDayTrips`, but the parser and documentation should live in the project repository.

## Activities

Activities describe why people travel and where destination opportunities exist.

Common first setup:

```python
activities = [
    mobility.HomeActivity(),
    mobility.WorkActivity(),
    mobility.OtherActivity(population=population),
]
```

Main activity objects:

- `mobility.HomeActivity`
- `mobility.WorkActivity`
- `mobility.StudyActivity`
- `mobility.ShopActivity`
- `mobility.LeisureActivity`
- `mobility.OtherActivity`

Parameter objects:

- `mobility.HomeParameters`
- `mobility.WorkParameters`
- `mobility.StudyParameters`
- `mobility.ShopParameters`
- `mobility.LeisureParameters`
- `mobility.OtherParameters`
- `mobility.ActivityParameters`

Common arguments and assumptions:

- `value_of_time`: value attached to activity time,
- `radiation_lambda`: destination-choice dispersion,
- `saturation_fun_ref_level`: occupation level used as a saturation reference,
- `saturation_fun_beta`: strength of the saturation response,
- `opportunities`: project-specific opportunity table, when the default proxy is too coarse.

Use `OtherActivity(population=population)` for a first model. For a project where "other" trips matter, document why residents are an acceptable opportunity proxy or replace it. See [activities](activities.md).

## Modes

Modes describe the transport options available to the population.

Common first setup:

```python
walk = mobility.WalkMode(transport_zones)
bicycle = mobility.BicycleMode(transport_zones)
car = mobility.CarMode(transport_zones)
```

Main mode objects:

- `mobility.WalkMode`
- `mobility.BicycleMode`
- `mobility.CarMode`
- `mobility.CarpoolMode`
- `mobility.PublicTransportMode`
- `mobility.IntermodalTransfer`
- `mobility.ModeRegistry`

Common arguments and assumptions:

- `transport_zones`: the zones used to compute costs,
- `generalized_cost_parameters`: time, distance, and fixed-cost assumptions,
- `routing_parameters`: routing assumptions for the mode,
- `congestion`: car-mode option that enables congestion feedback,
- `congestion_flows_scaling_factor`: scaling applied to modelled flows before congestion feedback.

Use car, walk, and bicycle first. Add public transport, carpool, congestion, or a custom registry only when the study question needs them. See [transport modes](modes.md).

## Costs And Routing Parameters

### `mobility.GeneralizedCostParameters(...)`

Use this to define the impedance used to compare modes and destinations.

Main arguments:

- `cost_constant`: fixed cost attached to the mode,
- `cost_of_distance`: distance cost, usually per kilometre,
- `cost_of_time`: a `CostOfTimeParameters` object.

### `mobility.CostOfTimeParameters(...)`

Use this to describe how travel time is valued.

The simplest setup uses an intercept:

```python
mobility.CostOfTimeParameters(intercept=5.0)
```

### Other cost and routing objects

- `mobility.PathTravelCosts`
- `mobility.PathGraph`
- `mobility.PathRoutingParameters`
- `mobility.PublicTransportRoutingParameters`
- `mobility.WalkParameters`
- `mobility.BicycleParameters`
- `mobility.CarParameters`
- `mobility.CarpoolParameters`
- `mobility.DetailedCarpoolRoutingParameters`
- `mobility.DetailedCarpoolGeneralizedCostParameters`
- `mobility.PublicTransportParameters`

Use these objects when the default mode setup is too coarse for the study. Keep changed values in the parameter report.

## Public-Transport Scenario Helpers

Use these objects to build a small GTFS feed in Python, for example for a test bus line or a provisional scenario line:

- `mobility.GTFSBuilder`
- `mobility.GTFSFeedSpec`
- `mobility.GTFSLineSpec`
- `mobility.GTFSStopSpec`
- `mobility.build_project_gtfs_zip`
- `mobility.build_gtfs_zip`

See the public-transport section of [transport modes](modes.md).

## Scenarios

### `mobility.Scenario(...)` and `mobility.Scenarios(...)`

Use these to name the situations you compare.

Common pattern:

```python
scenarios = mobility.Scenarios(
    [
        mobility.Scenario(name="default", title="Reference"),
        mobility.Scenario(
            name="project",
            title="Project",
            reference="default",
        ),
    ]
)
```

Main `Scenario` fields:

- `name`: short stable identifier used in folders and tables,
- `title`: readable title for plots and reports,
- `description`: short explanation of the modelling assumption,
- `reference`: metadata pointing to the reference scenario.

### `mobility.ParameterValue`

Use this when one parameter changes by scenario or by iteration:

```python
mobility.ParameterValue.by_scenario_and_iteration(
    default=0.0,
    project={1: 0.0, 5: 0.20},
)
```

See [scenarios](scenarios.md).

## Network Modifiers

Network modifiers describe project-specific changes to road-network assumptions:

- `mobility.BorderCrossingSpeedModifier`
- `mobility.LimitedSpeedZonesModifier`
- `mobility.NewRoadModifier`
- `mobility.RoadLaneNumberModifier`

Use them only when the scenario changes the road network or speed assumptions. Document the geometry, speed, capacity, or lane-number assumption with the scenario.

## Group-Day-Trip Model

### `mobility.PopulationGroupDayTrips(...)`

This is the main workflow object for daily mobility modelling.

Common pattern:

```python
population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=[walk, bicycle, car],
    activities=activities,
    surveys=[survey],
    scenarios=scenarios,
    parameters=parameters,
)
```

Main arguments:

- `population`: synthetic population,
- `modes`: list of available transport modes,
- `activities`: list of activities and opportunity assumptions,
- `surveys`: list of mobility surveys used for behaviour patterns,
- `scenarios`: declared scenarios, optional for a first default run,
- `parameters`: run and model parameters.

When you add a new country, keep the population, survey, local admin unit, opportunity, and GTFS inputs aligned on the same `country` code. The shared workflow only combines those prepared tables.

Use `population_trips.run(...)` to execute one concrete run. Use `population_trips.results(...)` to read indicators across scenarios or replications.

## Group-Day-Trip Parameters

Main parameter objects:

- `mobility.GroupDayTripsParameters`
- `mobility.GroupDayTripsRunParameters`
- `mobility.GroupDayTripsPeriodParameters`
- `mobility.GroupDayTripsOutputParameters`
- `mobility.GroupDayTripsActivitySequenceParameters`
- `mobility.GroupDayTripsBehaviorChangeParameters`
- `mobility.GroupDayTripsDestinationSequenceParameters`
- `mobility.GroupDayTripsModeSequenceParameters`
- `mobility.GroupDayTripsPlanUpdateParameters`
- `mobility.BehaviorChangePhase`
- `mobility.BehaviorChangeScope`

Common run arguments:

- `n_iterations`: number of model iterations,
- `n_replications`: number of replications,
- `seeds`: random seeds used by replications,
- `n_iter_per_cost_update`: how often transport costs are refreshed.

Common mode-sequence arguments:

- `k_mode_sequences`: number of candidate mode sequences,
- `mode_sequence_search_parallel`: enables parallel search,
- `use_rust_mode_sequence_search`: uses the faster compiled search when available.

See [run parameters](run_parameters.md).

## Results

Run one scenario:

```python
weekday_run = population_trips.run("weekday")
```

Read indicators:

```python
results = population_trips.results(
    "weekday",
    scenarios=["default", "project"],
)
```

Main result entry points:

- `results.metrics`
- `results.diagnostics`
- `results.tables`

Common metrics:

- `trip_count`
- `travel_distance`
- `travel_time`
- `cost`
- `ghg_emissions`

Common dimensions:

- `by_variable="mode"`
- `by_variable="activity"`
- `by_variable="distance_bin"`
- `by_variable="time_bin"`
- `by_variable="csp"`
- `by_zone="home_zone"`
- `by_zone="origin_zone"`
- `by_zone="destination_zone"`

Common result options:

- `iterations="last"` for final indicators,
- `iterations="all"` for iteration diagnostics,
- `output="table"` for explicit values,
- `output="plot"` for a visual check,
- `reference=("scenario", "default")` for scenario gaps,
- `reference="external"` for supported survey-comparable checks.

See [results and indicators](results.md).

## Older Or More Advanced Workflows

### `mobility.IndividualYearTrips`

`IndividualYearTrips` still exists, but the main documented workflow is now `PopulationGroupDayTrips`.
