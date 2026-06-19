# Quickstart

This quickstart runs a small weekday model around Limoges, France.

This page gives you a first complete run. Once this works on your computer, you can reuse the same structure for a larger study area and richer scenarios.

It follows the maintained example script in `examples/quickstart-fr.py`.

The goal is to:

- define a small study area,
- build a synthetic population,
- simulate weekday trips,
- read a first set of outputs.

## Before You Start

Install Mobility first. The recommended path uses Pixi. The mamba path is still supported for now.

On a first run, Mobility may download data, prepare local files, and install R packages. This can take a while. Later runs reuse the local cache.

This example deliberately stays small:

- Limoges keeps the study area compact and reuses a small OSM extract,
- 1000 people keeps runtime manageable for the tutorial,
- car, walk, and bicycle keep the first mode set limited,
- home, work, and other keep the first activity set limited,
- one iteration keeps the run short during the first workflow check.

## Complete Example

```python
import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params()

# Use Limoges and a limited radius to reuse the smaller Limousin OSM extract.
transport_zones = mobility.TransportZones("fr-87085", radius=10.0)

# Use EMP 2018-2019, the current Mobility survey source for French examples.
survey = mobility.EMPMobilitySurvey()

# Create a synthetic population of 1000 people for the area.
population = mobility.Population(transport_zones, sample_size=1000)

# Simulate trips for this population with car, walk, and bicycle.
population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=[
        mobility.CarMode(transport_zones),
        mobility.WalkMode(transport_zones),
        mobility.BicycleMode(transport_zones),
    ],
    activities=[
        mobility.HomeActivity(),
        mobility.WorkActivity(),
        mobility.OtherActivity(population=population),
    ],
    surveys=[survey],
    parameters=mobility.GroupDayTripsParameters(
        run=mobility.GroupDayTripsRunParameters(n_iterations=1),
        mode_sequences=mobility.GroupDayTripsModeSequenceParameters(
            mode_sequence_search_parallel=False,
        ),
    ),
)

# Run the weekday model.
weekday_run = population_trips.run("weekday")
weekday_plan_steps = weekday_run.get()["plan_steps"].collect()

# Use population_trips.results(...) as the main entry point for indicators.
weekday_results = population_trips.results("weekday")
trip_count_by_mode = weekday_results.metrics.trip_count(
    by_variable="mode",
    iterations="last",
    output="table",
)

# Plot origin-destination flows between transport zones.
od_flow_plot = weekday_results.metrics.trip_count(
    by_zone=["origin_zone", "destination_zone"],
    iterations="last",
    output="plot",
)

# Get a report of the parameters used by the model.
parameters_report = weekday_run.parameters_dataframe()
```

## What The Example Does

### Configure Mobility

```python
dotenv.load_dotenv()
mobility.set_params()
```

This configures data folders and R packages.

If you pass explicit folders to `set_params`, Mobility uses them. If you do not pass folders, Mobility uses its default folders and may ask before creating them.

### Define The Study Area

```python
transport_zones = mobility.TransportZones("fr-87085", radius=10.0)
```

This creates transport zones around Limoges. Transport zones are the spatial units used by the rest of the workflow.

### Build The Population

```python
survey = mobility.EMPMobilitySurvey()
population = mobility.Population(transport_zones, sample_size=1000)
```

The survey gives observed mobility behaviour. The synthetic population gives the local people who will be simulated.

### Describe Modes And Activities

```python
modes=[
    mobility.CarMode(transport_zones),
    mobility.WalkMode(transport_zones),
    mobility.BicycleMode(transport_zones),
]
```

The modes describe the transport options available to the population.

```python
activities=[
    mobility.HomeActivity(),
    mobility.WorkActivity(),
    mobility.OtherActivity(population=population),
]
```

The activities describe the reasons why people make trips.

### Run Weekday Trips

```python
weekday_run = population_trips.run("weekday")
```

`PopulationGroupDayTrips` is the main workflow object. It brings together the population, modes, activities, surveys, scenarios, and run parameters.

### Read Outputs

```python
weekday_plan_steps = weekday_run.get()["plan_steps"].collect()
weekday_results = population_trips.results("weekday")
trip_count_by_mode = weekday_results.metrics.trip_count(
    by_variable="mode",
    iterations="last",
    output="table",
)
parameters_report = weekday_run.parameters_dataframe()
```

These outputs are useful for a first check:

- `weekday_plan_steps` contains the simulated plan steps,
- `weekday_results` contains indicators, diagnostics, and tables for the weekday model,
- `trip_count_by_mode` contains one first indicator table,
- `parameters_report` lists the parameters used by the run.

This page keeps `weekday_run` only to inspect raw plan steps and the parameter report. For indicators, diagnostics, and scenario comparison, use `population_trips.results(...)`.

## What To Expect

A successful quickstart gives you:

- a `weekday_plan_steps` table with simulated plan steps,
- a `weekday_results` object for indicators, diagnostics, and tables,
- a `trip_count_by_mode` table with the number of trips by mode,
- an `od_flow_plot` object showing origin-destination flows,
- a `parameters_report` table for traceability.

The exact numbers depend on the data and parameters used on your machine. For a first check, focus on whether the workflow runs, the result tables contain rows, and the main indicators have a plausible order of magnitude.

The `trip_count_by_mode` table is a compact first check. Metric tables usually contain columns such as `scenario`, `day_type`, `iteration`, a grouping column like `mode`, and a value column such as `trip_count`, `travel_distance`, or `ghg_emissions`.

One documentation run of this quickstart, using explicit data folders and 1000 sampled people, produced:

```text
weekday_plan_steps shape: 1458 rows x 23 columns
parameters_report shape: 276 rows x 14 columns
```

The same run produced this `trip_count_by_mode` table:

```text
scenario  day_type  iteration  mode     trip_count  trip_count_std  n_replications
default   weekday   1          bicycle  1695.16     null            1
default   weekday   1          car      217089.35   null            1
```

Use these numbers as an example of the table shape and scale. They can change when input data, package defaults, random seeds, or the local cache change. For your first check, the table should contain rows, the columns should be understandable, and the represented trip counts should be plausible for the represented population.

If a download fails with a certificate error on a company network, see the certificate notes in the installation page.

## You Are Done When

You are done with the quickstart when:

- the script runs without an exception,
- `weekday_plan_steps` contains rows,
- `trip_count_by_mode` contains final weekday indicators,
- `od_flow_plot` is produced by your environment,
- you can rerun the script faster because cached data is reused.

The first run can be slower than later runs because data is downloaded and prepared. If a later run is still unexpectedly slow, check whether the project data folder is being reused.

## Change One Thing

Once the quickstart works, make one small change and rerun it.

For example, change:

```python
population = mobility.Population(transport_zones, sample_size=1000)
```

to:

```python
population = mobility.Population(transport_zones, sample_size=2000)
```

Then compare `trip_count_by_mode`. This is a small sensitivity exercise: it shows how sampling variability and runtime change before you add scenario assumptions, public transport, or a more detailed activity set.

## Next Steps

After this first run, read:

- the main modelling workflow,
- model checks for a study,
- study area and transport zones,
- transport modes, including how to configure public transport GTFS sources,
- scenarios,
- results and indicators.
