# Quickstart - First local run

This page shows the main first run for Mobility.

The goal is to:

* define a small study area,
* build a synthetic population,
* simulate weekday trips for that population,
* inspect a first set of outputs.

The code on this page follows the maintained example script in ``examples/quickstart-fr.py``.

## Before you start

Install Mobility first by following the installation page.

This quickstart assumes that your Mobility data folders are already configured. The example script reads them from the environment variables:

* ``MOBILITY_PACKAGE_DATA_FOLDER``
* ``MOBILITY_PROJECT_DATA_FOLDER``

If you prefer, you can also call ``mobility.set_params()`` with explicit paths.

## What this example does

The example uses:

* Foix, France, with a 10 km radius, for a small and relatively fast run,
* the French EMP mobility survey,
* a synthetic population of 1000 people,
* three transport modes: car, walk, and bicycle,
* three activities: home, work, and other.

It then computes weekday trip plans, global metrics, and a first origin-destination flow plot.

## Complete example

```python
import os
import dotenv
import mobility
from mobility.trips.group_day_trips import Parameters

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

# Using Foix (a small town) and a limited radius for quick results
transport_zones = mobility.TransportZones("fr-09122", radius=10.0)

# Using EMP, the latest national mobility survey for France
survey = mobility.EMPMobilitySurvey()

# Creating a synthetic population of 1000 for the area
population = mobility.Population(transport_zones, sample_size=1000)

# Simulating trips for this population for car, walk, bicycle
population_trips = mobility.GroupDayTrips(
    population=population,
    modes=[
        mobility.Car(transport_zones),
        mobility.Walk(transport_zones),
        mobility.Bicycle(transport_zones),
    ],
    activities=[
        mobility.Home(),
        mobility.Work(),
        mobility.Other(population=population),
    ],
    surveys=[survey],
    parameters=Parameters(
        n_iterations=1,
        mode_sequence_search_parallel=False,
    ),
)

# You can get weekday plan steps to inspect them
weekday_plan_steps = population_trips.get()["weekday_plan_steps"].collect()

# You can compute global metrics for weekday trips
global_metrics = population_trips.weekday_run.evaluate("global_metrics")

# You can plot weekday OD flows, with labels for prominent cities
weekday_results = population_trips.weekday_run.results()
labels = weekday_results.get_prominent_cities()
weekday_results.plot_od_flows(labels=labels)

# You can get a report of the parameters used in the model
report = population_trips.parameters_dataframe()
```

## Reading the example

### 1. Configure Mobility

``mobility.set_params(...)`` tells Mobility where to store shared datasets and project-specific datasets.

On a first run, this step can also trigger data preparation and R package setup depending on your environment.

### 2. Define the study area

```python
transport_zones = mobility.TransportZones("fr-09122", radius=10.0)
```

This creates the study area around Foix and builds the transport-zone input used by the rest of the workflow.

### 3. Build the population and model inputs

```python
survey = mobility.EMPMobilitySurvey()
population = mobility.Population(transport_zones, sample_size=1000)
```

The survey provides observed mobility behaviour patterns. The synthetic population provides the local population that will be simulated.

### 4. Simulate weekday trips

```python
population_trips = mobility.GroupDayTrips(
    population=population,
    modes=[
        mobility.Car(transport_zones),
        mobility.Walk(transport_zones),
        mobility.Bicycle(transport_zones),
    ],
    activities=[
        mobility.Home(),
        mobility.Work(),
        mobility.Other(population=population),
    ],
    surveys=[survey],
    parameters=Parameters(
        n_iterations=1,
        mode_sequence_search_parallel=False,
    ),
)
```

``GroupDayTrips`` is the main workflow object in this quickstart. It combines:

* a population,
* available transport modes,
* daily activities,
* survey data,
* model parameters.

### 5. Inspect the outputs

```python
weekday_plan_steps = population_trips.get()["weekday_plan_steps"].collect()
global_metrics = population_trips.weekday_run.evaluate("global_metrics")
weekday_results = population_trips.weekday_run.results()
report = population_trips.parameters_dataframe()
```

These outputs are useful for a first check:

* ``weekday_plan_steps`` gives access to simulated weekday trip-plan steps,
* ``global_metrics`` provides aggregated indicators,
* ``weekday_results`` gives access to result plots and summaries,
* ``report`` lists the parameters used for the run.

## What to expect

A successful run should give you:

* a dataframe-like table of weekday plan steps,
* aggregated weekday indicators,
* an origin-destination flow plot,
* a parameter report for traceability.

The first execution can take noticeably longer than later ones because Mobility may need to prepare local data and dependencies.

## Common first-run issues

### Missing environment variables

If ``MOBILITY_PACKAGE_DATA_FOLDER`` or ``MOBILITY_PROJECT_DATA_FOLDER`` are not defined, the example script will fail before the model starts.

In that case, either:

* define these variables in your environment or ``.env`` file,
* or replace the ``set_params`` call with explicit folder paths.

### Long setup time

The first run can be slow because Mobility may download, prepare, or cache data, and may also install required R packages.

### Windows and R package installation

If R package installation fails behind a proxy, see the installation page for the Windows-specific workaround using ``r_packages_download_method="wininet"``.

## Next steps

After this first run, the next useful pages are:

* transport zones,
* transport modes,
* trips,
* carbon computation and scenario analysis.
