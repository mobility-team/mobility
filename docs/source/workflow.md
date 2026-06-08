# Main Modelling Workflow

The main Mobility workflow is built around `PopulationGroupDayTrips`.

In a script or notebook, you first describe the territory and the people living there. Then you describe what they can do, how they can travel, and which scenarios you want to compare. Mobility then simulates daily plans and gives you indicators to inspect.

Each step can be limited for a first run, then made more detailed for a project study. In project work, keep the usual sequence visible: build a base case, document the inputs and assumptions, check the model against available evidence, run scenarios, then interpret differences with diagnostics and sensitivity tests. The [model checks page](model_checks.md) gives a more concrete checklist.

## 1. Configure Mobility

```python
import mobility

mobility.set_params(
    package_data_folder_path="path/to/shared-data",
    project_data_folder_path="path/to/project-data",
)
```

This tells Mobility where to store shared datasets and project cache files.

## 2. Define The Study Area

```python
transport_zones = mobility.TransportZones(
    ["fr-74010", "fr-74133"],
    level_of_detail=1,
)
```

Transport zones are the spatial units used for population, opportunities, routing, and results.

## 3. Build Surveys And Population

```python
survey = mobility.EMPMobilitySurvey()
population = mobility.Population(transport_zones, sample_size=1000)
```

The survey describes observed travel behaviour. The population describes the people living in the study area.

## 4. Configure Modes

```python
walk = mobility.WalkMode(transport_zones)
bicycle = mobility.BicycleMode(transport_zones)
car = mobility.CarMode(transport_zones)
```

Modes describe the transport options available to the model.

## 5. Configure Activities

```python
activities = [
    mobility.HomeActivity(),
    mobility.WorkActivity(),
    mobility.OtherActivity(population=population),
]
```

Activities describe the reasons why people travel and the opportunities available at destinations.

## 6. Configure Scenarios

```python
scenarios = mobility.Scenarios(
    [
        mobility.Scenario(name="default", title="Reference"),
    ]
)
```

Start with the default reference scenario. Add project scenarios once you have at least one parameter or input that changes. The scenario page shows how to compare several scenarios.

## 7. Create The Group-Day-Trip Model

```python
population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=[walk, bicycle, car],
    activities=activities,
    surveys=[survey],
    scenarios=scenarios,
    parameters=mobility.GroupDayTripsParameters(
        run=mobility.GroupDayTripsRunParameters(n_iterations=5),
    ),
)
```

This object is the main entry point for daily mobility modelling.

## 8. Run The Model

```python
weekday_run = population_trips.run(day_type="weekday")
```

For a first technical check, one iteration is enough. For a project result, use more iterations and check convergence diagnostics and indicator stability before interpreting scenario effects.

## 9. Read Results

```python
results = population_trips.results(
    "weekday",
)

trip_count = results.metrics.trip_count(
    by_variable="mode",
    iterations="last",
    output="table",
)
```

Start with tables so units, totals, and grouping columns are explicit. Then use plots and maps to inspect spatial patterns, modal patterns, and scenario gaps.

## From Quickstart To Project Work

The quickstart is intentionally small. A project setup usually changes several things:

- a larger study area and possibly an inner reporting perimeter,
- a larger population sample,
- more iterations and sometimes several replications,
- a richer activity set,
- public transport, congestion, carpool, or project-specific mode assumptions,
- explicit scenarios and parameter reports,
- diagnostics checked before scenario conclusions are written.

Make these changes gradually. A useful project habit is to keep one script that builds the model, one place where assumptions are named, and one results section that starts with the same checks every time: totals, mode shares, distances, main OD patterns, diagnostics, and scenario gaps.
