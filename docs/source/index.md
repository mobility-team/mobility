# Mobility Documentation

Mobility is an open-source Python package for people who model transport systems and local mobility.

The package is built for studies where you need to describe a territory, build a synthetic population, simulate daily trips, and compare what changes when the mobility system changes. It is mainly used today on French and Swiss territories, with open data, mobility surveys, Python, R, and routing tools.

The documentation separates two things: how to run the package, and how to make modelling choices that are defensible in a study. A quick run is useful for learning the workflow. A project model still needs the usual transport modelling discipline: a documented base case, traceable assumptions, calibration or reasonableness checks against available observations, diagnostics, and sensitivity tests when assumptions matter.

Mobility provides default parameters so a model can run before every local assumption is calibrated. These defaults are mixed: some are reasonable starting points, and some are not locally calibrated for a specific project territory.

If you are new to Mobility, start with the quickstart to see the workflow end to end. Then come back to the modelling pages when you want to adapt the base case, scenarios, data, and checks to your own territory.

## Choose Your Path

If you are using Mobility for the first time:

1. Install Mobility.
2. Run the Limoges quickstart.
3. Check the first indicators and the origin-destination plot.
4. Read the workflow and model-checks pages to understand how the objects fit together and what needs to be checked before scenario interpretation.

If you are adapting Mobility to a project territory, start with study area, population, activities, modes, scenarios, and results.

If you want to understand the model assumptions, read model steps, data sources, definitions, and the deeper sections of the modes page. These pages connect Mobility objects with standard concepts such as synthetic population, activity patterns, destination choice, mode choice, generalized cost, convergence diagnostics, and scenario comparison.

If you want to contribute, read the developer guide after you can run the quickstart locally.

After the first path, you should have a working mamba environment, a small weekday run around Limoges, and a first `population_trips.results("weekday")` object that you can use for indicators.

```{toctree}
:caption: Start
:maxdepth: 1

installation
quickstart
workflow
model_checks
```

```{toctree}
:caption: Modelling Guide
:maxdepth: 1

transport_zones
population
activities
add_country
current_countries
modes
scenarios
run_parameters
results
```

```{toctree}
:caption: Reference And Background
:maxdepth: 1

api_reference
data
model_steps
research_method
definitions
survey_codes
project_history
```

```{toctree}
:caption: Contributing
:maxdepth: 1

developer_guide
```
