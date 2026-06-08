# Model Checks For A Study

A completed Mobility run gives you model outputs. A study conclusion also needs a checked base case, documented assumptions, and evidence that the result is good enough for the question being asked.

Use this page as a checklist for moving from a technical run to a documented base case and then to scenario interpretation. The package can compute many of the tables, plots, and diagnostics, but the project team still has to decide which evidence is relevant and what tolerance is acceptable for the study.

## 1. Technical Run Checks

These checks show that the workflow is connected correctly:

- the model runs for the expected day type, scenario, iteration, and replication,
- plan-step tables are not empty,
- the population represented by `n_persons` matches the expected study population,
- main modes and activities appear where expected,
- no major zones are missing from maps or tables,
- the parameter report records the assumptions used for the run.

The quickstart stops at this level. It is enough for learning the package, but not for interpreting scenario effects.

## 2. Base-Case Reasonableness Checks

Before comparing scenarios, check whether the base case represents the territory well enough for the question being asked.

Start with survey-based checks, because Mobility already builds behaviour from survey sources and can compare some outputs with survey references. Then add any data source that can be matched to Mobility outputs. Several checks make it easier to see whether the model represents the mobility system across trip rates, modes, space, time, and network supply.

Useful checks include:

- trip rates and immobility by socio-professional category,
- mode shares by mode and by relevant population segment,
- travel distance and travel time distributions,
- activity duration and activity time series,
- main origin-destination flows and boundary effects,
- opportunity occupation by activity and destination zone,
- modelled travel times against observed or expected travel times,
- traffic counts, screenlines, or road volumes when they can be matched to model outputs,
- public-transport boardings, service counts, or stop/line indicators when they can be matched to model outputs.

The exact list depends on the study. A station-access study, a low-emission-zone study, and a land-use scenario do not need the same evidence.

## 3. Calibration And Validation Evidence

When observed data is available, separate three kinds of evidence:

- survey evidence, such as trip rates, motives, mode shares, distance distributions, and activity timing,
- spatial evidence, such as OD patterns, commuting flows, counts, boardings, or screenlines,
- supply evidence, such as travel times, public-transport calendars, speeds, and network assumptions.

Mobility can compare some metrics with survey references through `reference="external"`. Other checks still need project-specific tables or plots. Keep a short record of which observations were used for calibration, which were kept for validation, and which assumptions could only be tested with sensitivity analysis.

Mobility currently leaves benchmark thresholds to the project team. Choose them from the study question, the available evidence, and the consequences of the decision.

## 4. Sensitivity And Replications

Use replications to estimate how much results depend on sampling and random choices. Use sensitivity tests to estimate how much results depend on modelling assumptions.

Common sensitivity tests include:

- population sample size,
- number of iterations,
- generalized-cost parameters,
- activity opportunity proxies,
- public-transport access, waiting, and transfer assumptions,
- congestion or network feedback parameters,
- zoning level and reporting perimeter.

Scenario gaps should be larger than the variation created by sampling, iterations, and uncertain assumptions before they are treated as stable.

## Current Assumptions To Keep Visible

Some limits should be documented in project reports:

- default parameters are a mix of reasonable defaults and values that are not locally calibrated,
- national survey transfer can miss local behaviour patterns,
- transport-zone geometry, centroids, and boundary choices can affect short trips and maps,
- public transport uses a selected service period and generalized costs,
- congestion feedback needs project validation,
- land-use and activity opportunity proxies need project judgement when they drive the result.

## 5. Reporting A Scenario Result

A scenario result should report:

- the reference scenario,
- the changed assumptions,
- the day type and scenario year or context,
- the sample size, iterations, replications, and seeds,
- the input data versions that matter for the conclusion,
- the main diagnostics and sensitivity checks,
- the limits of interpretation.

This is especially important when a scenario changes land use, public transport service, car cost, network speed, or congestion feedback.
