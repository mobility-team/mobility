# Research Method

This page gives a stable description of the modelling method behind `PopulationGroupDayTrips`.

It is written for researchers and expert modellers who need to understand what kind of model Mobility is, what assumptions are built into the workflow, and what evidence is needed before results can support a study conclusion. It does not replace the source code or a project validation report.

## Model Purpose

Mobility estimates daily mobility plans for a synthetic population in a local territory.

The model is designed for scenario studies where the team wants to compare a reference situation with changed assumptions, such as a transport-cost change, a public-transport offer, a road-network change, a land-use change, or a different set of activity opportunities.

The main output is a simulated set of daily plan steps, weighted to represent the study population. Indicators are computed from these plan steps:

- trip counts,
- travel distance,
- travel time,
- generalized cost,
- greenhouse gas emissions,
- origin-destination flows,
- activity and time-use indicators,
- iteration diagnostics.

## Model Family

Mobility is an activity-based daily mobility model.

It uses ideas that are common in transport modelling:

- synthetic population,
- observed activity and trip patterns from surveys,
- destination choice,
- mode choice,
- generalized transport cost,
- opportunity constraints,
- iterative replanning,
- scenario comparison.

Mobility works with sampled population groups and daily activity plans, then aggregates the simulated result for reporting.

Network costs and public-transport costs are approximations. Check them against evidence relevant to the study, especially when the conclusion depends on routing, public transport, or congestion.

## Main Inputs

A `PopulationGroupDayTrips` model combines:

- transport zones,
- a synthetic population,
- one or more mobility surveys,
- activities and opportunity data,
- transport modes and generalized costs,
- scenario definitions,
- run parameters.

The synthetic population provides residents and represented-person weights. Mobility surveys provide observed behaviour patterns. Activities provide destination opportunity proxies. Modes provide travel costs between zones. Scenarios provide changed assumptions to compare.

## Activity Plans

Mobility first builds daily activity-motive sequences for population groups.

These sequences depend on survey behaviour and population characteristics such as socio-professional category, household car ownership, household size, and urban setting. Each sequence contains activity steps and expected activity-time needs.

The model then searches for destinations and mode sequences that can make the daily plan feasible under the current costs and opportunity constraints.

## Opportunity Capacity

Destination opportunities are not read only as counts of facilities.

For each activity, Mobility starts from a raw opportunity proxy, such as jobs, schools, shops, leisure facilities, or a project-specific table. This raw proxy is converted into an activity-time capacity:

```text
opportunity_capacity(zone, activity)
    = n_opp(zone, activity)
    / sum(n_opp(*, activity))
    * total_activity_duration(activity)
    * sink_saturation_coeff(activity)
```

The model compares simulated occupation with capacity:

```text
capacity_ratio = opportunity_occupation / opportunity_capacity
```

This means that an opportunity table affects how activity time can be distributed over destinations. If a project result depends strongly on activity opportunities, the raw proxy and the capacity interpretation both need to be documented.

## Generalized Cost

Mobility uses generalized cost to compare travel alternatives.

For path-based modes such as car, walk, and bicycle, the default structure is:

```text
generalized_cost = cost_constant
    + cost_of_distance * distance
    + cost_of_time(distance, country) * time
```

Distances are in kilometres and travel times are in hours. The cost unit depends on the parameters used by the modeller. If the parameters are expressed in euros, the generalized cost can be interpreted as euros. If the parameters are utility weights, it should be interpreted as an impedance value.

Generalized-cost parameters are modelling assumptions. They should be kept in the parameter report and tested when they drive a study conclusion.

## Plan Utility

Candidate plan steps are scored with an activity utility term and a travel-cost term.

For each non-home plan step, the current implementation uses:

```text
step_utility
    = activity_utility_scale
    * max(0, log(duration_per_person / min_activity_time))
    - generalized_travel_cost
```

The plan utility is the sum of step utilities, plus a home-night utility term. The model then compares current and candidate plans when it decides which groups change plans during the iteration loop.

See [model steps](model_steps.md) for a more operational view of the loop and formulas.

## Iterative Replanning

The group-day-trip model runs over several iterations.

At each iteration, Mobility can:

- compute generalized costs,
- sample destination sequences,
- search mode sequences,
- update OD flows,
- refresh travel costs,
- update opportunity occupation,
- select plan changes,
- write diagnostics.

Scenario changes can start at a chosen iteration. This is useful when the modeller wants a reference state before a project assumption is introduced.

The iteration count is part of the model specification. Interpret a project result after diagnostics show that the study indicators are stable enough for the question being asked.

## Randomness And Replications

Mobility uses sampling and random choices in several places, including population sampling and plan updates.

Replications with different seeds estimate how much indicators move because of sampling and random choices.

Scenario differences should be larger than the variation created by sample size, iteration settings, and uncertain assumptions before they are treated as stable.

## Public Transport

Public transport uses GTFS feeds when they are available.

The current public-transport workflow computes selected-period generalized costs with average waiting and transfer assumptions.

For research or expert project work, record:

- GTFS feed sources,
- feed dates,
- selected service date or period,
- access and egress assumptions,
- waiting and transfer assumptions,
- any additional scenario GTFS feeds.

## Congestion

Car mode can include congestion feedback.

Congestion makes the model heavier and adds assumptions about how modelled flows affect network costs. Treat congestion as a sensitivity assumption until it has been checked against traffic counts, screenlines, observed travel times, or known bottlenecks.

## Validation And Interpretation

A technical run only proves that the workflow executed.

A study model needs evidence that the base case is reasonable for the question being asked. Useful evidence can include:

- survey-based trip rates and immobility,
- mode shares,
- travel distance and time distributions,
- activity duration and activity time series,
- home-work flows,
- origin-destination patterns,
- road counts or screenlines,
- public-transport boardings or service counts,
- observed or expected travel times.

Mobility can compute some survey-comparable checks with `reference="external"`. Other checks require project-specific observations and scripts.

The project team decides what is good enough for the study. Mobility does not currently provide universal validation thresholds.

## Main Limits To Report

When Mobility is used in research, teaching, or consulting, keep these limits visible:

- default parameters need local checks,
- national survey transfer can miss local behaviour,
- zoning and boundary choices can affect short trips and spatial indicators,
- opportunity proxies can drive destination results,
- public transport is represented by selected service assumptions,
- congestion feedback needs project validation,
- scenario effects can be smaller than sampling or parameter uncertainty.

## Reproducibility

For each research or project result, keep:

- Mobility version or commit,
- input data versions and download dates,
- package and project data folders, or an archive of the prepared inputs,
- sample size,
- random seeds,
- iteration and replication settings,
- scenarios and changed parameters,
- parameter report from `weekday_run.parameters_dataframe()`,
- validation and sensitivity evidence.

These items make it possible to understand what changed between two model runs and whether a scenario result can be reproduced.

## Suggested Citation Practice

When citing a result produced with Mobility, cite both:

- the Mobility package or repository version used for the run,
- the project-specific model documentation that describes inputs, assumptions, validation, and scenario definitions.

The package explains the software method. The project documentation explains whether the model setup was appropriate for the study question.
