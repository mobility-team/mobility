# Definitions

This page defines common words used in Mobility.

## Transport Zone

A transport zone is a spatial unit used by the model.

People live in transport zones. Activities and opportunities are counted by transport zone. Transport costs are computed between transport zones.

## Local Administrative Unit

A local administrative unit is an official administrative area, such as a French commune.

Mobility can use local administrative units to build a study area before creating smaller transport zones.

## Synthetic Population

A synthetic population is a sample of people that represents the population of the study area.

It is a model input used to simulate daily mobility with represented people.

## Activity

An activity is a reason to be somewhere during the day.

Common activities are:

- home,
- work,
- study,
- shopping,
- leisure,
- other.

## Opportunity

An opportunity is something that can attract an activity to a zone.

Examples:

- jobs for work,
- schools for study,
- shops for shopping,
- leisure facilities for leisure.

In `PopulationGroupDayTrips`, raw opportunities are converted into activity-time capacity. An opportunity table helps distribute the amount of activity time that can be assigned to destination zones.

## Mode

A mode is a transport option used to make a trip.

Common modes are:

- walk,
- bicycle,
- car,
- public transport,
- carpool.

## Generalized Cost

Generalized cost combines several costs of travel into one value.

It can include:

- travel time,
- distance cost,
- a fixed cost for using a mode,
- waiting or transfer penalties.

Mobility uses generalized cost to compare destination and mode choices.

For path-based modes, Mobility combines a fixed term, a distance term, and a time term. Distance is in kilometres and time is in hours. The resulting cost unit depends on the parameters chosen by the modeller.

## Scenario

A scenario is a modelling assumption that can change between runs.

For example, a scenario can add a public transport line, change a speed, or add a cost to car travel.

## Reference Scenario

A reference scenario is the situation used for comparison.

It can be the current mobility system, or a baseline agreed by the study team for a forecast year.

## Iteration

An iteration is one step of the `PopulationGroupDayTrips` model loop.

Several iterations let the model update choices, costs, and opportunity saturation.

## Replication

A replication is another run of the same setup with another random seed.

Replications help estimate how much results depend on sampling and random choices inside the model.

## Survey Codes

French survey codes can appear in model inputs and outputs. They are listed on the [survey codes reference page](survey_codes.md).
