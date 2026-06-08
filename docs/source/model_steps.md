# Model Steps

This page gives a compact view of the current `PopulationGroupDayTrips` workflow.

The exact implementation can change, but the modelling logic follows these broad steps.

The original modelling discussion is tracked in [GitHub issue 145](https://github.com/mobility-team/mobility/issues/145#issuecomment-3228039287).

## Transport Modelling Context

Mobility uses concepts that are common in travel demand modelling: synthetic population, activity patterns, destination choice, mode choice, generalized cost, network costs, scenario comparison, and diagnostics. It still needs the usual study discipline: define the modelling purpose, document assumptions, check the base case against available observations, test sensitivity where assumptions are uncertain, and present scenario results with their limits.

## Initialisation

Mobility prepares:

- transport zones,
- a synthetic population,
- activity opportunities by zone,
- observed mobility patterns from surveys,
- transport costs between zones for each mode.

More concretely, Mobility first generates activity-motive sequences for residents of each transport zone. These sequences depend on the resident profile, including socio-professional category, household car ownership, household size, and urban setting. Mobility also estimates the activity-time needs attached to each step of the sequence.

At the same time, Mobility computes available opportunities by motive and by zone. Raw opportunity proxies, such as jobs or shops, are normalized within each activity and converted into `opportunity_capacity`. In this model, an opportunity is available activity time. Work opportunities are the amount of work activity that can be assigned to a destination.

## First Plans

For each population group, Mobility builds daily activity plans.

The model combines:

- the activity sequence,
- possible destinations,
- possible mode sequences,
- travel costs,
- available opportunities.

## Opportunity Capacity

For each activity, Mobility starts from a raw opportunity proxy `n_opp` by destination zone. Examples include jobs, schools, shops, leisure facilities, or a project-specific table.

The model converts this raw proxy into activity-time capacity:

```text
opportunity_capacity(zone, activity)
    = n_opp(zone, activity)
    / sum(n_opp(*, activity))
    * total_activity_duration(activity)
    * sink_saturation_coeff(activity)
```

This means a destination with twice as much `n_opp` receives twice as much capacity for that activity, after the activity total is fixed by the modelled demand. Capacity is then compared with modelled occupation:

```text
capacity_ratio = opportunity_occupation / opportunity_capacity
```

The destination saturation utility factor is:

```text
k_saturation_utility
    = max(0, 1 - capacity_ratio^beta / saturation_ref_level^beta)
```

When shadow prices are enabled, the model can also apply a negative destination shadow price once occupation exceeds the soft capacity. This reduces the utility of overloaded destinations.

## Plan Utility

Mobility scores candidate plan steps before selecting transitions between plans.

For each non-home plan step, the current implementation uses:

```text
step_utility
    = activity_utility_scale
    * max(0, log(duration_per_person / min_activity_time))
    - generalized_travel_cost
```

where:

```text
min_activity_time = mean_activity_duration * exp(-min_activity_time_constant)
```

Without destination shadow prices:

```text
activity_utility_scale
    = country_value_coefficient
    * k_saturation_utility
    * value_of_time
    * mean_activity_duration
```

With destination shadow prices:

```text
activity_utility_scale
    = (country_value_coefficient * value_of_time + destination_shadow_price)
    * mean_activity_duration
```

The plan utility is the sum of step utilities, plus a home-night utility term. Transition probabilities are then computed from current and candidate plan utilities, with optional pruning, revision probability, and transition-distance friction.

## Iterations

The model then runs several iterations.

At each iteration, it can:

- update transport costs,
- update destination choices,
- update mode choices,
- update opportunity saturation,
- store indicators and diagnostics.

Scenario changes can start at specific iterations. This lets a reference state settle before a project change is applied.

The loop can be read in this order:

1. Compute generalized transport costs for each motive, origin, and destination. The first iteration starts without congestion.
2. Compute destination-choice probabilities from the motive, trip origin, home zone, and generalized costs.
3. Sample a destination sequence for each activity sequence, home zone, and socio-professional category.
4. Search candidate mode sequences for the resulting trip sequence. The number of candidates is controlled by `GroupDayTripsModeSequenceParameters.k_mode_sequences`, which defaults to 3.
5. Compute flows by origin-destination pair and by mode, then refresh generalized costs. Congestion can be applied at this step when the mode and parameters allow it.
6. Select the share of people whose plan should change. This depends on destination opportunity saturation, possible comparative improvements, and a random change share.
7. Update the remaining opportunities at destinations.
8. Repeat the procedure for the people whose plans are still open to change.

This iterative structure is why model outputs should be read with diagnostics. A scenario comparison is easier to interpret when the iteration metrics show that the model has behaved consistently for the study question.

## Results

At the end of a run, Mobility can compute indicators such as:

- trip counts,
- distances,
- travel times,
- modal shares,
- greenhouse gas emissions,
- zone flows,
- diagnostics by iteration.

Use these outputs to compare scenarios and check whether the model result is stable at the precision needed for the study question. When observed data is available, use it for calibration, validation, or reasonableness checks before treating scenario differences as evidence.
