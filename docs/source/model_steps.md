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

More concretely, Mobility derives each demand group's allowed activity set from survey-weighted participation by socio-professional category (CSP), with a country-level fallback when no CSP activity passes the support threshold. It pools compatible survey schedules by home zone, then associates a schedule with a demand group when all its non-home activities are allowed. Activity durations are estimated separately for each group.

At the same time, Mobility computes available opportunities by motive and by zone. Raw opportunity proxies, such as jobs or shops, are normalized within each activity and converted into `opportunity_capacity`. In this model, an opportunity is available activity time. Work opportunities are the amount of work activity that can be assigned to a destination.

## First Plans

Mobility builds a shared set of daily plan candidates and then attaches compatible candidates to demand groups.

The model combines:

- survey-weighted activity schedules sampled for a home zone,
- one sampled destination sequence for each schedule and home zone,
- candidate mode sequences searched for each home zone and utility profile,
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

## Simulation steps and search batches

The model advances through the configured simulation steps. Scenario changes
and behavior-change phases can start at a chosen step. At each step, Mobility:

1. Resolves the active scenario and updates travel costs when scheduled.
2. Generates candidate schedules, destinations and modes in search batches.
   Each batch adds alternatives to the shared candidate pool, using the state
   and costs fixed at the start of the step.
3. Scores eligible plans for each demand group and estimates allocations.
   Search ends when it reaches its batch limit, converges, or has no more
   generation work.
4. Applies the allowed plan changes and updates destination occupation and
   diagnostics for the next step.

Search batches explore alternatives within a simulation step; they do not
advance the modeled system. For the sampling and stopping settings, see
[run parameters](run_parameters.md). Read the resulting step diagnostics
alongside scenario comparisons to check whether the model behaves consistently
for the study question.

## Results

At the end of a run, Mobility can compute indicators such as:

- trip counts,
- distances,
- travel times,
- modal shares,
- greenhouse gas emissions,
- zone flows,
- diagnostics by simulation step.

Use these outputs to compare scenarios and check whether the model result is stable at the precision needed for the study question. When observed data is available, use it for calibration, validation, or reasonableness checks before treating scenario differences as evidence.
