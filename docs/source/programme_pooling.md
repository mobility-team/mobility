# Borrowing French EMP programmes

A day programme describes a person's sequence of activities and their times.
Some population groups have very few complete days in the EMP survey. Mobility
lets these groups borrow programmes from similar groups across France. This is
called *pooling*. It is disabled by default to keep the existing sampling behavior.

## How it works

Groups with plenty of survey data mostly use their own programmes. Groups with
less data borrow more. Urban category, car ownership and occupation guide donor
selection but do not prevent borrowing across those groups. Weekdays and weekends
remain separate. The EMP category for inactive children under 15 also remains
separate from older groups.

Donors must have a complete home-to-home day with valid times and two to ten trips.
Staying home is handled separately. Probabilities are adjusted to match survey
summaries of trip counts, activities, departure times and activity durations.
Mode shares and distances are not used to fit these probabilities.

## Settings

To enable it for EMP, pass these settings to your usual `PopulationGroupDayTrips` setup:

```python
parameters = mobility.GroupDayTripsParameters(
    programme_pooling=mobility.ProgrammePoolingParameters(
        enabled=True,
        pooling_strength=30,
        fit_regularization=0.1,
    )
)
```

| Setting | What increasing it does |
|---|---|
| `pooling_strength` | Borrows more from broader groups and relies less on the exact group's data. |
| `fit_regularization` | Keeps donor probabilities closer to their starting survey weights and matching preferences, allowing less adjustment to the target summaries. |

`pooling_strength` is compared with the group's effective sample size, which
accounts for unequal survey weights. When both equal 30, local and broader data
receive equal weight in each mixture. The defaults are starting values, not
values calibrated for every study.

Use `mobility.ProgrammePoolingParameters(enabled=False)` to sample only from each
group's legacy programmes. This keeps the existing diary repairs and model priors.
The stricter complete-day rule applies to pooling donors and the fixed reference,
whether pooling is enabled or disabled.

## Survey observations stay unchanged

Mobility keeps two probabilities for each donor assigned to a target group:

- `p_sampling` controls programme sampling after borrowing.
- `p_reference` keeps the original survey probability in the donor's own group
  and is zero in borrowed assignments.

Changing pooling settings can change model results, but cannot change the survey
reference. Original modes and distances remain separate even when two respondents
have identical activity schedules. A group without original eligible days has no
observed reference, even if it can borrow programmes.

The model's duration and opportunity assumptions currently remain unpooled. They
are model inputs, separate from the observations used to evaluate results.

## Check the amount of borrowing

For a run with pooling enabled:

```python
results = population_trips.results("weekday")
coverage = results.diagnostics.programme_pooling()
assignments = results.diagnostics.programme_pooling(assignments=True)
```

`coverage` shows which groups have programmes, how much they borrow, and their
effective sample sizes. `assignments` lists the donor days and both probabilities.
Borrowed support should not be confused with original survey coverage.
