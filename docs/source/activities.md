# Activities

Activities describe why people travel.

They also provide the opportunity proxies that can attract trips to each transport zone. For example, work activity uses jobs, study activity uses schools or universities, and shop activity uses shops.

In the model loop, these raw proxies are converted into `opportunity_capacity`. The capacity is expressed as activity time that can be assigned to a destination. Jobs, schools, and shops are raw proxies. This matters when you interpret opportunity occupation and saturation.

## Main Activities

The usual activities are:

- `HomeActivity`
- `WorkActivity`
- `StudyActivity`
- `ShopActivity`
- `LeisureActivity`
- `OtherActivity`

A minimal first model can use only home, work, and other:

```python
activities = [
    mobility.HomeActivity(),
    mobility.WorkActivity(),
    mobility.OtherActivity(population=population),
]
```

`PopulationGroupDayTrips` requires at least one `HomeActivity` and one `OtherActivity`.

## A More Detailed Activity Set

A project model can add study, shop, and leisure:

```python
activities = [
    mobility.HomeActivity(),
    mobility.WorkActivity(),
    mobility.StudyActivity(),
    mobility.ShopActivity(),
    mobility.LeisureActivity(),
    mobility.OtherActivity(population=population),
]
```

Each activity brings its own opportunity data or opportunity-building logic.

For the standard activities, the modeller normally defines only the study area
and transport zones. Mobility then builds the needed opportunity data for the
countries present in those transport zones and keeps the rows that match the
selected local admin units.

The built-in data currently covers:

- work opportunities from jobs and active population,
- study opportunities from school and university capacity,
- shopping opportunities from shop turnover proxies,
- leisure opportunities from OpenStreetMap facilities.

If the study area contains a country without the needed built-in data, Mobility
fails with a clear message. Adding that country means preparing the same
normalized opportunity table for that activity, not changing the
`PopulationGroupDayTrips` workflow.

For a new country, each standard activity needs the same kind of input:

- work needs jobs, active population, and home-work flows,
- study needs school or university capacity, and school flows when that source is available,
- shopping needs a shop opportunity proxy,
- leisure is still built from OpenStreetMap facilities.

These inputs live with the activity they describe. For example, work data for a
country belongs in `mobility/activities/work/countries/` and has work
opportunities and work flows in the same normalized format as the existing
countries.

The full country checklist is documented in [add a country](add_country.md).

Use the minimal activity set when you are checking installation, testing a territory, or debugging modes.

Use the detailed activity set when the study question needs those motives. For example:

- add `StudyActivity` when school or university trips matter,
- add `ShopActivity` when retail access or shopping travel is part of the analysis,
- add `LeisureActivity` when leisure facilities or non-work travel are important.

Adding activities can make the model more expressive, but it also adds opportunity data and assumptions that need to be checked. In a study report, activity definitions should be linked to the question being asked: for example school access, retail access, leisure travel, or non-work travel.

For custom opportunities, provide a table with a destination zone column `to` and a positive opportunity proxy column `n_opp`. Mobility normalizes `n_opp` within each activity and scales it to the modelled activity-time demand.

The capacity formula is documented in the [model steps page](model_steps.md).

## Activity Parameters

Activities can use parameters such as:

- value of time,
- destination choice sensitivity,
- opportunity saturation level,
- opportunity saturation strength.

Example:

```python
work = mobility.WorkActivity(
    value_of_time=2.5,
    radiation_lambda=0.99986,
    saturation_fun_ref_level=1.2,
    saturation_fun_beta=4.0,
)
```

These values are modelling assumptions. `radiation_lambda` controls destination-choice dispersion. The saturation parameters control how attractive or costly a destination becomes as its modelled occupation approaches or exceeds capacity. Keep these assumptions visible, and test the sensitivity of important results to them when they are not calibrated from local evidence.

## Other Activity

`OtherActivity` needs either:

- a `population`, so residents are used as a proxy for other opportunities,
- or an explicit opportunities dataframe.

The usual first setup is:

```python
other = mobility.OtherActivity(population=population)
```

This is a first approximation. For a project where "other" trips are important, document why residents are an acceptable proxy for destination opportunities, or replace the proxy with project-specific opportunity data.
