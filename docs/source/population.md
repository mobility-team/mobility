# Population And Surveys

Mobility needs two inputs to model people:

- a synthetic population for the study area,
- one or more mobility surveys to describe observed travel behaviour.

## Synthetic Population

Use `Population` after creating transport zones:

```python
population = mobility.Population(
    transport_zones,
    sample_size=1000,
)
```

`sample_size` is the number of people sampled for the model. A larger sample gives more stable indicators but takes more time and disk space.

Result tables use represented-person weights. Mobility carries this weight, usually exposed as `n_persons`, and result metrics use it to compute trip counts, distances, times, emissions, and activity occupation. The first population check is: does the represented population match the study area?

For a first run, use a small sample to check the workflow. For project results, increase it and check sampling variability on the indicators you plan to report.

Typical computational use:

- a few hundred people for a CI run or a quick code check,
- around 1000 people for a first local run,
- a larger sample for project indicators, especially when you read results by zone, mode, activity, or socio-professional category.

There is no universal sample size. The useful size depends on the territory, the indicators you report, and how much variability you can accept.

## Surveys

For a French study area, use the EMP survey:

```python
survey = mobility.EMPMobilitySurvey()
```

For cross-border or project-specific studies, you can combine surveys:

```python
surveys = [
    mobility.EMPMobilitySurvey(),
    project_specific_survey,
]
```

Each country in the population needs survey data. If a project adds a custom survey parser, keep that parser in the project repository and pass the resulting survey object to Mobility.

Population, admin units, activity opportunities, and public-transport sources are country-specific data inputs. The shared model only needs the normalized tables and the matching `country` code for each study-area part.

To add a country, prepare these inputs with the same lower-case country code:

- local admin units with `local_admin_unit_id`, `local_admin_unit_name`, `country`, `urban_unit_category`, and `geometry`,
- population groups with `transport_zone_id`, `local_admin_unit_id`, household/person attributes, `country`, and `weight`,
- mobility surveys with `survey_name` and `country`,
- activity opportunities with destination zone `to` and opportunity count `n_opp`,
- GTFS source files covering the study area.

National surveys contain detailed behaviour patterns. For a serious project, compare model outputs with local evidence when it exists: household travel surveys, commuting flows, counts, public-transport boardings, or other project data.

## Practical Advice

Start with a small sample to check the full workflow.

Then increase the sample size and compare:

- total trip counts,
- immobility and trips per person,
- distance by mode,
- emissions by mode,
- key zone indicators.

If these indicators move more than the study can tolerate, increase the sample size or use replications before drawing conclusions from scenario differences.

For a project report, keep the sample size and random seeds in the parameter report. This makes it easier to distinguish a real scenario effect from sampling noise.
