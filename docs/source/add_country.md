# Add a country

Mobility gets the country list from the study area and transport zones. The
modeller should still define only the study area, transport zones, population,
activities, modes, surveys, and run parameters.

Adding a country means preparing the data that those objects need.

See [current countries](current_countries.md) for the sources used today by
France and Switzerland.

Start with local admin units. A country can only appear in a study area after
Mobility can load its local administrative units. Population, activity, survey,
and public transport data are only requested for countries that are present in
the transport zones.

This means a radius around a border city does not automatically add a country
that has no local admin units yet. For example, a Strasbourg study area remains
France-only until German local admin units are added.

## Edit points

Add the new country close to the data it describes.

| Input | Add country here | Example to copy |
| --- | --- | --- |
| Local admin units | `mobility/spatial/countries.py` | `FrenchAdminUnits` |
| Local admin unit categories | `mobility/spatial/countries.py` | `FrenchLocalAdminUnitsCategories` |
| Legal population | `mobility/population/countries.py` | `FrenchCityLegalPopulation` |
| Population groups | `mobility/population/countries.py` | `FrenchPopulationGroups` |
| Work data | `mobility/activities/work/countries/` | `FrenchWork` |
| Study data | `mobility/activities/studies/countries/` | `FrenchStudy` |
| Shopping data | `mobility/activities/shopping/countries/` | `FrenchShopping` |
| Public transport | `mobility/transport/modes/public_transport/gtfs/countries.py` | `FrenchGTFS` |

Each edit point has a short country list. Add the country code there after the
country file exists.

Example:

```python
def available_work_data():
    return {
        "fr": FrenchWork(),
        "ch": SwissWork(),
        "de": GermanWork(),
    }
```

## Local admin units

Prepare local admin units with:

- `admin_id`,
- `admin_name`,
- `country`,
- `geometry`.

The geometry must have a CRS. The shared `LocalAdminUnits` object converts these
columns to the historical local admin unit table used by the model.

Local admin unit IDs should be prefixed to avoid collisions, for example
`fr-75056` or `ch-6621`. Keep the country in the `country` column. Model code
should not read the country from the ID prefix.

Also prepare local admin unit categories with:

- `local_admin_unit_id`,
- `urban_unit_category`.

## Population

Legal population must return:

- `local_admin_unit_id`,
- `legal_population`.

Population groups must build rows with:

- `transport_zone_id`,
- `local_admin_unit_id`,
- `age`,
- `socio_pro_category`,
- `ref_pers_socio_pro_category`,
- `n_pers_household`,
- `n_cars`,
- `weight`,
- `country`.

Every country present in the transport zones must have legal population and
population groups.

## Surveys

Every population country must have survey coverage.

Each survey object must have:

- `survey_name`,
- `country`.

The survey tables must be compatible with the daily activity, time, mode, and
immobility calculations used by `PopulationGroupDayTrips`.

## Work data

Create a country file in `mobility/activities/work/countries/`.

The country work object has:

- `opportunities`,
- `flows`.

`WorkOpportunities.validate_opportunities(...)` checks the required columns.
`WorkFlows.validate_flows(...)` checks the required flow columns. These checks
fail before invalid tables are cached.

Work opportunities need:

- jobs index named `local_admin_unit_id`,
- jobs column `n_jobs_total`,
- active population index named `local_admin_unit_id`,
- active population column `active_pop`.

Work flows need:

- `local_admin_unit_id_from`,
- `local_admin_unit_id_to`,
- `mode`,
- `ref_flow_volume`.

Flow filters should keep rows where the origin or the destination is in the
selected local admin units. This keeps boundary-crossing flows.

If two national files contain the same cross-border flow, use
`country_priority_order` to choose one source:

```python
work_flows = mobility.WorkFlows(
    countries=["fr", "ch"],
    local_admin_unit_ids=["fr-74010", "ch-6621"],
    country_priority_order=["ch", "fr"],
)
```

## Study data

Create a country file in `mobility/activities/studies/countries/`.

The country study object has:

- `opportunities`,
- `flows` when flow data exists.

`StudyOpportunities.validate_opportunities(...)` checks the required opportunity
columns. `StudyFlows.validate_flows(...)` checks the required flow columns.

Study opportunities need:

- `local_admin_unit_id`,
- `geometry`,
- `school_type`,
- `n_students`.

Study flows need:

- `local_admin_unit_id_from`,
- `local_admin_unit_id_to`,
- `school_type`,
- `n_students`.

If a country has no study flow source yet, the country flow object should fail
with a clear message.

## Shopping data

Create a country file in `mobility/activities/shopping/countries/`.

The country shopping object has:

- `opportunities`.

`ShoppingOpportunities.validate_opportunities(...)` checks the required columns.

Shopping opportunities need:

- `local_admin_unit_id`,
- `lon`,
- `lat`,
- `turnover`.

France and Switzerland currently use a shared NAF/NOGA turnover bridge in the
shopping country folder. A new country should prepare its own shopping source in
its country file and return the required columns above.

## Public transport

Add GTFS source support in
`mobility/transport/modes/public_transport/gtfs/countries.py`.

The source class must insert GTFS rows into the `GTFSSources` table. The routing
code then uses the combined source list for the selected study area.

France also uses an area filter based on declared GTFS coverage. Other countries
can add a similar filter when their source metadata supports it.

## Checks before use

Before using a new country in a scenario study, check:

- all selected transport zones have a country,
- every population country has survey coverage,
- all opportunity tables pass their required-column checks,
- opportunities are positive and spatially plausible,
- cross-border flows are kept when they matter,
- public transport sources cover the study area and the study period,
- units and reference years are documented.
