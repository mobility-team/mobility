# Study Area And Transport Zones

Every Mobility study starts with a territory.

Transport zones are the spatial units used by the model. People live in transport zones, activities happen in transport zones, and travel costs are computed between transport zones.

For a first run, you can use a small area around one city. For a project study, you can use a larger list of local administrative units and choose a smaller inner perimeter for the indicators.

## A Small Study Area

For a first French example, use a city code and a radius:

```python
transport_zones = mobility.TransportZones("fr-87085", radius=10.0)
```

This creates a small study area around Limoges. It keeps runtime lower than a large territory during the first workflow check.

Use this pattern when you want to test the workflow around one commune before preparing a full study perimeter. Choose the radius as a modelling assumption: it should keep the run tractable and avoid an artificial cut through important nearby origins or destinations.

## Cross-Border Radius Searches

A radius search can only include countries for which Mobility has local
administrative units. Mobility first builds the study area from local
administrative units, then uses the countries found in that study area to select
population, activity, survey, and public transport inputs.

For example, a radius around Strasbourg currently uses French local
administrative units. German municipalities are not added, because Germany does
not yet have local administrative units in Mobility. Downstream objects therefore
do not request German population or activity data for that run.

When a neighbouring country is missing from a radius-based study area, treat the
result as a modelling limitation of the current data coverage. To include that
country, first add its local administrative units, then add the other data needed
by the model.

## Several Local Administrative Units

For a project study, pass a list of local administrative unit ids:

```python
transport_zones = mobility.TransportZones(
    ["fr-74010", "fr-74133"],
    level_of_detail=1,
)
```

`level_of_detail` controls how detailed the generated transport zones should be. The current public values are `0` and `1`. Zone detail affects routing time, aggregation, and how much spatial variation can be interpreted.

A practical sequence is:

- use `level_of_detail=0` for a first run, a broad diagnostic, or a faster sensitivity test,
- use `level_of_detail=1` when intra-communal differences matter for the study question,
- move from `0` to `1` only after the first setup runs correctly.

More detailed zones can improve spatial interpretation, but they also increase routing work, file sizes, and the number of results to explain. As with any zonal transport model, the zoning system is part of the model specification: it can affect OD flows, modal indicators, and maps.

Mobility creates detailed zones from local administrative units and building-footprint density. Sparse generated zones can be merged before final zone ids are written. A zone is a modelling object used for population allocation, opportunity allocation, route-cost matrices, and result aggregation.

## A Main Perimeter Inside A Larger Area

Many studies need two perimeters:

- a large area for routing and cross-border trips,
- a smaller area where the final indicators are read.

Use `inner_local_admin_unit_id` for the smaller reporting area:

```python
transport_zones = mobility.TransportZones(
    lau_ids,
    level_of_detail=1,
    inner_local_admin_unit_id=inner_lau_ids,
)
```

This is useful when people can travel outside the main perimeter, but the study focuses on residents or zones inside it.

This is common for metropolitan and cross-border studies. Keep the routing area large enough to avoid cutting real trips too early, then use the inner perimeter for the indicators you report.

For a radius-based study area, `inner_radius` can be used in the same spirit: keep a larger routing area, then flag a smaller inner area for reporting.

## Cut Out Areas That Should Not Become Zones

Use `cutout_geometries` to remove areas such as a lake:

```python
transport_zones = mobility.TransportZones(
    lau_ids,
    cutout_geometries=lake_geometry,
)
```

This keeps the generated zones closer to the study area geography.

Use cutouts when a large geometry would otherwise create misleading zones, for example a lake, a mountain area, or another place where people do not live and transport-zone centroids would be hard to interpret.

## Practical Checks

After creating transport zones, check:

- the number of zones,
- whether the main communes are present,
- whether the inner perimeter is correct,
- whether obvious no-population areas have been removed,
- whether the level of detail is understandable for maps and tables,
- whether zone centroids and network access points are credible for the shortest trips,
- whether boundary zones are affecting the indicators you plan to report.

If runtime or interpretation becomes difficult, review the perimeter and zone detail before adding more modes or scenarios.

## Main Objects

The main user-facing objects are:

- `mobility.LocalAdminUnits`
- `mobility.TransportZones`
