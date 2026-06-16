# Transport Modes

Transport modes describe the options available to the population.

For a first model, start with a limited set of modes such as car, walk, and bicycle. Once the workflow is running, you can add public transport, carpool, congestion, or more detailed costs.

Build modes in layers:

1. Start with car, walk, and bicycle.
2. Check trip counts, distances, and OD flows against the evidence available for the study area.
3. Add public transport if GTFS data and access assumptions are ready.
4. Add congestion or carpool when the study question needs them.

This keeps the supply assumptions easier to review. If a larger setup fails, return to the limited mode set and add one feature at a time.

## Initial Mode Set

```python
walk = mobility.WalkMode(transport_zones)
bicycle = mobility.BicycleMode(transport_zones)
car = mobility.CarMode(transport_zones)
```

These modes use the study area's transport network and compute travel costs between transport zones.

## Network Data

Road, walking, and cycling networks come from OpenStreetMap. When a maximum speed is missing in OSM, Mobility infers it from the road category.

Mobility converts the OSM data into transport graphs and simplifies them to keep routing computations manageable:

- successive road segments without intersections are merged,
- dead-end segments are removed.

The dead-end simplification assumes that the buildings representing transport zones are located near roads that can be used for routing between zones.

The table below shows which OSM categories are currently kept for car, walk, and bicycle graphs.

| OSM category | Car | Walk | Bike |
| --- | --- | --- | --- |
| `bridleway` | No | Yes | Yes |
| `cycleway` | No | Yes | Yes |
| `ferry` | Yes | Yes | Yes |
| `footway` | No | Yes | Yes |
| `living_street` | Yes | Yes | Yes |
| `motorway` | Yes | No | No |
| `motorway_link` | Yes | No | No |
| `path` | No | Yes | Yes |
| `pedestrian` | No | Yes | Yes |
| `primary` | Yes | Yes | Yes |
| `primary_link` | Yes | Yes | Yes |
| `residential` | Yes | Yes | Yes |
| `secondary` | Yes | Yes | Yes |
| `secondary_link` | Yes | Yes | Yes |
| `service` | Yes | Yes | Yes |
| `steps` | No | Yes | Yes |
| `tertiary` | Yes | Yes | Yes |
| `tertiary_link` | Yes | Yes | Yes |
| `track` | No | Yes | Yes |
| `trunk` | Yes | Yes | Yes |
| `trunk_link` | Yes | Yes | Yes |
| `unclassified` | Yes | Yes | Yes |

## Generalized Cost

Mobility compares modes with generalized cost, a standard transport modelling concept used to express travel impedance.

Generalized cost is a way to bring several travel constraints into one value. It can include:

- time,
- distance cost,
- a fixed cost for using a mode,
- waiting or transfer time.

For path-based modes such as car, walk, and bicycle, Mobility computes:

```text
generalized_cost = cost_constant
    + cost_of_distance * distance
    + cost_of_time(distance, country) * time
```

Distances are in kilometres and travel times are in hours. The cost unit comes from your parameters. Euro-per-kilometre and euro-per-hour parameters give a cost in euros. Abstract utility weights give an impedance value.

Example:

```python
car = mobility.CarMode(
    transport_zones,
    generalized_cost_parameters=mobility.GeneralizedCostParameters(
        cost_constant=3.0,
        cost_of_distance=0.0,
        cost_of_time=mobility.CostOfTimeParameters(
            intercept=5.0,
        ),
    ),
)
```

These values are modelling assumptions. In a calibrated model, justify cost parameters with local evidence or accepted references. In an exploratory model, keep them visible and test how sensitive results are to them.

For sensitivity testing, change one cost assumption at a time when possible. This makes it easier to explain whether a result changed because of time sensitivity, distance cost, fixed cost, or another parameter.

## Public Transport

Public transport uses GTFS feeds when they are available.

Add public transport after the initial mode set runs correctly. Public transport introduces more assumptions than car, walk, and bicycle: service calendars, waiting time, transfers, access and egress modes, and possible extra scenario feeds.

The current public-transport workflow computes selected-period generalized costs with average waiting and transfer assumptions.

It is usually combined with an access mode and an egress mode. A common first setup is walk, public transport, then walk:

```python
walk_transfer = mobility.IntermodalTransfer(
    max_travel_time=20.0 / 60.0,
    average_speed=5.0,
    transfer_time=1.0,
)

walk_pt = mobility.PublicTransportMode(
    transport_zones,
    first_leg_mode=walk,
    last_leg_mode=walk,
    first_intermodal_transfer=walk_transfer,
    last_intermodal_transfer=walk_transfer,
    routing_parameters=mobility.PublicTransportRoutingParameters(
        gtfs_reference_date="2026-01-01",
        gtfs_sources_folder="inputs/gtfs_sources",
    ),
)
```

Project scenarios can add extra GTFS files. Use this to represent a defined service assumption, such as a new line, a changed frequency, or a temporary service change.

### GTFS Data Preparation

GTFS feeds describe stops, calendars, service times, routes, and public-transport modes such as bus, tram, train, and metro.

Mobility can select official GTFS files for the countries covered by the study area. The modeler must provide a `gtfs_reference_date` and a project folder for the GTFS sources file. Mobility then builds a small SQLite file listing the GTFS sources selected for that date and study area. This file can be kept with the project inputs so another user can run with the same source catalog.

For France, Mobility first uses the `covered_area` metadata from transport.data.gouv.fr to skip GTFS datasets that are clearly outside the study area. This is only a first filter. Mobility still checks the operator coverage geometry before selecting a GTFS file for the run. Other countries can use the same pattern with their own GTFS source table.

By default, Mobility only uses reproducible archived GTFS files. If an intersecting source has no archived file, or if its latest archived file is too old, Mobility warns and skips it. If no usable public transport source remains for the study area, the run fails. Live GTFS URLs can be enabled explicitly with `use_live_gtfs=True`, but this makes results depend on the provider state at download time.

```python
routing_parameters = mobility.PublicTransportRoutingParameters(
    gtfs_reference_date="2026-01-01",
    gtfs_sources_folder="inputs/gtfs_sources",
    max_gtfs_file_age_days=30,
)
```

For project use, operator feeds are filtered to keep lines with at least one stop in the study transport zones. The feeds use a common date and are merged into one feed. Missing transfers are added between stops within 200 metres, with transfer time estimated from straight-line distance.

The current public-transport preparation selects a Tuesday service day from the GTFS calendars, using the Tuesday with the most services in the month with the highest average service. Treat this selected service day as a modelling assumption and record the GTFS versions and dates used by the run.

For project documentation, record which GTFS feeds and dates were used. Public-transport results are hard to interpret later when service calendar traceability is missing.

### Public-Transport Graph

Mobility converts the public-transport offer into a graph that can be combined with access and egress graphs such as walking, cycling, or driving.

The graph preparation includes:

- grouping nearby stops within 40 metres to create entry and exit nodes,
- computing average travel times between two stops on the same line,
- computing waiting times between arrivals and departures of the same line at a stop,
- computing average minimum transfer times to other accessible services,
- discarding transfers longer than 20 minutes,
- estimating initial waiting time from average headway,
- estimating perceived waiting time from headway and missed-service risk,
- computing the difference between the target arrival time and possible actual arrival times,
- computing straight-line distances between stops for distance-based indicators.

These travel times are approximations. Real travel time depends on the departure time, because vehicle speeds, waiting times, and transfer times change during the day.

The public-transport travel time can include:

- waiting at the access node,
- travel to an intermediate transfer stop,
- waiting during a transfer,
- travel to the egress node.

A maximum travel time filters out paths with too many transfers or too much detour.

### Create A Small Scenario GTFS Feed

For a light scenario, you can create an additional GTFS feed directly in Python. This can represent a provisional service assumption before preparing a full operator-style feed.

```python
import mobility

builder = mobility.GTFSBuilder(
    agency_id="test_agency",
    agency_name="Test Agency",
    route_id="test_line",
    route_short_name="T1",
    route_type="bus",
    service_id="test_service",
)

builder.add_stops(
    {
        "first_stop": [6.151086, 46.209558],
        "second_stop": [6.192774, 46.253015],
    }
)
builder.add_line(
    [("first_stop", "second_stop", 27 * 60)],
    start_time=6 * 3600,
    end_time=9 * 3600,
    period=15 * 60,
)

gtfs_path = builder.write_project_zip("test_line.zip")
```

Then pass `gtfs_path` through `PublicTransportRoutingParameters(additional_gtfs_files=[gtfs_path])` when you configure public transport routing.

## Congestion

Car mode can include congestion:

```python
car = mobility.CarMode(
    transport_zones,
    congestion=True,
    congestion_flows_scaling_factor=0.5,
)
```

Use congestion once the basic model is running and the study question needs network feedback. It makes the run heavier and adds parameters that should be documented with the other supply assumptions.

Check congestion feedback against the traffic evidence available for the study: counts, screenlines, known bottlenecks, or observed travel times. Treat congestion as a sensitivity assumption until that check is done.

## Main Objects

The main user-facing objects are:

- `mobility.WalkMode`
- `mobility.BicycleMode`
- `mobility.CarMode`
- `mobility.CarpoolMode`
- `mobility.PublicTransportMode`
- `mobility.IntermodalTransfer`
- `mobility.GeneralizedCostParameters`
- `mobility.CostOfTimeParameters`
- `mobility.PublicTransportRoutingParameters`
- `mobility.GTFSBuilder`
