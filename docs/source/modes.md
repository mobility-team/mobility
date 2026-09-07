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

Mobility prepares one dated timetable from the selected GTFS files, then uses it to calculate public-transport costs. The [GTFS preparation steps](gtfs-data-preparation) below describe how that day is chosen and which service assumptions are made.

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

(gtfs-data-preparation)=
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

#### Timetable Terms

A **feed** is one operator or data provider's GTFS ZIP file. A **route** usually represents a public-transport line. A **trip** (`trip_id`) is one vehicle journey along a sequence of stops; it is not a passenger journey. A **service calendar** (`service_id`) gives the operating dates shared by one or more trips.

A **stop visit** is one trip calling at one stop: one row in `stop_times.txt`. Ten trips calling at five stops each represent 50 stop visits. Repeated calls at the same stop count separately. This measures scheduled supply, not passenger numbers or the number of distinct stops.

#### Keep Trips Serving The Study Area

Mobility keeps stops within the transport zones and a surrounding 10 km area. A trip must have at least two stop visits in this area to be retained. These are visits, so they need not be at two different stops. Stops with missing or invalid coordinates are removed.

The complete stop sequence of a retained trip is used to check and estimate its times. Stops outside the area are then removed from the prepared timetable. This preserves the original first departure time when generating frequency-based departures.

Feeds with identical contents in the timetable tables used by preparation are included once. Different feeds from the same operator remain separate, even if some trips overlap. Mobility adds a feed number to identifiers such as `trip_id` and `service_id`, so identifiers from different feeds cannot be confused.

#### Check And Prepare Times

Arrival and departure times are converted to seconds from the start of the GTFS service day. Times beyond midnight, such as `25:10:00`, remain part of that service day. Times are not converted between the time zones of different feeds.

Blank times between known first and last times are estimated by linear interpolation using `stop_sequence`. This assumes time increases in proportion to the sequence numbers, rather than distance travelled. For example, a missing time at sequence 2 between 08:00 at sequence 1 and 08:10 at sequence 3 becomes 08:05.

A whole trip is removed if an arrival or departure time is missing at either end of its original sequence, if it departs before arriving at a stop, or if it reaches the next stop before leaving the previous one. A warning reports removed trips. Invalid time formats, duplicate stop sequence numbers, broken references between tables, and invalid calendar values raise errors that must be resolved in the input data.

For `frequencies.txt`, Mobility creates departures from `start_time`, separated by `headway_secs`, strictly before `end_time`. Each departure keeps the original trip's travel and stopping times. Frequency periods for the same trip must not overlap. For `exact_times=0` or an omitted `exact_times`, evenly spaced departures are an approximation: the source specifies a headway rather than exact departure times. A warning identifies this assumption.

#### Choose One Tuesday

Mobility applies the weekly calendars in `calendar.txt` and the additions and cancellations in `calendar_dates.txt` on their actual dates. A feed may use dated exceptions without a weekly calendar. Dates are never shifted to make feeds overlap.

After spatial filtering, time checks and frequency expansion, Mobility counts the stop visits on each candidate Tuesday across all retained feeds. It chooses the Tuesday with the largest total, taking the earliest date when totals are equal. It does not select a month first or count service calendar identifiers.

The count covers the whole service day, before the routing time window and boarding restrictions are applied. The selected day is therefore a measure of the greatest scheduled supply under these rules; it is not necessarily a typical day, the best morning peak, or the Tuesday closest to `gtfs_reference_date`.

The reference date chooses source archives; their operating calendars determine the selected service day. A feed with no service on that Tuesday contributes no trips and produces a warning. If no Tuesday has usable service, preparation stops with an error. Additional scenario feeds must contain the intended operating dates. Two successive feeds from one operator only contribute together where their real operating dates overlap.

Mobility then retains trips running on that date and their routes, agencies, stop visits and stops. If `expected_agencies` is set, each supplied name must match an agency with retained service, ignoring case and allowing part of a name.

#### Prepare Transfers And Service Information

Mobility adds walking connections in both directions between different retained stops within 200 metres. The assumed time is:

```text
transfer time in seconds = 31 + 1.125 × straight-line distance in metres
```

These connections do not follow footpaths or account for barriers. Declared transfers in `transfers.txt` can connect stops farther apart. A declared minimum time is used when present; otherwise the same distance formula is used. A station-level rule is applied to its retained platforms.

The following rules are carried into the public-transport graph:

- Declared rules take priority over added walking connections.
- A rule naming both connecting routes takes priority over one naming a single route, which takes priority over a rule applying to all routes.
- Among equally applicable rules, a prohibition prevents the transfer; otherwise the largest minimum time is used.
- Transfer type 1 does not cause the model to hold a connecting vehicle. It is handled with the available timetable and minimum walking time.
- Trip-specific rules and transfer types 4 and 5 (remaining in the same vehicle, or prohibiting that continuation) are unsupported and raise an error when their stops are used on the selected day.

Pickup and drop-off restrictions are preserved. The graph allows ordinary boarding and alighting only where the corresponding value is 0. Values 1, 2 and 3 do not permit that action in the model; booking or driver-arranged boarding is not simulated. These restrictions do not remove the stop visit from the Tuesday selection count.

Route types are assigned a transport-mode label and a default vehicle capacity from Mobility's route-type table. These are modelling defaults, not fleet observations. An unrecognised route type currently receives the bus label and a capacity of 50 passengers. A missing route short name uses `route_long_name` if that column exists, otherwise `route_id`.

This preparation covers fixed-stop timetables. Station pathways and accessibility restrictions are not used to calculate these transfer connections.

#### Check The Prepared Timetable

Mobility saves six tables: agencies, routes, stops, trips, stop times and transfers. They are stored in Parquet format, which can be read with `pandas.read_parquet`. A JSON summary file whose name ends in `gtfs_router.json` links to those tables and records:

- the selected date and stop-visit totals for candidate Tuesdays;
- each source file, a content checksum used to detect identical feeds, and whether it was retained or skipped;
- the number of trips from each prepared feed running on the selected day;
- counts of removed invalid trips and trips with estimated intermediate times for each prepared feed;
- the frequency and interpolation assumptions.

These preparation counts describe trips before frequency expansion and date selection; the selected trip counts describe the resulting timetable. A feed discarded entirely because it has no usable trips is recorded as skipped, without those detailed preparation counts.

Record the source files, operating dates, selected Tuesday and warnings with the study. Check that important operators and the intended scenario service are represented. Identical feed detection does not remove overlapping trips from different feed versions.

Saved results are reused when their inputs are unchanged. If an additional GTFS file is edited, construct the public-transport objects again so Mobility detects the change. Old RDS timetables from the earlier preparation are not reused. Timetable preparation runs in Python; the following graph calculation still uses R and average travel, waiting and transfer costs.

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
