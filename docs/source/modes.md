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

GTFS is a standard format for public-transport timetables. A GTFS file describes lines, stops, vehicle journeys and the dates when they run.

Mobility turns these files into one timetable for one Tuesday. It keeps journeys serving the study area, checks their times, chooses the Tuesday with the most scheduled stop visits, and prepares connections between stops. The steps below explain what this means for your model.

#### Select Source Files

Provide a `gtfs_reference_date` and a `gtfs_sources_folder` in your routing parameters. Mobility uses them to select available GTFS files for the study area and save a list of those sources with your project. Keep this list so another modeller can use the same source files. It is stored as a small database file in SQLite format; you do not need to edit the database yourself.

For France, Mobility first checks the areas listed by transport.data.gouv.fr, then the area covered by each operator. This avoids downloading files that clearly do not serve the study area.

By default, Mobility uses archived files: saved versions that can be downloaded again for a later run. It warns and skips a source if no archived version is available or if that version is too old. If no usable source remains, the run stops with an error. Setting `use_live_gtfs=True` also allows current files from providers' websites, whose contents may change between runs.

```python
routing_parameters = mobility.PublicTransportRoutingParameters(
    gtfs_reference_date="2026-01-01",
    gtfs_sources_folder="inputs/gtfs_sources",
    max_gtfs_file_age_days=30,
)
```

#### Timetable Terms

These GTFS terms appear in the files and in Mobility's messages:

| Term | Meaning | Example |
| --- | --- | --- |
| Feed | One GTFS ZIP file from an operator or data provider. | A city's bus and tram timetable. |
| Route | Usually a public-transport line. | Bus line 12. |
| Trip (`trip_id`) | One scheduled vehicle journey along a sequence of stops. | Line 12's 08:00 departure. |
| Service calendar (`service_id`) | The operating dates shared by one or more trips. | The dates when the weekday timetable applies. |
| Stop visit | One trip calling at one stop. | The 08:00 bus calling at the station at 08:10. |

A GTFS trip describes a vehicle journey. One bus trip may carry many passengers, each making their own journey.

Each row in `stop_times.txt` is a stop visit. Ten bus trips calling at five stops each represent **50 stop visits**. Repeated calls at the same stop count separately. This count describes the scheduled service available; it does not measure passenger numbers.

#### Keep Trips Serving The Study Area

Mobility keeps stops within the transport zones and a surrounding 10 km area. A trip must have at least two stop visits in this area to be retained. These are visits, so they need not be at two different stops. Stops with missing or invalid coordinates are removed.

Mobility checks times using the trip's full stop sequence, then removes stops outside the area. It remembers the original first departure time, even if the trip starts outside the study area. This is needed to place repeated departures at the correct times.

Files with identical contents in the timetable tables used by preparation are included once. Different files from the same operator remain separate, even if some journeys appear in both. Check for this when adding your own files.

Mobility adds a source number to GTFS identifiers. For example, trip `t1` from the first file becomes `1-t1`. Another file can then use `t1` without the two journeys being confused.

#### Check And Prepare Times

GTFS allows a day's timetable to continue after midnight. For example, `25:10:00` means 01:10 the following morning, as part of the previous day's service. Mobility preserves this meaning and stores times as seconds from the start of that service day. It does not convert times between different time zones.

Mobility estimates a missing intermediate time from the known times around it. For example, if stops 1 and 3 have times of 08:00 and 08:10, the missing time at stop 2 becomes 08:05. This method is called **linear interpolation**. It uses the stop sequence numbers (`stop_sequence`), so equally spaced numbers receive equal shares of the time, even if the distances between stops differ.

Mobility removes a whole vehicle trip and issues a warning if:

- an arrival or departure time is missing at its original first or last stop;
- it departs from a stop before arriving there;
- it reaches the next stop before leaving the previous one.

Other invalid data stop preparation with an error. Examples include a time written in the wrong format, two stops with the same sequence number in a trip, a trip referring to a route that does not exist, or an invalid calendar value. Correct the source file or obtain a corrected version before running again.

#### Prepare Repeated Departures

Some feeds describe a service as, for example, "a bus every 10 minutes" in `frequencies.txt`. Mobility creates individual departures from this information. An interval from 08:00 to 09:00 with a 10-minute gap gives departures at **08:00, 08:10, 08:20, 08:30, 08:40 and 08:50**. The end time is excluded.

The gap between departures is called the **headway**. GTFS stores it in seconds as `headway_secs`, with the interval given by `start_time` and `end_time`. Each generated departure keeps the original trip's travel times and time spent stopped. Intervals for the same trip must not overlap.

When `exact_times` is 0 or omitted, the source gives a headway without promising exact departure times. Mobility still places departures at regular intervals and issues a warning about this approximation.

#### Choose One Tuesday

Mobility applies the weekly calendars in `calendar.txt` and the additions and cancellations in `calendar_dates.txt` on their actual dates. A feed may use dated exceptions without a weekly calendar. Dates are never shifted to make feeds overlap.

After the preceding steps, Mobility counts stop visits on each Tuesday covered by the retained timetables. It chooses the Tuesday with the largest total. If several Tuesdays have the same total, it chooses the earliest.

For example, if two Tuesdays have 1,000 and 1,200 stop visits, Mobility selects the second. It compares actual scheduled visits, rather than how many calendar names or lines are listed in a file.

**The choice uses the whole day's timetable**, even if your model calculates travel costs only during the morning peak. It also includes stop visits where passengers cannot board. Check that the selected date suits your study: the busiest Tuesday under this rule may not represent a typical day or your busiest morning peak.

There are two different dates to distinguish:

| Date | What it controls |
| --- | --- |
| `gtfs_reference_date`, supplied by you | Which archived source files Mobility selects. |
| Selected Tuesday, calculated by Mobility | Which operating day is used to build the model timetable. |

The selected Tuesday need not be close to the reference date. A feed with no service on that Tuesday contributes no trips and produces a warning. If no Tuesday has usable service, preparation stops with an error.

Additional scenario files must use the intended operating dates. For example, a September-only timetable will not contribute trips to a July Tuesday. Two successive files from one operator contribute together only if their actual operating dates overlap.

Mobility keeps the trips running on the selected Tuesday, together with their lines, operators, stop visits and stops. You can use `expected_agencies` to require named operators to be present. Each name must match all or part of an operator name with service that day; capitalisation does not matter.

#### Prepare Transfers And Service Information

Mobility adds walking connections in both directions between different retained stops within 200 metres. The assumed time is:

```text
transfer time in seconds = 31 + 1.125 × straight-line distance in metres
```

For stops 100 metres apart, this gives 143.5 seconds, or about 2 minutes 24 seconds. These connections do not follow footpaths or account for barriers such as fences or rivers.

The source file may also supply transfer rules in `transfers.txt`, including connections longer than 200 metres. Mobility uses a stated minimum time when one is supplied; otherwise it uses the formula above. A rule for a whole station applies to its retained platforms.

When calculating travel costs, Mobility applies these priorities:

- Rules supplied in the file take priority over walking connections added by Mobility.
- More targeted rules take priority: a rule for changing from line A to line B comes before a rule for all changes from line A, which comes before a general rule between the stops.
- If equally targeted rules disagree, a rule forbidding the transfer wins. Otherwise, Mobility uses the longest minimum transfer time.

Some transfer arrangements are not fully represented:

- A timed connection (`transfer_type=1`) does not make the model hold the connecting vehicle. The calculation uses its timetable and the minimum transfer time.
- Trip-specific rules and transfer types 4 and 5 (remaining in the same vehicle, or prohibiting that continuation) are unsupported and raise an error when their stops are used on the selected day.

Boarding means getting on a vehicle; alighting means getting off. Mobility preserves the corresponding GTFS fields, `pickup_type` and `drop_off_type`. Only value 0 allows the action in the model. Values 1, 2 and 3 prevent it, including services that require a booking or an arrangement with the driver. The stop visit still counts when choosing the Tuesday.

Mobility assigns each route a mode, such as bus or tram, and a default vehicle capacity from its route-type table. These capacities are assumptions, rather than counts of seats or standing places in the operator's actual vehicles. An unrecognised route type currently receives the bus label and a capacity of 50 passengers.

If a route has no short name, Mobility uses `route_long_name` if that column exists, otherwise `route_id`.

This preparation covers services with listed stops and timetables. The transfer calculation does not use detailed walking routes inside stations or accessibility restrictions.

#### Check The Prepared Timetable

Mobility saves six tables: agencies, routes, stops, trips, stop times and transfers. They are stored in Parquet format, which can be read with `pandas.read_parquet`. A JSON summary file whose name ends in `gtfs_router.json` links to those tables and records:

- the selected date and stop-visit totals for candidate Tuesdays;
- each source file, whether it was used or skipped, and a code calculated from its contents to identify identical files;
- the number of trips from each prepared feed running on the selected day;
- counts of removed invalid trips and trips with estimated intermediate times for each prepared feed;
- the frequency and interpolation assumptions.

The removed-trip and estimated-time counts refer to the original vehicle trips, before repeated departures are created and a Tuesday is chosen. The selected-trip count refers to the final timetable. For a file skipped because none of its trips could be used, the summary records that outcome without the detailed counts.

Before using the public-transport results, check:

1. **The date:** does the chosen Tuesday represent the period you want to study?
2. **The operators:** are the important operators present, and do warnings explain any missing ones?
3. **The service:** were many trips removed or times estimated? Do your additional scenario files actually run on that date?
4. **Overlapping files:** could different versions of an operator's timetable count the same journeys twice?
5. **Transfers:** are straight-line walking times reasonable at the important interchanges?

Keep the source files, dates, summary and relevant warnings with the study so you can explain the service assumptions later.

Mobility reuses saved results when the inputs are unchanged. After editing an additional GTFS file, rerun the Python code that creates your `PublicTransportMode`, so Mobility detects the new contents.

Timetable preparation runs in Python. The next step, building the public-transport network for cost calculations, still uses R. Saved timetables from the older preparation are rebuilt.

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
