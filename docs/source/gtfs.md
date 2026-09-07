(gtfs-data-preparation)=
# GTFS Data Preparation

GTFS is a standard format for public-transport timetables. A GTFS file describes lines, stops, vehicle journeys and the dates when they run.

Mobility turns these files into one timetable for one Tuesday. It keeps journeys serving the study area, checks their times, chooses the Tuesday with the most scheduled stop visits, and prepares connections between stops. The steps below explain what this means for your model.

## Select Source Files

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

## Timetable Terms

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

## Keep Trips Serving The Study Area

Mobility keeps stops within the transport zones and a surrounding 10 km area. A trip must have at least two stop visits in this area to be retained. These are visits, so they need not be at two different stops. Stops with missing or invalid coordinates are removed.

Mobility checks times using the trip's full stop sequence, then removes stops outside the area. It remembers the original first departure time, even if the trip starts outside the study area. This is needed to place repeated departures at the correct times.

Files with identical contents in the timetable tables used by preparation are included once. Different files from the same operator remain separate, even if some journeys appear in both.

Mobility adds a source number to GTFS identifiers. For example, trip `t1` from the first file becomes `1-t1`. Another file can then use `t1` without the two journeys being confused.

## Check And Prepare Times

GTFS allows a day's timetable to continue after midnight. For example, `25:10:00` means 01:10 the following morning, as part of the previous day's service. Mobility preserves this meaning and stores times as seconds from the start of that service day. It does not convert times between different time zones.

Mobility estimates a missing intermediate time from the known times around it. For example, if stops 1 and 3 have times of 08:00 and 08:10, the missing time at stop 2 becomes 08:05. This method is called **linear interpolation**. It uses the stop sequence numbers (`stop_sequence`), so equally spaced numbers receive equal shares of the time, even if the distances between stops differ.

Mobility removes a whole vehicle trip and issues a warning if:

- an arrival or departure time is missing at its original first or last stop;
- it departs from a stop before arriving there;
- it reaches the next stop before leaving the previous one.

Other invalid data stop preparation with an error. Examples include a time written in the wrong format, two stops with the same sequence number in a trip, a trip referring to a route that does not exist, or an invalid calendar value. Correct the source file or obtain a corrected version before running again.

## Prepare Repeated Departures

Some feeds describe a service as, for example, "a bus every 10 minutes" in `frequencies.txt`. Mobility creates individual departures from this information. An interval from 08:00 to 09:00 with a 10-minute gap gives departures at **08:00, 08:10, 08:20, 08:30, 08:40 and 08:50**. The end time is excluded.

The gap between departures is called the **headway**. GTFS stores it in seconds as `headway_secs`, with the interval given by `start_time` and `end_time`. Each generated departure keeps the original trip's travel times and time spent stopped. Intervals for the same trip must not overlap.

When `exact_times` is 0 or omitted, the source gives a headway without promising exact departure times. Mobility still places departures at regular intervals and issues a warning about this approximation.

## Choose One Tuesday

Mobility applies the weekly calendars in `calendar.txt` and the additions and cancellations in `calendar_dates.txt` on their actual dates. A feed may use dated exceptions without a weekly calendar. Dates are never shifted to make feeds overlap.

After the preceding steps, Mobility counts stop visits on each Tuesday covered by the retained timetables. It chooses the Tuesday with the largest total. If several Tuesdays have the same total, it chooses the earliest.

For example, if two Tuesdays have 1,000 and 1,200 stop visits, Mobility selects the second. It compares actual scheduled visits, rather than how many calendar names or lines are listed in a file.

**The choice uses the whole day's timetable**, even if your model calculates travel costs only during the morning peak. It also includes stop visits where passengers cannot board. The busiest Tuesday under this rule may differ from a typical day or the busiest morning peak.

There are two different dates to distinguish:

| Date | What it controls |
| --- | --- |
| `gtfs_reference_date`, supplied by you | Which archived source files Mobility selects. |
| Selected Tuesday, calculated by Mobility | Which operating day is used to build the model timetable. |

The selected Tuesday need not be close to the reference date. A feed with no service on that Tuesday contributes no trips and produces a warning. If no Tuesday has usable service, preparation stops with an error.

Additional scenario files must use the intended operating dates. For example, a September-only timetable will not contribute trips to a July Tuesday. Two successive files from one operator contribute together only if their actual operating dates overlap.

Mobility keeps the trips running on the selected Tuesday, together with their lines, operators, stop visits and stops. You can use `expected_agencies` to require named operators to be present. Each name must match all or part of an operator name with service that day; capitalisation does not matter.

## Connections Between Stops

An ordinary transfer has two parts: **whether the connection is allowed**, and **the minimum time needed to make it**. The rules can apply to all lines at two stops, or only to particular lines.

Mobility starts by allowing walking connections in both directions between different stops within 200 metres. It estimates the minimum connection time as:

```text
minimum connection time in seconds = 31 + 1.125 * straight-line distance in metres
```

For stops 100 metres apart, this gives 143.5 seconds, or about 2 minutes 24 seconds. The estimate does not follow footpaths or account for barriers such as fences or rivers.

Different lines sharing the same stop can also connect, with the same formula giving a minimum of 31 seconds. Declared rules can replace this default or forbid the connection.

The operator can supply additional information in `transfers.txt`:

| Information in the file | How Mobility uses it |
| --- | --- |
| A recommended connection (`transfer_type=0`) | Allows the connection, using the stated minimum time or the distance estimate. |
| A minimum connection time (`transfer_type=2`) | Uses the required `min_transfer_time`, in seconds. |
| A forbidden connection (`transfer_type=3`) | Removes the connection. |

These rules take priority over connections added by Mobility and can also connect stops more than 200 metres apart. A rule for a whole station applies to its platforms. Station walking routes and accessibility restrictions are not used to calculate connection times.

A rule naming particular lines takes priority over a general rule between the stops. For example, an exception allowing a change from line A to line B can override a general prohibition. A rule naming both lines comes before one naming just one line. If rules at the same level disagree, a prohibition wins; otherwise Mobility uses the longest minimum connection time.

## Time Spent Transferring

The minimum connection time determines which departures a passenger can reach. A departure is reachable when it leaves at or after the arrival time plus this minimum.

For example, a passenger arrives at 08:00 and needs five minutes to transfer. They cannot catch an 08:03 departure. They can catch a departure at 08:05 or later. If the next departure is at 08:10, the **total transfer time is ten minutes**: five minutes to make the connection and five minutes waiting.

The model averages this full time, from arrival to the next reachable departure, for each connection between lines and stops. The perceived transfer cost applies `transfer_time_coeff` to that average. Connections with an average transfer time of 20 minutes or more are removed.

## Special Operating Arrangements

Some GTFS transfer information describes how vehicles operate, beyond walking and waiting between stops:

| Arrangement | Current treatment |
| --- | --- |
| A connecting vehicle is expected to wait (`transfer_type=1`). | Mobility uses the timetable and minimum connection time. It does not simulate holding the vehicle. |
| A rule applies to particular vehicle journeys (`from_trip_id` or `to_trip_id`). | The affected connections are omitted with a warning; timetable preparation continues. |
| Passengers stay aboard as a vehicle continues on another trip, or that continuation is prohibited (`transfer_type=4` or `5`). | The affected connections are omitted with a warning; timetable preparation continues. |

The model calculates average costs between lines and stops, so it cannot reliably apply rules to individual vehicle journeys. Its fallback removes all connections between the affected stops in the stated direction, including added walking connections and other rules for that pair. For station rules, it removes the corresponding platform connections. This can exclude transfers on other lines too. The reverse direction is kept unless another rule excludes it.

Rules for stops or explicitly named lines absent from the selected timetable have no effect. Unusable transfer values, such as a missing required minimum time or a negative time, use the same omission fallback. Warnings identify the affected stops and the reason; they do not stop preparation.

## Boarding, Alighting And Vehicle Information

Boarding means getting on a vehicle; alighting means getting off. These permissions are separate from the transfer rules. Mobility preserves the GTFS fields `pickup_type` and `drop_off_type`. For each stop visit, value 0 permits the action; values 1, 2 and 3 restrict it, including services that require a booking or an arrangement with the driver. Restricted stop visits still count when choosing the Tuesday.

The graph combines journeys by line and stop to calculate average travel costs. It allows boarding at a line's stop if at least one journey permits boarding there within the modelled time window. It applies the same rule separately to alighting. For example, if boarding is forbidden at 08:10 but allowed at 08:30 on the same line, that stop remains available for boarding in an 08:00–09:00 modelled period. This average-cost graph does not enforce every individual journey's restriction. A journey outside the modelled period cannot make the stop available within it.

Mobility assigns each route a mode, such as bus or tram, and a default vehicle capacity from its route-type table. These capacities are assumptions, rather than counts of seats or standing places in the operator's actual vehicles. An unrecognised route type currently receives the bus label and a capacity of 50 passengers.

If a route has no short name, Mobility uses `route_long_name` if that column exists, otherwise `route_id`.

This preparation covers services with listed stops and timetables.

## Saved Timetable, Stops And Lines

Mobility saves six tables: agencies, routes, stops, trips, stop times and transfers. They are stored in Parquet format, which can be read with `pandas.read_parquet`. A GeoPackage whose name ends in `gtfs_stops_and_lines.gpkg` contains the stops and lines, plus three tables describing preparation:

- **`summary`** records the selected date, preparation version and the date-selection, frequency and interpolation assumptions.
- **`sources`** records each source file, whether it was used or skipped, a code calculated from its contents to identify identical files, and counts of selected trips, removed invalid trips and trips with estimated times.
- **`tuesdays`** records the stop-visit total for each candidate Tuesday.

The removed-trip and estimated-time counts refer to the original vehicle trips, before repeated departures are created and a Tuesday is chosen. The selected-trip count refers to the final timetable. For a file skipped because none of its trips could be used, the summary records that outcome without the detailed counts.

Open the GeoPackage in QGIS to inspect the merged timetable for the selected Tuesday:

- **`stops`** contains every retained stop, its coordinates, number of stop visits and number of lines serving it.
- **`lines`** contains one directed segment per line and pair of consecutive stops, with the line and operator information and the number of vehicle journeys using that segment. Opposite directions and different branches remain separate.

Both layers record the selected date and describe the whole day's service, including restricted stop visits. The line geometry connects retained stops with straight segments; it does not follow roads or tracks and does not use `shapes.txt`. These layers show the prepared timetable, before the graph applies the modelled time window and boarding or transfer restrictions.

This standalone example prepares a timetable and a map of its stops and lines in a project folder. It needs no population, transport modes or simulation. The municipality and date below are example inputs; replace them with those for your study. The first run downloads the required geographical data and available archived GTFS files:

```python
import mobility
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter

mobility.set_params(project_data_folder_path="my-project", r_packages=False)
# Example study area: within 50 km of Rennes, France.
zones = mobility.TransportZones("fr-35238", radius=50.0, backend="python")
sources = mobility.GTFSSources(
    gtfs_reference_date="2026-07-24",
    gtfs_sources_folder="my-project/inputs/gtfs_sources",
    countries=["fr"],
    transport_zones=zones,
)
router = GTFSRouter(transport_zones=zones, gtfs_sources=sources)

# Prepare the timetable and its map if they are not already saved.
paths = router.get()
gpkg_path = paths["stops_and_lines"]
print(gpkg_path)
```

Open the printed path in QGIS, or read the layers in Python:

```python
import geopandas as gpd

stops = gpd.read_file(gpkg_path, layer="stops")
lines = gpd.read_file(gpkg_path, layer="lines")
summary = gpd.read_file(gpkg_path, layer="summary")
sources = gpd.read_file(gpkg_path, layer="sources")
tuesdays = gpd.read_file(gpkg_path, layer="tuesdays")
```

Change the municipality code, radius and reference date to inspect another study area. This example prepares official source timetables only. To include a custom timetable, pass `additional_gtfs_files=["path/to/your-feed.zip"]` when creating `GTFSRouter`. Calling `router.get()` prepares the timetable and map without building the travel-cost graph.

Mobility reuses saved results when the inputs are unchanged. After editing an additional GTFS file, rerun the Python code that creates your `PublicTransportMode`, so Mobility detects the new contents.

Timetable preparation runs in Python. The next step, building the public-transport network for cost calculations, still uses R. Saved timetables from the older preparation are rebuilt.

(gtfs-scenario-timetable)=
## Create A Small Scenario GTFS Feed

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
