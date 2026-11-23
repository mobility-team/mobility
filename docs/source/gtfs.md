(gtfs-data-preparation)=
# GTFS preparation

Mobility combines GTFS feeds into one Tuesday timetable for the study area.

## Freeze sources

```python
routing_parameters = mobility.PublicTransportRoutingParameters(
    gtfs_reference_date="2026-09-01",
    gtfs_sources_folder="inputs/gtfs_sources",
)
```

The reference date selects archives published on or before that date. On the first run, Mobility records their URLs and checksums in a SQLite file. **Commit this file** so that later runs use the same sources. Mobility can download a missing ZIP again from its recorded URL, but keeping a backup is useful if the archive later disappears.

The snapshot also records each feed's service dates, allowing Mobility to skip feeds that cannot run during the study period. Older snapshots are enriched into a new SQLite file without replacing the original; review and commit the new file if you want to use it. In France, Mobility searches the full archive history of each discovered dataset, although datasets removed entirely from the catalogue may be missing. In Switzerland, it searches the relevant timetable years.

If an archive is missing, Mobility stops and lists the affected feeds. You can omit a feed deliberately by adding its ID to `excluded_gtfs_sources`. To refresh a snapshot or replace an incompatible one, choose a new sources folder.

## Choose service dates

By default, Mobility looks at the 28 days starting on the reference date and selects the Tuesday with the most scheduled stop visits, taking calendar exceptions into account. This describes the level of service, not passenger demand. Set both `gtfs_service_start_date` and `gtfs_service_end_date` to use another window, or set both to the same Tuesday to model that particular day.

An old archive can still contain valid service, so Mobility does not limit archive age by default. You can set `max_gtfs_file_age_days` if you need a limit, and use `expected_agencies` to require particular operators on the selected day.

## Inspect the result

`router.get()["stops_and_lines"]` returns a GeoPackage you can open in QGIS:

- `stops` and `lines`: retained stops and straight segments, with whole-day service counts.
- `summary`: selected Tuesday and coverage warnings.
- `sources`: candidate feeds, checksums, selected-trip counts and exclusion reasons. Feeds filtered out earlier using their service dates remain listed in SQLite.
- `tuesdays`: service counts for candidate Tuesdays.
- `trip_overlaps`: cross-feed trip pairs with matching ordered stops and times,
  when any candidates exist. Trip IDs are prefixed by the feed number shown in
  `sources`. The table records the maximum stop distance and time difference.
- `within_feed_overlaps`: automatically removed same-feed trip IDs and the
  trip retained for each, when any exist.
- `headway_overlaps`: matching cross-feed headway intervals and priority decisions.
- `within_feed_headway_overlaps`: automatically removed duplicate headway intervals.

The counts cover the whole prepared day, before the routing graph applies its time restrictions.

## Deal with overlapping feeds

A regional feed can repeat trips already present in a city feed, causing the model to count the same service twice. Mobility checks for overlaps after selecting the Tuesday to model, so duplicates can still influence which Tuesday is chosen when the window contains more than one.

When two feeds appear to contain the same trips, Mobility stops and reports the feed IDs, matching-trip counts and agencies listed in each feed. The agency inventory covers the whole feed, including any agencies outside the study area.

Review the feeds, then tell Mobility which one to prefer. For example:

```python
routing_parameters = mobility.PublicTransportRoutingParameters(
    gtfs_reference_date="2026-10-06",
    gtfs_sources_folder="inputs/gtfs_sources",
    gtfs_feed_priority=["transport_data_gouv:80594", "transport_data_gouv:83634"],
)
```

The first feed has the highest priority. In this example, Mobility removes matching trips from the second feed but retains services found only there. List every feed named in the error, in your preferred order; for an additional GTFS file, use the path shown in the error. Changing the order rebuilds the cached timetable.

One trip can match several trips from another feed. Mobility pairs them one-to-one, so a single preferred trip cannot remove several trips from the other feed. It chooses as many pairs as possible and prefers closer matches. The `trip_overlaps` table records the chosen pairs and removed trips, while `sources` gives the number removed from each feed.

## How trips are compared

For scheduled trips, Mobility compares the stops kept in the study area and its buffer. Trips are candidates when they have the same number of stops in the same order, corresponding stops are within 10 metres, departure times are identical, and pickup and drop-off rules match. Route names and IDs are not used for matching across feeds.

Arrival times may differ even for the same vehicle. For example, an aggregated feed may record arrival and departure at the same time, while the original feed records an earlier arrival. The overlap error reports the largest arrival difference for each feed pair, and `trip_overlaps` records it for each candidate. Since arrival times affect travel times and connections, review these differences and rank the feed whose full timetable you want to keep first.

This check can miss a copy if one feed has an extra or missing stop. It can also match two separate vehicles, or trips that follow the same path locally but diverge outside the area. Transfer rules are not compared, so inspect the result before relying on it; a count of matching pairs is not necessarily a count of duplicate trips.

Within one feed, Mobility automatically removes scheduled trips only when their route, stop IDs, times, and pickup and drop-off rules match exactly. It keeps the trip with the first ID in alphabetical order and warns which feed was affected. Since separate vehicles can have identical schedules, review the warning and the `within_feed_overlaps` table when the number of vehicles matters.

## Services described by frequency

Some feeds describe a service as running every few minutes rather than giving exact departures. Mobility compares these services by their stop pattern, relative departure times, operating window and *headway*—the time between vehicles. The window is measured at the first stop kept inside the study area and buffer. Arrival-time differences are reported for these candidates too.

When two feeds describe the same window and headway, Mobility asks for a feed priority and removes the lower-priority interval. It removes identical intervals within one feed with a warning. If matching patterns have overlapping windows but different times or headways, Mobility stops for review rather than splitting the intervals. Resolved cases appear in the `headway_overlaps` and `within_feed_headway_overlaps` tables.

Mobility does not yet compare a frequency interval with individual scheduled trips, or check partially overlapping intervals within one feed. It warns when frequency-based services are present. GTFS records marked `exact_times=1` instead describe exact scheduled departures, so Mobility compares them as individual trips.

## How trips become a graph

Mobility groups trips on the same route when they visit the same remaining
stops in the same order. For example, two trips that join at a stop and then
follow the same route share their departures and estimated headway from that
stop. Arrivals into the stop remain separate when the trips came from different
stops. If a trip returns to a stop, each visit has its own node. Passengers
can transfer between patterns when the stop and GTFS transfer rules allow it.

This graph is an approximation, not a full timetable router. Travel times are
averaged across trips with the same pattern, so a path does not necessarily
correspond to one vehicle journey from start to finish. Trips that share an
initial section but later go to different stops remain separate. The model
therefore does not combine their frequencies when a passenger could take either
trip along that shared section.

Representative buildings are connected to the nearest graph vertices using
straight-line distances and the configured access and egress speeds. These
connections are included in the leg distances and travel times. The final
connection is also included when checking the target arrival time, rounded to
the graph's one-minute time steps. The complete
access and egress legs must each fit their configured time limit. Mobility
checks the selected graph path; it does not search for another path if that
path exceeds a limit after adding a building connection.

(gtfs-scenario-timetable)=
## Add a scenario feed

```python
builder = mobility.GTFSBuilder(
    agency_id="test", agency_name="Test Agency",
    route_id="line", route_short_name="T1", route_type="bus", service_id="service",
)
builder.add_stops({"a": [6.15, 46.21], "b": [6.19, 46.25]})
builder.add_line([("a", "b", 27 * 60)],
                 start_time=6 * 3600, end_time=9 * 3600, period=15 * 60)
custom_gtfs = builder.build_asset()
```

Pass the result as `additional_gtfs_files=[custom_gtfs]`. The generated service runs daily from 2000 through 2100, and its ZIP is cached from the builder's inputs. Later edits to the builder do not change an existing asset.

You can also pass paths to external ZIPs. After editing one of those files, recreate the mode so Mobility can hash the new contents. The builder's `write_zip()` and `write_project_zip()` methods remain available when you need to write a ZIP directly.
