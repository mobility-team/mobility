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

The reference date selects archives published by that date. The first run creates a SQLite snapshot containing their URLs and checksums. **Commit this SQLite** to reuse the same sources on later runs. Missing ZIPs are downloaded from the frozen URLs and checked; keep ZIP backups if you need to run without those archives remaining online.

Each downloaded feed also caches its service dates in a JSON sidecar. SQLite stores these bounds to filter expired feeds before attaching them to the router; unknown dates remain candidates. Older snapshots are enriched into a new SQLite file using their frozen sources, without replacing the original—commit the new file after reviewing it.

France uses complete archive histories within discovered datasets; Switzerland uses the relevant timetable years. Entirely removed French datasets may no longer be discoverable when creating a historical snapshot.

Missing archives fail with an error listing the affected feeds. To deliberately omit them, copy the suggested `excluded_gtfs_sources` into your parameters. Existing snapshots are never silently replaced: use a new folder for a deliberate refresh or an incompatible snapshot.

## Choose service dates

By default, Mobility selects the Tuesday with the most scheduled stop visits in the 28 days starting on the reference date, applying calendar exceptions. This measures scheduled service, not passenger demand. To change the window, set both `gtfs_service_start_date` and `gtfs_service_end_date`.

Archive age is unrestricted by default: an old ZIP can still contain valid service. Set `max_gtfs_file_age_days` only if you need an age limit. `expected_agencies` can require particular operators on the selected day.

## Inspect the result

`router.get()["stops_and_lines"]` returns a GeoPackage you can open in QGIS:

- `stops` and `lines`: retained stops and straight segments, with whole-day service counts.
- `summary`: selected Tuesday and coverage warnings.
- `sources`: candidate feeds, checksums, selected-trip counts and exclusion reasons. Feeds filtered out earlier using their service dates remain listed in SQLite.
- `tuesdays`: service counts for candidate Tuesdays.

These counts describe the prepared day, before the routing graph's time restrictions. Possible overlapping feeds are reported for review.

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

Pass `additional_gtfs_files=[custom_gtfs]`. Generated feeds run daily from 2000 through 2100. The ZIP is generated and cached from the builder's inputs. Later builder edits do not alter an existing asset.

External ZIP paths remain supported. Recreate the mode after editing an external file so its contents are rehashed. `write_zip()` and `write_project_zip()` still write files directly.
