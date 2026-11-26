# Design: diagnose and exclude parts of GTFS feeds

Status: proposed. This document describes a possible feature; the parameter and
diagnostics below are not implemented yet.

## Problem and goal

A feed can contain a malformed trip, service or route while the rest of its
service is useful. Today some errors identify only the feed or the kind of
problem. The modeller can exclude the whole source or repair a copy, but cannot
ask Mobility to omit a known part of a provider feed. We want clear diagnostics
and explicit, reproducible exclusions, without automatically guessing a repair.

For example, a calendar date of `52020430` is outside the range used by the
current pandas calendar reader. It might be a mistake or an intentional distant
end date. Excluding its service loses every trip using that service; it is not
equivalent to correcting the date. A future date-handling improvement can be
considered separately.

## Proposed user interface

Add an optional `gtfs_exclusions` parameter to
`PublicTransportRoutingParameters`. It maps source identifiers to the original
IDs in that source. Names and IDs below are examples, not actual diagnoses:

```python
gtfs_exclusions = {
    "transport_data_gouv:example": {
        "services": ["bad-service"],
        "routes": ["bad-route"],
        "trips": ["bad-trip"],
    },
}
```

Use the same `provider:resource_id` identifiers as `excluded_gtfs_sources`.
For additional local or generated feeds, use the path shown in diagnostics,
consistent with `gtfs_feed_priority`. Define path normalization once and use
it for matching, reporting and cache identity, including Windows paths. IDs
are exact strings from the original feed, before Mobility adds feed prefixes or
creates frequency departures. Preserve leading zeroes and case.

Lists are sets: normalize order and reject duplicate or empty IDs, unknown keys,
and a source listed both here and in `excluded_gtfs_sources`. An unknown source
or an ID absent from the corresponding original table is an error, with the
source and IDs listed. An ID present in the feed but outside the study area or
inactive in the selected window is still known and may be excluded. Check
against the original tables, not the spatially filtered result. Fail clearly if
the relevant table cannot be read well enough to verify an ID. Define source
validation against the frozen source catalogue, not just feeds selected for
the current area, so a valid exclusion does not become an error when the study
area changes. Exclusions for known but unselected sources have no effect and
are reported as such. If a local path or generated feed is unavailable, report
that separately instead of calling its IDs unknown.

## Removal rules

Exclusions apply to the whole feed, before date selection, spatial filtering,
frequency expansion and overlap detection. Apply the union of the following
rules; a trip affected by more than one rule is removed once.

| Excluded ID | Records removed from the prepared input |
| --- | --- |
| Trip | Every `trips.txt` row with that ID, its stop times and frequency rows, and transfer rules naming that trip. |
| Service | Every calendar and exception row for that service and every trip using it, with that trip's dependent records. |
| Route | Every route row with that ID and every trip using it, with that trip's dependent records; also remove transfer rules naming that route. |

If a duplicated trip, service or route ID is excluded, remove **all** rows
sharing that ID. This interface cannot choose one of several ambiguous rows.
It also cannot remove just one occurrence of a frequency-generated departure:
the original template trip is the unit of exclusion. Do not remove stops or
agencies solely because they were named by removed trips; normal downstream
filtering can discard unused records.

Removing a trip- or route-specific transfer rule removes the rule itself. Do
not widen a remaining restriction or turn an unreadable rule into permission.
Apply existing conservative transfer handling to the remaining rules. An
exclusion can remove all usable service: retain the existing clear failure if
no usable trips remain, and apply `expected_agencies` to the final timetable.

## Processing and cache identity

Calendar filtering must happen on raw string IDs **before** dates are parsed,
including during source metadata extraction. The snapshot's cached service
bounds currently depend on the original calendar. Do not use those bounds to
skip a feed when exclusions may change or cannot validate its coverage. An
implementation may compute exclusion-specific bounds, or conservatively use
unknown bounds and retain the feed for later selection. An unrelated malformed
calendar must still fail; do not silently coerce dates or discard bad rows.

Route and trip exclusions likewise need to be applied before the validations
they are intended to bypass. Filter dependent tables consistently, including
stop times before checking for unknown trip references, and frequency and
transfer rows before their validation. Preserve streaming where practical for
large stop-time files. Still validate the remaining records and fail on
unrelated defects. A broken ZIP, missing table or unreadable identifier column
may prevent selective exclusion entirely.

Normalize and include exclusions in every affected asset's cache identity,
including calendar metadata, source selection where relevant, the timetable,
and downstream assets through their dependencies. Avoid mutating the frozen
source snapshot or original ZIPs. The snapshot records source files, while the
exclusions are model inputs. Changing exclusions must rebuild affected results;
reordering equivalent lists must not. Retain source checksums and deterministic
row ordering. Audit existing snapshot reuse and versioning when implementing;
do not rely on code changes alone to invalidate cached data.

## Diagnostics and advice

For each detected record-level error, report the source identifier, table,
original offending IDs and reason. Where readable, include route names, service
IDs and affected trip counts. Show a bounded sample and total count; provide a
complete machine-readable diagnostic for large sets, even when preparation
fails before the GeoPackage is written. Define a stable output location and
replace it atomically so an old report cannot be mistaken for the current run.
Report generation must not hide the original error if the report cannot be
written. Avoid expensive additional scans when the necessary data is unavailable;
say when counts or details could not be determined.

When a specific exclusion can address the reported check, show a copyable
example and its consequences. Make clear that it removes all matching records,
may remove valid service and is a modeller's decision. Do not suggest an ID
when it is missing, ambiguous or cannot be traced reliably; show the source or
path and direct the user to inspect or repair the feed instead. Preserve the
underlying error and source path, including for custom feeds. Multiple
pathologies may require several iterations; resolving one diagnostic does not
guarantee that the feed is otherwise valid.

Successful runs should record requested exclusions, matched IDs, removal
counts by source and reason, and IDs with no local or selected-day effect in a
source report. Count each removed trip once in totals, while preserving all
applicable reasons. Include original IDs or a linked detailed table, not only
prefixed IDs. Document that counts of original trips differ from generated
frequency departures and that removing service can change the selected Tuesday,
network coverage and modeled results.

## Initial diagnostic coverage

| Existing check | Candidate advice when IDs can be recovered |
| --- | --- |
| Duplicate trip ID, invalid stop sequence, time or frequency | Exclude the original trip; duplicate IDs remove all matching trips. |
| Invalid calendar date or weekday, duplicate service, conflicting exception | Exclude the service and all its trips. |
| Trip references a missing route or service | Exclude the affected trip, or exclude its route/service when identifiable; show the wider effect. |
| Duplicate route ID or overlapping services | Exclude a named route or trip only after the modeller reviews the affected service. Existing feed priority remains available for cross-feed matches. |
| Missing or unreadable tables, broken archive, missing IDs | No reliable record-level suggestion; retain the source-level error or advise repairing the input. |

Existing automatic removal, interpolation and conservative transfer behavior
are not changed by this proposal. Report their effects separately from explicit
exclusions. Detection is not a complete GTFS validity check, and some checks
operate only on retained local trips or the selected day.

## Acceptance checks

- A synthetic feed with an out-of-range calendar date reports its source and
  service ID. Excluding that service allows other services to prepare; a second
  unexcluded invalid service still fails. Exercise both snapshot metadata and
  timetable preparation.
- Duplicate trip IDs, invalid stop sequences, bad frequency intervals, calendar
  conflicts and duplicate route IDs can be excluded at their stated scope;
  unrelated trips remain. Verify duplicate IDs remove every matching row.
- Test dependent stop times, frequencies and trip/route transfer rules, including
  overlapping exclusion reasons and zero remaining service.
- Test original string IDs, custom paths, unknown IDs and sources, known IDs
  outside the area/window, and errors where an ID cannot be recovered.
- Test that changed exclusions rebuild affected caches and change results,
  equivalent ordering reuses identity, and original snapshots and ZIPs remain
  unchanged. Cover snapshot reuse and exclusion-aware calendar bounds.
- Verify diagnostic limits, complete failure reports, unavailable context,
  source-level errors, and successful removal counts. Compare unaffected trips
  and selected dates with a manually cleaned fixture for reproducibility.

## Decisions before implementation

- Confirm the public parameter name and representation, including how generated
  `CustomGTFS` assets receive a stable user-facing identifier.
- Choose the persistent location and format for diagnostics on failed runs,
  and how to avoid collisions between concurrent runs.
- Decide whether to deliver all three exclusion scopes at once or introduce
  them in stages. Each delivered scope needs early filtering, diagnostics and
  cache tests together.
