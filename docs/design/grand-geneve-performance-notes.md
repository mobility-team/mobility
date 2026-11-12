# Grand Genève performance notes

These measurements are diagnostic runs, not forecasts for a full experiment.
The cache-only probe is `tests/manual/inspect_grand_geneve_execution.py`; it
refuses to download missing inputs. It logs the actual run settings and the
trace location. Summarize a trace with:

```powershell
pixi run python tests/manual/report_simulation_trace.py <trace.jsonl>
```

The trace samples process RSS. A peak recorded inside an operation is the peak
of the whole process while that operation was active; it is not memory owned by
that operation. Parent and child timings overlap. The report's self times
subtract direct child intervals, and its inclusive times must not be added.

## Existing five-step baseline

The earlier run had two scenarios, one seed, five steps, six activity draws,
one destination draw, three mode sequences, and up to three search batches.
It used the current cached population and disabled additional scenario GTFS
feeds. It predates direct inner flow aggregation and the latest index changes.
It took 1,053 seconds and its peak process RSS was 38.45 GiB. The inner-loop
wall time and peak RSS by step were:

| Step | Inner-loop seconds | Peak process RSS, GiB |
| --- | ---: | ---: |
| 1 | 50.2 | 7.75 |
| 2 | 117.4 | 14.35 |
| 3 | 185.2 | 21.08 |
| 4 | 245.8 | 27.19 |
| 5 | 279.3 | 31.94 |

Across the run, inner-loop self time included stable-index extension (137 s),
time-sequence indexing outside its child index calls (69 s), allocated-step
expansion (58 s), and compact candidate grouping (48 s). The peak occurred
in an outer behavioral update, during allocated-step expansion. These figures
identify work to inspect; they do not predict additive savings.

## Candidate-history expansion

The outer update used to expand each context's candidate steps to every demand
unit and then compact that full table. The newer path first checks whether the
context history can be represented losslessly as shared steps and memberships;
if it can, only memberships are expanded. It falls back to the old path when
the representation is not valid or is not smaller. Saved checkpoint format
and candidate IDs remain unchanged.

The first controlled comparison used two scenarios and steps, one seed, two
activity draws, one destination draw, one mode sequence, and two search
batches. Both runs used tracing and disabled additional scenario GTFS feeds.
The reference took 93.07 s and peaked at 10.24 GiB RSS; early compaction took
87.18 s and peaked at 10.50 GiB. The largest difference in mode trip counts
was 2.8e-9 represented trips. This is one comparison; the RSS peak occurred
during input preparation, so it does not establish a candidate-history memory
saving. A trace-disabled repeat took 99.06 s; normal run-to-run variation is
larger than any instrumentation conclusion from this single pair.

A larger paired comparison used three steps, three activity draws, one
destination draw, two mode sequences and two batches, with the same two
scenarios and one seed. The reference took 169.30 s and peaked at 10.80 GiB;
early compaction took 154.01 s and peaked at 10.42 GiB. All three outer
updates used early compaction, and mode trip counts differed by at most
1.9e-9 represented trips. The highest reference RSS was during an outer
update; the new run's overall peak was during setup. In these single runs,
outer asset self time was 17.8 s versus 12.9 s; other stages also varied, so
the total wall-time difference cannot all be attributed to compaction.

The comparison uses `MOBILITY_PROBE_REFERENCE_EXPAND=1` to restore the old
expansion for the diagnostic. `MOBILITY_PROBE_STEPS`,
`MOBILITY_PROBE_ACTIVITIES`, `MOBILITY_PROBE_DESTINATIONS`,
`MOBILITY_PROBE_MODES`, `MOBILITY_PROBE_BATCHES`, and
`MOBILITY_PROBE_SCENARIOS` control its workload; `MOBILITY_PROBE_REPLICATION`
selects one of the script's seeds. Set `MOBILITY_PROBE_TRACE=0`
to disable tracing. `MOBILITY_PROBE_REAL_GTFS=1` retains scenario GTFS inputs;
otherwise the probe strips them. The latter is not a complete comparison of
the public transport scenarios.

The probe normally refuses to rebuild transit inputs. Set
`MOBILITY_PROBE_BUILD_LOCAL_TRANSIT=1` to permit the timetable and public
transport and intermodal graphs to be built using already cached feed assets. Missing source
or feed assets still stop the probe; the probe does not download them. This
option can rebuild project transit caches, just as a normal scenario run can.
The earlier probes set a 5 GB managed temporary-file limit; limits are now
opt-in through `MOBILITY_PROBE_TEMP_GB` and
`MOBILITY_PROBE_MEMORY_LIMIT_GIB`. The cached Swiss GTFS archive contains about
2.48 GiB of uncompressed files, including a 2.02 GiB stop-times table; it
needs room alongside the simulation's other temporary files.

## All-scenario setup check

A minimal cache-only run included all five declared scenarios (`default`,
`ljls`, `mouettes`, `l272`, and `carbon_tax`) with their scenario GTFS
definitions, one step, one seed, one activity draw, one destination draw,
one mode sequence and one search batch. It completed in 55.90 s with a
10.32 GiB process RSS peak and 4,024 shared candidate IDs. The generated GTFS
ZIPs were written under a temporary probe directory, not the project data
directory. The probe now uses a stable temporary feed directory between runs,
because the router cache identity includes feed paths. The scenario changes start later, so this checks configuration and
initial shared execution only; it does not validate changed networks, assess
result stability, or establish that 20 steps fit in memory.

An attempt to extend that minimal run to six steps passed the first four but
stopped at the first changed transit scenario because `GTFSRouter` needed to
be rebuilt and the default probe forbids it. At that point the sampled process
RSS peak was 8.68 GiB; the run did not complete, so that is not the peak for
the requested six steps.
Allowing local transit preparation reached the cached Swiss feed but stopped
when its extraction exceeded the probe's 5 GB temporary-file limit. No feed
download occurred. Its sampled RSS peak was 7.94 GiB before the run stopped.
A further attempt with a 10 GB limit prepared the local timetable and transit
graph, then stopped because the intermodal graph was not on the probe's local
build allowlist. Its sampled RSS peak was 10.53 GiB and its managed temporary
files peaked at 5.49 GiB; it also did not complete six steps. A subsequent
attempt built the intermodal graph and stopped at the derived public-transport
travel-cost table. The allowlist now includes both local assets, while source
downloads remain blocked.

With the local derived assets allowed, the minimal all-scenario run completed
six steps in 696.45 s. Python-process peak RSS was 13.08 GiB and sampled managed
temporary files peaked at 5.49 GiB. The trace did not include routing child
process memory; subsequent probes sample the process tree separately, and that
sum may count shared pages more than once. Inner-loop times by step were 11.9,
29.8, 31.5, 33.8, 457.4, and 32.1 s. Step five included changed transit input
preparation. Some transit assets had already been prepared by the interrupted
attempts, so this is neither a fully cold nor a fully warm measurement.

The final shared pool contained 22,760 physical candidate IDs. All five
scenarios produced step-six results. For example, default and carbon-tax car
trip counts were approximately 4,486,282 and 4,444,685 respectively. One seed,
one activity draw, one destination draw, one mode sequence and one batch are
too little to treat these as stable effect estimates or to assert convergence.

Repeating exactly those settings with the resulting transit caches warm took
247.44 s. The Python trace peak was 11.88 GiB; the two-second sampled sum of
Python and child-process RSS peaked at 11.25 GiB. These sampled peaks use
different intervals and the process-tree sum can double-count shared pages, so
they are not directly comparable. Managed temporary files peaked at 3.10 GiB.
The six inner-loop times were 9.5, 25.0, 27.0, 28.9, 33.9 and 25.3 s. The
candidate-pool counts matched the previous run at every step, and the largest
absolute trip-count difference across all scenarios, modes and steps was
9.4e-9. The previous run was partially warm because of interrupted preparation;
the approximately 449 s difference is not a fully cold-start benchmark.

A warm run with the same minimal effort and replication 1 completed in
299.22 s, with an 11.90 GiB Python trace peak and an 11.66 GiB two-second
sampled process-tree RSS peak. At step 6, default-scenario car trips changed
from about 4,486,282 to 4,362,352 and bicycle trips from 74,787 to 25,614
between replications 0 and 1. The carbon-tax minus default car difference was
about -41,597 versus -47,066 trips. These are only two seeds and one batch,
but the bicycle variation reinforces that this is not a production-stability
setting. A different seed also makes the two timings unsuitable as an
optimization benchmark.

The warm replication-1 run timed existing GTFS SHA-256 calls without adding
extra hashing: 114 calls across 46 files took approximately 0.75 s in total.
The source-manifest file was checked 41 times in 0.18 s, and the large Swiss
ZIP was hashed once in 0.24 s. On this warm run, these checks were not a
meaningful runtime bottleneck and should not be weakened on this evidence.

An initial 20-step minimal-effort probe was accidentally started with a 20 GiB
process-tree guard. It reached step 9 and was interrupted during step 10 by
that probe guard, not by a model error. Python RSS peaked at 19.92 GiB during
step 9; the two-second sampled process-tree peak reached 21.80 GiB before the
interruption was handled. The compact scoring path was used for all five
scenarios in steps 1–5, but the trace shows the full row-grouping path for
all scenarios in steps 6–9. This is a priority to investigate: it coincides
with scenario changes and sharply growing transient memory. The probe now
defaults to no added memory or temporary-disk guard, and a 20-step repeat was
started without either limit.

The unguarded repeat completed step 11 with the same five scenarios, one seed,
one activity draw, one destination draw, one mode sequence, and one batch.
Shared candidate IDs grew from 102,888 at step 9 to 248,029 at step 10 and
546,013 at step 11. Inner-loop times were 63.3, 122.3 and 214.5 s, with
Python-process trace peaks of 20.04, 28.95 and 42.45 GiB, respectively.
These are intermediate results, not a completed 20-step run. The trace shows
that the compact candidate grouping path is not used after step 5; the reason
and an equivalent compact representation remain to be investigated. The
opt-in `MOBILITY_PROBE_EXPLAIN_FALLBACK=1` diagnostic logs whether the existing
physical-step or membership uniqueness check prevented compaction; it does not
add a full-table checksum to each batch.

The six-step diagnostic identified the first fallback: physical steps were
unique, but multiple history records existed for the same modeled plan and
home zone. Candidate attachment now merges those histories before expanding
demand groups: minimum iteration and first-seen step, maximum last-seen and
last-active steps. This follows the existing history-merge rule and replaces
the former arbitrary selection among those records.

At minimal effort, the former step-six path spent 2.25 s grouping expanded
candidate steps; the merged-history run used factored grouping for all five
scenarios and spent 1.10 s there. Overall run times were 222.28 and 226.06 s,
so this small example does not establish an overall runtime improvement. All
trip-count keys matched and the largest absolute difference was 7.5e-9
represented trips. The history merge can affect later pruning compared with
the former arbitrary choice; this six-step result does not establish that all
later behavior is unchanged.

A later minimal-effort probe used two scenarios (default and carbon tax), one
seed, one activity, destination and mode draw, and one search batch. Factored
scoring was used for both scenarios through step 11; no expanded context
grouping appeared in those steps. The shared pool reached 139,010 candidates
at step 11, whose inner loop took 26.4 s and reached 13.62 GiB Python RSS.
The process-tree monitor stopped the run while preparing costs for step 12:
the sampled process and process-tree peaks were 13.35 and 14.03 GiB. The
14 GiB process-tree limit can be exceeded between samples or before a native
operation returns. This run used the history merge already described above;
it started before the subsequent context-history compaction changes. It does
not establish that step 12 or the five-scenario run fits in memory.

The later five-scenario, ten-step minimal-effort trace also contains 50
factored candidate-grouping calls and no row-based candidate-grouping calls.
The history fallback therefore did not occur in those completed evaluations;
the much larger unguarded run above predates the history fix. This does not
prove that the fallback cannot occur in longer or different runs.

The remaining physical-step check also protects against differences that the
modeled plan key does not itself rule out. In particular, destination sequence
keys are built from the ordered destinations, while a physical step includes
both its origin and destination. A different origin with the same destination
sequence could therefore require different step rows for the same modeled key.
We have not established that this occurs in normal complete plans. A unit test
checks that conflicting physical steps use the row-based path, so the check is
retained until the stronger invariant is established.

For the captured step-six carbon-tax choices, a separate diagnostic compared
behavioral transitions per demand unit with transitions after grouping units
by the earlier captured scoring-context map. It verified identical choices,
utilities, feasibility and reference times within each context. Across 43,054
units and 9,898 contexts, this reduced transition rows from 190,262 to 42,367.
The resulting context totals matched with both full retention and 95% retention
in this capture. Transition and allocation calculations took 0.15 versus 0.039 s
with full retention, and 0.24 versus 0.065 s with 95% retention; these times
exclude preparing and expanding the tables. The context map was captured in
another run, and the equality checks above only establish compatibility for
the captured choices. In general, pruning still needs to happen per demand
unit: units in one context can have different origin allocations and therefore
different low-population targets. This diagnostic does not establish an
end-to-end memory or runtime improvement.

The coordinated behavioral update now calculates probabilities once for each
scoring context and occupied origin, then restores unit-specific plan IDs and
previous utilities before applying transitions and pruning per unit. Finite
distance-threshold calculations continue to use the unit-level path. On the
same captured inputs, the production shared calculation reproduced all
190,262 unit-level transition rows (within numerical tolerance), taking
0.103 s including expansion. A bounded two-scenario, two-step Geneva probe
completed in 50.6 s with 8.10 GiB sampled process-tree peak and 10.37 GiB
peak Python RSS from completed trace spans; these measures sample differently.
It preserved the represented population. A subsequent run with the same
settings kept expanded scored choices lazy when using shared transition
probabilities; persistent plan IDs are still built for every choice. It took
53.0 s, with 8.05 GiB sampled process-tree peak and 10.36 GiB peak Python RSS
from completed spans. Trip counts matched to within 2e-9 represented trips.
These short runs do not demonstrate an overall performance gain. Reducing the
persistent plan index has since been narrowed as described below.

Multi-batch search now measures convergence and recent allocation effects from
the fixed outer allocation after applying reachable behavioral transitions.
The optional fresh-MNL flow output is still calculated separately when requested.
Temporary plan IDs cover only occupied origins and reachable targets; the
normal per-unit pruning is applied before context allocations are combined.
On the saved step-six choices, the new estimate matched a direct per-unit
calculation both with full retention and with 95% retention. In a two-scenario,
two-step probe with two mode draws and up to two batches, eight reachable
allocation estimates took 2.06 s in total, the inner loops took 39.54 s, and
the full run took 75.52 s with 7.65 GiB sampled process-tree peak. The Python
trace reported a 10.29 GiB peak, using different sampling. This is not a
comparison against the old search: the new convergence signal can change the
number and selection of generated candidates and therefore simulation results.
Generation already reuses occupied activity and destination sequences in
restricted phases, but the shared candidate pool can also contain historical
or other-scenario plans that cannot be reached by a given scoring context.
An intermediate batch that cannot yet end the search now scores only reachable
context-plan combinations. The first batch scores the full pool to identify
infeasible origins; contexts with one retain all candidates, since forced
reassignment bypasses normal restrictions. A final full evaluation preserves
candidate history and the optional MNL diagnostic. Previously scored results
excluded from the intermediate batch remain in the bounded cache when they fit.
The iteration cache version was updated because candidate selection can change.

In a bounded two-scenario, two-step, three-batch probe that switched to mode
replanning at step two, the filtered intermediate batch scored 298,226 of
805,149 context-step rows per scenario. With earlier scores retained in the
cache, the step-two inner loop took 30.77 s versus 32.28 s when scoring all
choices. Complete run times were 87.12 versus 86.28 s, and traced Python RSS
peaks were 10.49 versus 10.29 GiB. Candidate pool sizes matched, and trip
counts differed by at most 1.40e-9 represented trips. This single small
comparison does not establish a full-run time or memory improvement.
The reachable filter was subsequently moved before shared physical steps are
attached to scoring contexts. In the same bounded workload it reduced the
intermediate batch from 193,572 to 60,001 context-plan memberships per scenario.
The traced process RSS peak during those two scenario evaluations was 4.12 GiB,
versus 4.33 GiB when filtering after step attachment. Overall traced peaks were
10.37 and 10.49 GiB, while step-two inner times were 34.29 and 30.77 s and
whole-run times were 92.76 and 87.12 s. Candidate counts matched and the
largest trip-count difference was 1.40e-9. Process RSS includes other live
tables and allocator retention; these single runs do not isolate the memory
owned by the filtered rows or establish a time improvement.
An experiment also restricted the last batch while retaining unscored plans
in history. Keeping their old timings increased the final compact candidate
table from 36.76 MB to 40.27 MB per scenario; reusing matching modeled
timings reduced it to 36.88 MB. The latter run took 95.52 s, including
34.98 s for the step-two inner loop, compared with 86.28 s and 32.28 s for
the full-scoring reference. It also changes when unmatched history is timed.
The final-batch restriction was removed; the production path still scores the
full final pool. These are single bounded measurements, not scaling forecasts.
The optional MNL diagnostic is now computed only after the final search batch,
including when generation stops before reaching the batch limit.
When unit-level probability pruning is inactive, the reachable estimate now
applies transitions directly to context-level origins. This is valid because
the same origin and context have the same transition probabilities; pruning,
when active, still runs separately for each demand unit. On the saved step-six
choices the context calculation matched the per-unit totals and took 0.042 s,
compared with 0.193 s for the earlier implementation that expanded transition
rows to units. This is an isolated timing, not a complete simulation comparison.
Inner OD-mode volumes and convergence norms now sum contributions in sorted
order. This prevents a different candidate row order from changing a small
floating-point contribution and potentially changing a convergence decision.
The outer behavioral update now assigns new unit-level plan IDs only to
occupied origins and targets with a transition row. Existing IDs stay
in the index, and the full candidate history remains available for later
steps. In the captured step-six input, 222,857 of 706,277 scored unit-plan
choices were occupied or reachable under the captured transition parameters;
this does not account for IDs already retained from earlier steps. A matching
two-scenario, two-step, two-batch probe reduced each scenario's retained index
from 12,957,840 to 3,860,780 bytes after step one and from 37,390,864 to
9,022,216 bytes after step two. Candidate counts matched and the largest trip
count difference was 4.66e-9. The runs took 75.52 and 76.87 s, and their
traced Python RSS peaks were 10.29 and 10.49 GiB; these short runs do not show
an overall runtime or peak-memory improvement. New IDs can differ from the
former all-candidates index; the iteration cache version was changed. Finite
transition-distance thresholds now use distances between context-level plans
before selecting unit-level IDs. A small two-scenario integration check matched
the unit-level transition probabilities in full and mode-replanning phases;
the Geneva measurements above did not enable transition distances.
An experiment changed the schedule-catalogue update from eager operations to
a streaming lazy query, so the anti-join could be pushed into the query. On a
synthetic million-step table with 100,000 new rows it took 0.041 s versus
0.032 s for the existing calculation, with equal results. This does not measure
peak memory or represent the full Geneva workload; the change was not retained.
The catalogue update now uses the stable index's previous maximum ID to select
new schedules, avoiding a join against all existing IDs. On the same synthetic
million-step shape (100,000 new rows), the selected rows and resulting
schedules matched and this part took 0.013 s versus 0.033 s. The measurement
does not establish a run-level speed or memory improvement.

Completed scenario evaluations now compact candidate history before waiting
for the rest of the cohort, when the batch may be the last. A five-scenario,
three-step, single-batch comparison used identical settings with compaction
performed either after or during scenario evaluation. At step three, each
retained candidate-history table estimated 24.10 MB before and 5.78 MB after;
these are table estimates and can include shared buffers. Traced step-three
inner-loop RSS peaks were 5.72 and 5.67 GiB, and inner-loop times were 24.56
and 21.56 s. Whole-run times were 91.97 and 84.92 s; whole-process traced
peaks were 10.31 and 10.35 GiB. All 55 trip-count rows matched within 3.73e-9.
These single short runs do not establish a general runtime or peak-memory gain.
Completed evaluations now also retain only the step columns needed downstream.
When neither fresh-MNL flows nor finite transition distances are requested,
they retain steps only for occupied or reachable plans; full candidate history
and scored utilities remain available. In the same five-scenario workload,
the retained step table at step three estimated 10.12 MB per scenario.
The traced step-three inner-loop peak was 5.45 GiB versus 5.67 GiB before
this change; whole-process peaks were 10.40 and 10.35 GiB. Step-three inner
times were 22.24 and 21.56 s, and whole-run times were 87.15 and 84.92 s.
All 55 trip-count rows matched within 8.39e-9. These figures include shared
buffers, allocator retention and other live tables, and are not scaling forecasts.
Retained history now also keeps context memberships separate from the map to
demand units. Unit-level rows are reconstructed when needed, and temporary
state spills retain the map. In a subsequent five-scenario, three-step probe,
step-one membership plus its map estimated 1.64 MB per scenario, compared with
4.76 MB for expanded memberships in the previous run. At step three the new
membership and map estimated 4.28 MB per scenario. These table estimates may
share buffers. Candidate counts matched and all 55 trip totals matched within
5.59e-9 represented trips. Whole-run times were 111.00 versus 83.53 s and
traced process peaks were 10.14 versus 10.37 GiB. Many unrelated operations
were also slower in the new run, so this pair does not establish a runtime
effect or predict memory needs for a larger simulation.
An alternative joined occupied unit-plan keys to the context map before
looking up history, aiming to avoid expanding unoccupied memberships. On a
synthetic input with 60,000 context memberships, eight units per context and
16,000 occupied unit-plan keys, it produced the same rows but took 1.53 s for
ten selections versus 0.061 s for the existing lazy join. The alternative was
not retained. This isolated test does not measure a complete simulation or
establish how the two approaches compare for other input shapes.
For restricted phases, an alternative selected retained steps from the behavior
restriction alone rather than calculating transition probabilities. On the
saved step-six input it retained 59,359 context plans versus 50,124 with the
probability calculation, and took 0.039 versus 0.031 s. The alternative was
not retained; this isolated comparison does not establish a run-level effect.

Utility-profile assignment now retains demand-unit columns in Polars and
constructs only the profile-ID list in Python. Resolution still happens once
per distinct segment membership, with profiles assigned in first-seen order.
Repeated identical segment lists are also normalized only once; lists with
different orders or duplicates still resolve to the same membership.
On 43,054 synthetic units, ten assignments took 0.22 s versus 0.63 s for the
former row-dictionary approach. In a five-scenario, three-step Geneva pair,
15 scoring-context builds took 1.55 versus 2.38 s; the full runs took 84.30
versus 87.15 s. Traced process peaks were 10.54 and 10.40 GiB, and all 55
trip-count rows matched within 1.03e-8 represented trips. These single runs
do not establish a general whole-run speed or memory gain.

Scoring-context groupings can now be reused across steps and scenarios during
one coordinated run. Demand and freshly resolved profile assignments must both
match exactly; the cache is cleared at the end of the run. In a comparable
five-scenario, three-step probe, 15 grouping builds became one. The inclusive
time for building or checking contexts was 1.25 s versus 1.55 s before reuse;
whole-run times were 83.53 and 84.30 s, with traced peaks of 10.37 and
10.54 GiB. All 55 trip-count rows matched within 1.68e-8 represented trips.
These single runs do not establish a general speed or memory gain.

After the context allocation, smaller unit plan index and retained-table changes,
the five-scenario minimal-effort probe was repeated for six steps: one seed,
one activity, destination and mode draw, and one inner batch (30 rough effort
points including the six steps). The coordinated runs took 168.57 s versus
167.68 s in the earlier six-step probe. Step-six inner-loop traced Python RSS
was 7.87 versus 8.44 GiB, and the highest traced process RSS was 10.16 versus
10.41 GiB. All 115 trip-count rows had matching keys and differed by at most
9.32e-9 represented trips. The current run used a 10 GiB sampled process-tree
limit; trace RSS and sampled tree RSS measure different things. These single
runs do not establish a runtime improvement or that 20 steps and 10 scenarios
fit in memory.

After retaining context memberships instead of expanding them, the same
five-scenario, six-step probe completed in 156.18 s versus 168.57 s for the
earlier probe. At step six the inner loop took 15.44 versus 17.49 s and its
traced process peak was 7.95 versus 7.87 GiB. The whole-process traced peaks
were 10.39 versus 10.16 GiB. All 115 trip-count rows had the same keys and
differed by at most 1.03e-8 represented trips. These are individual runs and
do not establish a general speed or memory improvement. The 10 GiB sampled
process-tree monitor is not a hard limit on short-lived process peaks.

Temporary state spills preserve the compact context membership and unit map.
Durable iteration checkpoints still write the expanded candidate step table;
on resume it is compacted again after reading. Keeping durable checkpoints
compact would need their complete-file checks and cohort fingerprints to cover
all component files, including the unit map.

The highest traced peak in these short runs occurs during initial state
construction, before the simulation steps. A one-step probe with additional
spans measured survey-plan preparation at 2.35 s and 10.32 GiB peak Python
RSS, followed by survey-duration summaries at 4.27 s and 7.68 GiB. The cached
Geneva population-groups parquet contains 36,446,984 rows; survey-plan
preparation groups those into 28,507 demand groups before splitting them into
43,054 demand units. The recorded peak covers the whole preparation method,
not an individual operation, so it does not by itself identify the allocation
responsible. Its population-weight aggregation sorts values before summing to
preserve reproducible results; any alternative must retain that behavior.

More detailed tracing located the peak in the population grouping itself.
The grouping now encodes repeated string labels as categorical values before
grouping and restores the original string columns afterward. It still sorts
the individual weights and sums the same values. A two-million-row sample
produced exactly equal grouped values, and a unit test covers null labels and
floating-point weights whose order matters. In two one-step, five-scenario
probes on the full input, the grouping span peaked at 6.26 GiB versus 10.34 GiB
and took 1.39 versus 1.52 s. The highest traced process peak in the complete
probe was 7.87 versus 10.34 GiB; all 15 trip-count rows matched within 2.33e-9
represented trips. These are single runs, with other live and allocator-held
data included in RSS, and do not establish the peak for a larger simulation.

The weighted survey step cache contains 24,333,009 rows and stores its labels
as compact enums. Initial duration summaries now retain those labels while
grouping and only convert country labels to strings after grouping, where
needed to join population totals with different enum domains. In a matching
one-step probe, the summary span took 3.73 versus 4.65 s and its traced peak
was 6.16 versus 7.87 GiB. The complete traced process peak was 6.58 versus
7.87 GiB, and the 15 trip totals matched within 1.87e-9 represented trips.
Tests compare enum and string inputs with different country domains and check
reproducibility under reordered rows. These single probes do not establish a
general speed or memory gain, or the peak of a larger simulation.

A later ten-step attempt used five scenarios, one seed, one activity,
destination and mode draw, and one inner batch. It completed eight steps
(40 rough effort points) before the 10 GiB sampled process-tree monitor
interrupted step nine. The sampled tree peak was 10.21 GiB and the trace
recorded a short Python-process peak of 10.47 GiB; neither sampler is a hard
ceiling. The completed inner-loop times for steps 1 through 8 were 9.3, 19.5,
21.3, 22.4, 22.5, 15.6, 15.5 and 17.9 s, with traced process peaks of 4.25,
4.44, 5.69, 6.31, 7.26, 7.88, 8.57 and 9.69 GiB. The shared pool had 37,322
candidates at step eight. In the interrupted step, the highest traced peak
occurred while materializing and indexing modeled timings during the third
scenario evaluation. The sampled process RSS includes all live tables and
allocator-retained memory; it does not identify the memory owned by that
operation. No complete step-nine or final results were produced.

Timing-key construction now selects only columns used in the key and hashes
rounded seconds directly rather than adding four full-length integer columns.
On two million captured candidate rows with constructed demand keys, two hash
fingerprints matched; sampled process peaks were 1.084 versus 1.203 GiB and
times were 0.319 versus 0.300 s. A unit test also compares the actual old and
new timing hashes. In a three-step, five-scenario probe, all 55 trip totals
matched within 5.59e-9, while overall traced peaks were 6.62 versus 6.51 GiB
and summed timing-index spans were 2.30 versus 2.29 s. These single runs do
not establish a run-level peak or speed improvement or that step nine fits.

An isolated experiment also replaced the eager join that attaches time IDs
with a streaming join. Without an explicit order, the streaming result had a
different row order; requesting left order reproduced the eager output exactly
on a 500,000-row sample. On two million captured rows with constructed plan
keys and extra scoring columns, separate-process sampled peaks were 0.864 GiB
for the ordered streaming join and 0.714 GiB for the eager join; times were
0.114 and 0.091 s. The alternative was not retained. These isolated samples do
not establish the behavior of the full simulation.

After scored steps are materialized, their old lazy query and local candidate
intermediates are now released before timing indexing. In a three-step,
five-scenario probe the step-three inner-loop peak was 5.56 GiB versus
5.94 GiB before the release; whole-process peaks were 6.74 and 6.62 GiB, and
all 55 trip totals matched within 1.03e-8 represented trips. These individual
runs include allocator-retained memory and do not establish a general peak
reduction or whether the interrupted ninth step will fit.

The ten-step probe was repeated after the timing-key and reference-lifetime
changes. It again completed eight steps and the 10 GiB monitor interrupted
step nine. Step eight's inner-loop traced peak was 9.62 GiB versus 9.69 GiB
before; the interrupted step reached 10.10 GiB traced Python RSS and 10.01 GiB
sampled process-tree RSS, versus 10.47 and 10.21 GiB previously. The monitor
interrupted different points in the two runs, and sampling can miss short
peaks, so the interrupted-step figures are not controlled peak comparisons.
Neither run produced final results.

An opt-in inventory after step eight then found at least 40 live Polars
profile-cost tables estimated at 158 MiB each. These estimates may overlap in
their buffers, so they cannot be summed as process memory. Each completed
iteration kept its own profile-cost cache on its transport-cost object even
after the resolver released the iteration's value. The cache is now cleared
after the iteration's state has been produced; a later request can rebuild it.
In the repeat inventory no such 158 MiB profile table appeared among the
largest live frames. RSS at the inventory point was 4.45 versus 8.85 GiB,
and the step-eight inner-loop peak was 6.08 versus 9.68 GiB.

The repeat completed all ten steps for five scenarios, one seed, one activity,
destination and mode draw and one inner batch: 50 rough effort points. It took
373.93 s, with a 7.94 GiB traced Python-process peak and a 10 GiB sampled
process-tree limit. The earlier completed probe took 349.09 s and reached
14.16 GiB traced Python RSS; the runs were not controlled benchmarks, and the
newer run also collected the inventory. All 195 trip-count rows had matching
keys and differed by at most 1.21e-8 represented trips. The candidate count at
step ten was 87,487 in both runs. This does not establish the memory or time
needed for twenty steps, ten scenarios, or higher sampling effort.

An opt-in `MOBILITY_PROBE_EXPLAIN_SCORING_STEP` setting now writes Polars'
optimized query plan immediately before modeled scoring steps are collected.
In a one-step Geneva probe, the plan projects 20 of 21 candidate columns and
15 of 19 scoring columns, performs three joins, then sorts the plan steps and
uses a window operation to update timings. The scoring lookup on the right of
the main join is already materialized before this query. This is the plan for
one step, not a measurement of each operation's memory ownership; later-step
plans and data sizes may differ. Switching the whole computation to eager would
materialize more intermediate results and prevent some cross-operation
optimization. Isolated eager-versus-streaming comparisons remain useful when
they preserve results and ordering.

The opt-in `MOBILITY_PROBE_EXPLAIN_EVALUATION_STEP` diagnostic now records
optimized plans for collections called directly by the plan updater, the
materialized Polars tables in scope, and a sampled RSS span for each call.
It does not force extra collections; table size estimates may share buffers,
and a scope can contain tables that the query does not actually use.
In a six-step, two-scenario Geneva probe with one draw per sampling dimension
and one inner batch, the step-six collection of timed scored steps returned
683,800 rows and 29 columns (63.7 MiB estimated) in 0.17 and 0.25 seconds.
In the first collection, sampled Python RSS rose from 3.91 to 4.28 GiB; this
includes all live allocations and cannot be attributed to one operator. The
optimized plan still has the scoring and profile joins followed by a sort and
window over the plan keys. The candidate and compact candidate variables in
scope each reported 37.3 MiB, but their estimates must not be added as if
they had independent buffers. The full step-six inner loop took 8.94 seconds;
the instrumented direct collections are only part of that time. The run
completed with a 5.29 GiB sampled process-tree peak, and all 46 trip-count
rows matched a previous two-scenario run within 2.61e-8 represented trips.
This diagnostic identifies a collection to investigate further; it does not
establish which operator caused its peak or how larger runs will behave.

The timing update sorts rows by plan and step before finding each next
departure. It now compares adjacent plan keys instead of grouping those sorted
rows a second time with a window. On 683,800 captured scored steps, an eager
diagnostic reproduced the previous grouped-window result exactly, including
schema and row order. The window took 0.17 s; the adjacent-key calculation
including the final duration update took 0.004 s. These stages were measured
in one process with different already-live tables, not as an end-to-end or
controlled memory benchmark. In a subsequent six-step, two-scenario run, the
two step-six scored-step collections took 0.14 and 0.12 s versus 0.17 and
0.25 s in the previous instrumented run. The inner loop took 7.30 versus
8.94 s, with sampled peaks of 4.48 and 4.36 GiB respectively. All 46 trip
totals matched within 3.73e-9 represented trips. Single runs do not establish
a whole-run speedup or memory reduction. `MOBILITY_PROBE_CAPTURE_TIMING_STEP`
and `tests/manual/inspect_timing_update.py` provide the isolated diagnostic.

An opt-in `MOBILITY_PROBE_TRACE_SHARED_ROAD` probe records timestamped output
from the shared road-cost process. In a six-step, five-scenario run, the first
shared call took 8.00 s and used one distinct flow table. The recorded markers
showed about 2.7 s before the scenario began, 0.77 s for traffic assignment,
and 2.55 s for routing its requested OD pairs; aggregation and file output
followed. The final-cost call took 20.11 s and used two distinct flow tables.
For its two scenarios, assignment markers were 2.25 and 1.41 s apart, and
routing markers were 4.40 and 4.07 s apart. These are one run's timestamps,
not isolated CPU timings, and include logging and scheduling delays. All 115
trip totals matched the preceding six-step probe within 9.32e-9 represented
trips. Routing each distinct flow scenario is a larger target than the initial
OD-flow preparation in this sample; reducing that work must preserve the
destination choices and their travel costs.

The same road diagnostic counted 1,137,950 route requests and 1,137,585
distinct graph-vertex pairs in the Geneva sample. Only 365 requests were
duplicates, so deduplicating them before routing is unlikely to repay the
additional grouping and join work here. The diagnostic also saved the two
distinct flow inputs and reference road costs to a temporary folder for
isolated comparisons; it does not change the simulation's route requests.

The shared road calculation now drops building and coordinate columns after
constructing the route requests, keeping the vertex pairs, zone IDs and
weights needed for routing and aggregation. An isolated comparison using the
captured two-flow input returned exactly equal schemas, keys and values for all
1,070,826 zone pairs. Sampled process-tree peaks were 1.28 GiB with the narrower
table and 1.38 GiB before; times were 21.12 and 20.55 s. These single isolated
runs do not establish a speedup or a full-simulation peak reduction.

Completed scenarios now share the same modeled schedule table when their full
tables, including row order and schema, are equal. In a five-scenario,
three-step probe, the five tables were identical at steps two and three and
were each represented by one shared table after the step. The step-three
table's estimated size was 9.66 MB. These estimates can include buffers already
shared before this change, so they are not measured process-memory savings.
The three equality-check spans totaled 0.006 s. The 55 trip-count rows matched
the earlier probe within 3.73e-9 represented trips. The probe also included
the later initialization changes described above, so its whole-process peak
and runtime cannot be attributed to schedule sharing alone.

The same equality check now shares equal current-plan, current-step, plan-index,
destination-index and mode-index tables too. In a follow-up three-step probe,
all six checked tables were identical across the five scenarios after each
step. The step-three current-step table estimated 45.99 MB and the plan index
9.35 MB; those estimates may include buffers shared before the change, so they
are not process-memory savings. The checks took 0.042 s across three steps,
the overall traced peak was 6.52 versus 6.61 GiB in the preceding probe, and
all 55 trip totals matched within 1.68e-8 represented trips. These are single
runs; differing scenario state is kept separately.

Compact candidate history components are now checked independently as well:
identical physical steps, memberships and unit maps can each be shared without
combining histories that differ. In a subsequent five-scenario, three-step
probe, each component was identical across the five scenarios at all three
steps. At step three the shared step, membership and unit-map tables estimated
2.02, 3.76 and 0.52 MB respectively; these estimates may count buffers already
shared before this change. All 55 trip totals matched the preceding probe
within 1.21e-8 represented trips. Checks of all shared state and candidate
tables took 0.072 s across three steps, versus 0.042 s for state tables alone;
traced process peaks were 6.51 and 6.52 GiB. These single runs do not establish
a general runtime or process-memory improvement.

A six-step repeat included the carbon-tax change at step five and restricted
mode replanning at step six. After step five, current plans, current steps and
both candidate-history tables had two distinct versions across the five
scenarios; the schedule, sequence indexes, plan index and unit map still
matched. After step six the plan index also had two versions. The run took
171.25 s versus 156.18 s in the earlier six-step probe, with overall traced
process peaks of 7.90 and 10.39 GiB; the later run also includes the startup
optimizations. Step-six inner-loop peaks were 7.90 and 7.95 GiB. All 115 trip
totals matched within 1.40e-8 represented trips. This pair does not isolate
the effect of sharing or establish scaling to larger runs.

An experiment also tried sharing equal materialized tables while completed
scenario evaluations waited for the outer step. In a five-scenario, three-step
probe, the completed step and utility tables did not pass the exact equality
check, while their plan indexes and schedules did. The check includes row
order, so this does not establish whether the same rows appear in a different
order. Step-three inner-loop peaks were 5.79 GiB with the experiment and
5.49 GiB in the preceding run; all 55 trip totals matched within 5.59e-9.
These single runs do not isolate a memory effect. The experiment was removed
because it did not demonstrate a useful benefit for the large tables.
An opt-in diagnostic subsequently sorted the first-step carbon-tax and default
tables by plan and step keys. Both their 60,744 step rows and 42,696 utility
rows then matched exactly in every column and schema. This identifies row order
as the difference for those two scenarios at that step; it does not establish
equality in later steps or when scenario inputs differ. An experiment then
sorted completed choices and shared exact matches. Both step and utility tables
were shared across the five scenarios through step three; sorting and checking
took 0.129 s in total. Step-three inner-loop peaks were 5.53 GiB with the
experiment and 5.49 GiB before it, and the 55 trip totals matched within
1.03e-8. The experiment passed focused random-state, multi-batch and checkpoint
tests, but was removed because this probe did not demonstrate a peak-memory
gain and changing row order could change floating-point aggregation in other
workloads. These single runs do not establish the effect at larger scales.

Another experiment reused reconstructed candidate history when scenarios
already shared the same history and sequence-index tables. In the three-step,
five-scenario probe, candidate-hash calls inside pool preparation fell from 20
to 12; total candidate-hash time fell from 0.734 to 0.644 s. Pool preparation
took 31.92 versus 31.66 s, traced process peaks were 6.42 and 6.51 GiB, and
the 55 trip totals matched within 1.31e-8. The experiment was removed: this
short run did not show a worthwhile pool-level gain, while caching retained
additional tables during pool construction. It does not rule out a benefit
for larger pools.

A second probe with the same two scenarios, seed and sampling settings ran 11
steps after adding restricted outer choices and mapping times only for occupied
plans. Candidate counts matched the earlier probe at every completed step.
At step 11 the inner loop took 28.12 s and reached 13.61 GiB Python RSS;
both outer behavior updates together took 6.17 s and reached 13.37 GiB. The
earlier probe measured 26.44 s and 13.62 GiB for the inner loop, and 6.44 s
and 13.61 GiB for the outer updates. These single runs do not establish a
speed or memory improvement; the complete candidate pool is still scored and
indexed before the outer transition choices are restricted.

The same minimal-effort probe exposed a separate modeling issue. The Geneva
script sets `n_warmup_steps=0` and `max_inactive_age=1`, but in that run those settings were
applied only when candidate memory was built outside the shared-pool path. The
coordinated shared-pool run carried candidates forward without applying that
age filter. At the end of step 7, roughly 446,000 of 665,000 history
memberships in each scenario had dates older than the configured limit; at
step 9 the count was about 129,000 of 1.18 million. These counts include plans
that may have become occupied in that step, and are not counts of plans safe
to delete. Applying the old age rule would change the available choice sets
and potentially the modeled results, so it is not a retention-only optimization.
After excluding plans occupied at the end of each step, the corresponding
counts were about 296,000 at step 7 and 93,000 at step 9, per scenario. The
shared-pool merge and future generation can still affect the consequences of
applying a per-unit age filter.

The missing filter is already present in the committed shared-pool branch
(`3f3507f`); it is not introduced by the later uncommitted performance work.
The pre-branch `origin/main` implementation at `e2a7561` passed
`max_inactive_age` to candidate-memory construction. Consequently this is a
behavior difference from the earlier model, not simply an optional performance
improvement.

The earlier rule first merged old and newly generated candidates, marked
currently occupied plans active at the current step, and merged their history.
After warmup it retained a candidate when its most recent first-seen,
last-generated or last-active step was at most `max_inactive_age` steps ago.
For example, with age 1 at step 7, a plan last generated or occupied at step 6
remains; an unoccupied plan last seen at step 5 does not. The shared pool also
refreshes generated plans across scenarios of the same replication, so adding
the filter requires deciding its placement relative to that sharing and
preserving occupied plans and run-local history. Applying it only to the shared
physical catalogue would not reproduce a per-unit history rule.
The shared-pool preparation reduces compact previous memberships by
plan and merges the earliest first-seen and latest seen/active dates before
reattachment. Therefore two demand units with different histories can receive
the same merged history. A correct per-unit age filter needs those distinct
dates retained through the pool merge; filtering the already merged dates is
not sufficient to restore the earlier behavior.

The agreed policy now limits history per physical plan and replication rather
than reconstructing the old per-unit histories. After warmup, the first batch
of each outer step removes a replication's plan if it was neither generated nor
active in any scenario within the configured age. Occupied plans are marked
active before the filter. A physical plan is removed from the shared table only
when no replication retains it. Later inner batches do not age the pool. Run
and checkpoint identities were changed because pruning changes choice sets.
In a six-step, two-scenario Geneva run with one draw per sampling dimension
and one inner batch, the shared pool contained 13,287 physical plans at step 6,
versus 21,771 in the earlier unpruned run. Per-scenario candidate membership
was estimated at 2.85 MB versus 5.26 MB. The new run took 127.11 s and its
sampled process-tree peak was 5.18 GiB; the earlier instrumented run took
118.22 s and peaked at 6.02 GiB. These single runs include different diagnostic
overhead and do not prove a run-level speed or peak-memory improvement. Trip
totals matched to rounding through step 2, then changed, as expected when old
choices are removed; the largest absolute mode/scenario difference at step 6
was about 6,790 represented trips. This is not a full-scale benchmark.

When the configured search has only one batch, the runner now skips building
score and complete-plan caches that could only be reused by a later batch. In
two nine-step, two-scenario minimal-effort probes, candidate counts matched
and all 70 trip-count rows matched within 6.6e-9 represented trips. Summed
inner-loop times were 112.80 s before and 107.65 s after, with Python RSS peaks
of 9.48 and 9.39 GiB in those spans. The runs were not controlled benchmarks;
the latter also counted old, unoccupied memberships after each step. The
traces do confirm that 18 complete-plan signature and cache-retention calls,
which took 1.44 s in the earlier probe, no longer occur in this configuration.

The runner also omits the shared-pool catalogue when a batch is known to be the
last permitted one; that catalogue is only used to prepare a subsequent batch.
Another nine-step probe matched the preceding probe's candidate counts and all
70 trip-count rows within 4.7e-9 represented trips. Its inner loops totaled
119.37 s versus 107.65 s, and their peak Python RSS was 9.26 versus 9.39 GiB.
The runs had different concurrent workloads and do not establish a speed or
memory improvement. The trace confirms 18 fewer candidate-content hashing calls
in this one-batch workload; the catalogue construction itself is also skipped.

Modeled schedule indexing now selects catalogue columns before joining new
schedules, so the join does not carry unrelated scoring and population values.
An isolated diagnostic using five million captured Geneva candidate rows and
constructed score columns produced matching two-seed output fingerprints.
The full and narrowed joins took 0.391 and 0.396 s, with sampled process peaks
of 1.262 and 1.235 GiB respectively. These observations do not establish an
end-to-end performance gain; the constructed inputs are not complete model
state. The diagnostic is `tests/manual/benchmark_schedule_projection.py`.

With one search batch and no requested inner-flow output, the runner also skips
the fresh MNL allocation and flow reduction: no preceding batch exists for a
convergence comparison. It still computes those outputs when requested, for
debug logging, or when multiple batches may run. A four-step, two-scenario
probe matched the first four steps of the preceding nine-step probe across 30
trip-count rows within 3.8e-9 represented trips. The earlier trace contained
eight allocation and eight flow-reduction spans totaling 0.55 s; the new trace
contained none. Inner-loop totals were 52.26 and 53.00 s, with process peaks
of 5.25 and 5.34 GiB, so these runs do not establish an overall speedup.

Within each scenario's inner loop, modeled-time indexing now reuses its index
when the exact schedule catalogue produced by the previous call is passed on.
The cache is local to that inner loop and a different catalogue rebuilds the
index. A four-step, two-scenario comparison matched all 30 trip-count rows
within 1.9e-9 represented trips. The 16 time-indexing spans totaled 1.82 s
before and 1.41 s after; inner-loop totals were 53.00 and 50.75 s, with process
peaks of 5.34 and 5.38 GiB. These individual runs do not establish a general
speedup. A two-batch integration comparison also matched candidate IDs,
allocations and random-generator state with the cache enabled and disabled.

Candidate evaluation now collects the observed plan keys once for both stable
ID assignment and the occupied-plan coverage check. It no longer attaches
those IDs to individual steps before timing, where they were discarded. In a
four-step, two-scenario probe all 30 trip-count rows matched within 1.9e-9
represented trips. Summed candidate-evaluation times were 8.05 s before and
8.13 s after; traced inner-loop peaks were 5.38 and 5.26 GiB. These individual
runs show no material runtime improvement.

## Choices reachable under restricted behavior

An opt-in capture at step 6 recorded the carbon-tax scenario immediately
before its mode-replanning transition, with one activity, destination and mode
draw and one inner batch. The capture retained the full scored choice set;
the production outer-update filter was disabled only for this diagnostic.
Across all 43,054 demand units, 263,626 of 706,277 unit-plan choices (37.33%)
matched an occupied plan under the mode restriction or were needed to retain
the current plan. No occupied origin in this capture needed forced reassignment.

Using a scoring-context map captured in an earlier run, the corresponding
context-plan count was 59,359 of 164,145 (36.16%). Before computing this
comparison, the analysis checked that every mapped context had the same set of
choices, utility and feasibility values across all its units in this capture.
That check does not prove that the earlier map is the exact context assignment
for this step. The result is evidence of possible early filtering, not a direct
measurement of saved scoring time or memory. The diagnostic is
`tests/manual/analyze_transition_eligibility.py`.

Moving this restriction before scoring requires additional work. A current
plan may become infeasible under new costs, in which case its unit needs the
broader set of alternatives for forced reassignment. In addition, the inner
loop currently measures fresh MNL shares over the broader pool; filtering that
pool would change its convergence and adaptive-generation signals. A complete
change should first identify infeasible occupied plans and measure the actual
per-origin transition distribution, while retaining distinct eligible sets
for origins that share a scoring context.

In the main scoring path, mobile plans keep their already assigned IDs when
utilities are aggregated, and the completed index is reused when stay-home
steps are attached. A two-scenario probe with three activity draws, one
destination draw, two mode draws and two inner batches matched the preceding
probe's 24 trip-count rows within 2.8e-9 represented trips. Across three steps,
stable-index spans fell from 129 calls and 9.81 s to 117 calls and 8.03 s;
candidate-evaluation spans totaled 43.08 and 40.16 s. Total inner-loop times
were 93.95 and 94.29 s and traced process peaks were 10.34 and 10.48 GiB.
These single runs do not establish an overall speed or memory improvement.

The pre-index-reuse probe above took 132.41 s overall, compared with 153.89 s
in the earlier three-step comparison using the same stated effort settings.
Their 24 trip-count rows matched within 4.7e-9 represented trips. The newer
run included the intervening implementation changes, and the two runs had
different execution conditions, so the difference cannot be attributed to any
single optimization.

On congestion steps, profile scoring now reuses the iteration's default
OD-by-mode cost table instead of rebuilding it through an equivalent
transport-cost asset. Profile-specific overrides are still computed separately.
In a three-step, two-scenario probe, calls to generalized costs with road flows
fell from nine to seven per mode. The car calls totaled 22.72 s before and
21.92 s after; total probe time was 67.85 and 65.58 s. All 22 trip-count rows
matched within 2.8e-9 represented trips. Routing dominates those car-call
times, and these individual runs do not establish a general speedup.

Compatible scenarios now share one road assignment when their flow files are
byte-for-byte identical. The runner hashes each file and then checks its bytes
before sharing; it keeps separate result tables for each scenario. Different
flow files, networks and routing settings continue to be assigned separately.
In a six-step, two-scenario minimal-effort probe, the step-6 inputs contained
two identical flow files. That shared-cost span took 7.65 s, versus 11.94 s in
an earlier nine-step probe that assigned both; a final-cost calculation in the
six-step probe had two different flow files and assigned both. The 46 trip-count
rows through step 6 matched within 6.6e-9 represented trips. These individual
runs are not a controlled benchmark; later scenarios can have different flows
and receive no benefit from this sharing.

After inner evaluation, the coordinator now drops candidate attachment views
that the outer update does not consume. A first attempt at that lifetime change
also removed a marker needed to keep the preceding state available to all
scenarios. A five-scenario probe exposed the mistake: it rebuilt earlier state
and was interrupted during step 5 after the memory monitor sampled 15.12 GiB.
The coordination check now records whether an evaluation was supplied before
the state builder consumes it; a regression test covers the previous-state
lifetime. A repeat completed six steps in 168.01 s, with a 10.41 GiB traced
Python RSS peak and an 8.88 GiB sampled process-tree peak. It used one draw at
each sampling level, one batch and no additional scenario transit feeds. Its
default and carbon-tax trip counts matched a two-scenario reference across 46
rows within 5.6e-9 represented trips. The failed and corrected runs are not a
controlled performance comparison, and this does not validate a full 20-step
or ten-scenario workload.

A subsequent bounded probe completed ten steps with the same five scenarios,
one seed, one activity, destination and mode draw, one inner batch, and no
additional scenario transit feeds. It took 349.09 s. The shared pool contained
52,431 candidates at step 9 and 87,487 at step 10; those steps took 43.6 and
67.0 s, with traced Python RSS peaks of 11.79 and 14.16 GiB. The two-second
sampled process-tree peak was 13.88 GiB, so the 14 GiB monitor did not trigger;
sampling is not a hard memory ceiling. No initial state was rebuilt during an
outer step. Default and carbon-tax trip totals matched 78 corresponding rows
from the earlier two-scenario probe within 1.3e-8 represented trips. This is a
minimal-effort run with scenario feeds disabled, not validation of production
sampling or a twenty-step, ten-scenario simulation.

An experiment materialized only the trip and timing columns after scoring each
scenario, intending to release wider scored-step tables sooner. In an otherwise
matching five-scenario, ten-step probe, all 195 trip-count rows matched within
8.4e-9 represented trips. At step 10, inner-loop peak Python RSS was 14.20 GiB
versus 14.12 GiB for the reference, and the summed scenario-evaluation times
were 18.43 versus 18.48 s. The experiment was removed: these measurements do
not demonstrate a benefit that justifies the additional materialization. Peak
RSS includes allocator-retained memory, so the result does not measure the
precise amount of live data released.

The unguarded repeat completed step 13 with 926,997 shared candidate IDs.
Step 12's inner loop took 318.5 s and peaked at 49.30 GiB Python RSS; step 13
took 348.6 s and peaked at 49.55 GiB. During step 14, the process failed while
allocating another 16 MiB, after Windows reported insufficient paging-file
capacity. No probe memory or temporary-file limit was set. The largest RSS
recorded by a completed trace span was 50.69 GiB in step 14 candidate
grouping; the actual short-lived peak could have been higher. No step-14 or
final results were produced. Per scenario, the retained step-13 plan index
was about 697 MB and modeled schedules about 323 MB; those sizes can share
buffers and must not be added as a measurement of unique physical memory.
Across completed spans before the failure, self times included 254 s preparing
shared congested costs, 239 s grouping expanded candidate steps, 216 s indexing
time sequences outside nested index calls, and 143 s extending stable indexes.
These are nested-trace self times from an incomplete run; they are useful for
ranking investigation targets but are not a forecast for 20 steps.

The following rows are from the unguarded run. A step's wall time includes the
inner search, outer behavioral update and shared cost preparation. Peak is the
whole Python process, sampled by the trace; it excludes subprocess memory.

| Step | Shared candidate IDs | Step seconds | Inner seconds | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 4,024 | 11.2 | 10.9 | 3.93 |
| 2 | 8,972 | 22.1 | 21.5 | 5.00 |
| 3 | 14,426 | 24.7 | 23.9 | 6.27 |
| 4 | 19,154 | 27.8 | 26.8 | 7.40 |
| 5 | 20,703 | 40.6 | 36.1 | 9.28 |
| 6 | 22,760 | 67.4 | 31.4 | 11.81 |
| 7 | 22,849 | 66.2 | 32.7 | 13.57 |
| 8 | 67,134 | 74.2 | 39.7 | 16.32 |
| 9 | 102,888 | 100.2 | 63.3 | 20.04 |
| 10 | 248,029 | 169.3 | 122.3 | 28.95 |
| 11 | 546,013 | 280.3 | 214.5 | 42.45 |
| 12 | 883,865 | 393.5 | 318.5 | 49.30 |
| 13 | 926,997 | 434.2 | 348.6 | 49.55 |

Steps 1–13 total 1,711.8 s of step wall time; the last completed trace span
before the step-14 failure ended 2,075.1 s after the coordinated run started.
Step 14 has no complete step or inner-loop span. The candidate count is the
shared pool's distinct candidate IDs, not the number of demand-group choices
or people using them.

The default scenario's retained tables grew as follows (MiB; estimates may
share buffers):

| Step | Plan index | Modeled schedules | Candidate membership | Current plan steps |
| --- | ---: | ---: | ---: | ---: |
| 5 | 49.5 | 12.6 | 21.6 | 49.6 |
| 9 | 126.5 | 32.6 | 57.6 | 82.0 |
| 11 | 369.6 | 113.0 | 227.1 | 178.4 |
| 13 | 664.3 | 308.1 | 249.6 | 162.1 |

These tables explain part of the retained state but not the roughly 50 GiB
process peak. Expanded candidate-step grouping and other intermediate tables
are materialized during evaluation; per-table estimates do not identify how
much of peak RSS each operation owns.

Transit preparation for a changed scenario reads and extracts the cached base
feeds again, including the large Swiss archive, before adding that scenario's
feed. Each scenario's router has its own input-hash-prefixed outputs, so these
outputs can be reused on later runs if the feed paths and contents stay stable.
For first-run cost, investigate caching the filtered per-feed tables shared by
the scenarios. Any such cache must preserve source ordering and content
identity and must still choose the service date jointly across all feeds; a
per-feed choice of date could change the model.

## Sensitivity to a small change in effort

For context, comparing the *default scenario at step 2* in the two
early-compaction probes above gives the following represented trip counts:

| Mode | 2 activities, 1 mode | 3 activities, 2 modes |
| --- | ---: | ---: |
| Bicycle | 10,178 | 101,520 |
| Car | 5,453,149 | 5,526,035 |
| Walk | 1,631,684 | 1,657,696 |
| Public transport | 207,974 | 226,921 |
| Total | 7,302,985 | 7,512,172 |

Both have one seed, one destination draw, and two search batches. The
first run requested two steps and the second requested three; only step-two
results are compared. The increase from 0.14% to 1.35% bicycle trip share
shows that this very low effort is not a stable basis for drawing mode-share
conclusions. This is not a quality or convergence assessment: it changes two
sampling settings at once, uses one seed, and contains no observed-data
comparison. Vary settings separately and repeat seeds before choosing a
production effort level.

The older captured candidate query projects columns into its inputs but then
joins candidate steps to units before a union and unique operation. Moving
compaction before expansion is a deliberate materialization boundary: it
avoids relying on the optimizer to infer that a complete history can be
represented by separate physical steps and memberships. The later expansion
of current plan details remains a separate memory target.

In the saved step-two Geneva candidate history, plan and demand IDs are
UInt32, zone IDs and iteration numbers are UInt16, step positions are UInt8,
times are Float32, and country, occupation, activity and mode are encoded
enums. Do not narrow IDs based solely on the current dataset: later steps,
other populations or other projects can exceed the observed range.
