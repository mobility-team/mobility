# Grand Genève performance notes

## Current experiments

All timings below are single runs. The reported Python RSS is a sampled process
peak, not memory attributed to one operation. Pool counts matched in the
before/after comparisons, and reported trip totals differed by less than
9e-9 persons.

The separate complete-plan scores-and-timings cache was removed after it showed
no whole-run advantage in these samples. The shared physical candidate pool
and smaller step-scoring lookup remain. At 54 rough effort points, disabling
complete-plan reuse changed runtime from 101.67 to 100.96 s and peak RSS from
7.08 to 6.60 GiB. At 108 points, runtime was 116.58 versus 116.74 s and peak
RSS was 10.23 versus 8.72 GiB. The 108-point comparison preceded a small
plan-ID flow-join change. Older measurements of result reassembly below refer
to the removed path.

Evaluation now asks for profile costs only for OD pairs present in its new
scoring inputs. In a one-point probe, the profile cost table fell from
4,024,443 rows to 42,448. At 108 points, the scoring span took 4.06 versus
3.08 s, evaluation 37.98 versus 35.74 s, and the whole run 113.62 versus
106.24 s; peak RSS was similar. Pool counts matched and trip totals differed
by at most 6.52e-9 persons. These are single runs and the whole-run change
includes normal variation. Filtering during mode-cost construction was slower
at both 54 and 108 points, so mode search keeps the existing full cost cache
and filters immediately before building its search index.
In the divergent two-scenario 72-point run, evaluation took 26.36 versus
25.91 s, pool counts matched, and trip totals differed by at most 5.59e-9
persons. Peak RSS was essentially unchanged in this single comparison.
For two divergent scenarios and two sequential seeds at 16 points, pool counts
matched and trip means differed by at most 4.66e-9 persons; total time was
130.78 versus 134.16 s and traced peak RSS was 8.73 versus 8.56 GiB.

Fresh evaluation now collects scored steps once for coverage and modeled
timing, and assigns persistent plan IDs only after timing. At 108 points,
collecting once changed evaluation time from 37.03 to 36.29 s and peak RSS from
8.72 to 8.73 GiB. Plan IDs can change, so run and checkpoint identities were
updated. The shared catalogue also carries existing physical IDs between
batches, and keeps the ID list in columns rather than Python tuples. In an
isolated 400,000-ID check, the columns occupied 6.4 MB and conversion to
Python tuples allocated about 48 MB; this is not a whole-run memory estimate.
Scenarios within a seed now share the filtered physical candidate table and
refreshed-ID table during attachment. In the divergent two-scenario eight-point
probe, pool counts matched and trip totals differed by at most 6.52e-9 persons.
Pool preparation took 21.48 versus 20.12 s, and peak RSS was 6.35 versus
6.53 GiB. These are single runs; larger scenario cohorts are needed to assess
the tradeoff.

Mode-search validation compares integer location lists directly rather than
building strings. In a synthetic one-million-row check, the validation group
count took 0.209 s with strings and 0.069 s with lists; counts matched. This is
an isolated check, not a measured full-run speedup.
An opt-in `MOBILITY_PROBE_MODE_REUSE_DIAGNOSTIC=1` measures repeated
profile-and-location chains within an outer step. In a one-step, 54-point
probe, all 65,812 input chains were distinct within their batches; the third
batch repeated 3,079 chains from earlier batches, or 4.7% of all searches.
No mode-result cache was added on this evidence.

Mode search now builds its cost index only for legs in the sampled location
chains, including the return to the starting location. In a two-point probe,
each batch went from 1,341,481 available cost rows to about 24,000 needed
rows; mode search took 1.49 versus 0.32 s across the two batches. At 54
points, mode search took 3.97 versus 1.13 s; at 108 points, 7.00 versus
3.07 s. Pool counts matched, and trip totals differed by at most 4.66e-9
persons. Whole-run comparisons varied and do not establish the same saving
for the entire run. The filtering is tested against actual Rust search output
for multiple profiles and closing legs.
In the divergent two-scenario 72-point probe, mode search took 5.37 versus
1.48 s and pool preparation 26.82 versus 23.71 s. Counts matched and trip
totals differed by at most 7.46e-9 persons; peak RSS was approximately
6.84 versus 6.82 GiB. These are single-run comparisons.
In the divergent two-scenario, two-seed 16-point probe, mode search took
9.03 versus 1.79 s across 12 calls; total time was 132.93 versus 130.78 s.
Pool counts matched and mean trip totals differed by at most 3.26e-9 persons.

Destination probabilities are retained per utility profile, scenario and
outer step; OD cost tables are not retained. At 108 points, destination
sampling took 19.02 versus 16.39 s, whole-run time was 115.07 versus 110.64 s,
and peak RSS was 8.73 versus 8.79 GiB. At 54 points, sampling time was
essentially unchanged. Two scenarios and two seeds at low effort took 110.57
versus 113.72 s, with peaks of 7.18 versus 7.27 GiB. The cache helps when
profiles recur, but may cost time and memory when they do not. The manual
probe accepts `MOBILITY_PROBE_NO_DEST_PROBABILITY_REUSE=1` for comparisons.

For scenario scaling, two scenarios and two sequential seeds, with two steps,
one draw per dimension and two batches (16 rough effort points), took 110.57 s
and peaked at 7.18 GiB. The carbon-tax change in the Grand Genève script starts
at step five, so these short runs have identical scenario costs. Moving the tax
to step two with `MOBILITY_PROBE_SCENARIO_CHANGE_STEP=2` produced genuinely
different scenarios: the corresponding run took 132.93 s, peaked at 8.68 GiB,
and ended with 19,644 and 19,726 shared plans for the two seeds. These are
different workloads, not a controlled incremental-cost comparison. The probe
accepts `MOBILITY_PROBE_REPLICATIONS=0,1` to select multiple seeds.

With three activity draws, three mode draws, two steps and two batches, one
scenario (36 points) took 68.59 s, while two divergent scenarios (72 points)
took 107.17 s. The latter ended with 136,522 shared plans and peaked at
6.87 GiB. Inner loops totaled 29.56 versus 60.87 s; pool preparation 16.70
versus 30.53 s; scenario evaluation 11.59 versus 27.19 s; and road preparation
15.02 versus 20.19 s. These are single runs and spans overlap their parents.
In a repeated 72-point probe, restoring previous catalogue candidates took
0.13 s across four scenario restorations and building catalogues took 0.26 s;
the enclosing pool-preparation spans totaled 26.82 s. This does not rule out
larger catalogue costs at higher effort or with more scenarios, but suggests
that generation and evaluation deserve priority at this size.

The shared road calculation reuses its existing building-to-vertex table. An
isolated one-flow comparison produced exactly equal costs for 1,070,826 zone
pairs; the single-run timings, 7.69 and 7.77 s, do not demonstrate a speedup.

## Higher-effort single-scenario probe

A five-step, single-scenario, seed-0 probe used full replanning, six activity
draws, one destination draw, three mode draws and up to three inner batches.
All 15 batches ran: 270 rough effort points. It completed in 379.48 s without
a memory limit. The traced Python RSS peak was 14.75 GiB, and the two-second
process-tree sampler observed 13.31 GiB; their peaks use different intervals.
The shared pool grew from 127,967 plans after step one to 386,487 after step
five. Inner-loop seconds and Python RSS peaks by step were 27.5 / 6.59 GiB,
56.3 / 10.64 GiB, 66.7 / 12.55 GiB, 74.9 / 13.47 GiB, and 82.4 / 14.75 GiB.

Across the 15 batches, scenario evaluation took 180.3 s inclusive, and shared
pool generation and attachment took 120.7 s inclusive. Road-cost preparation
took 40.1 s, including final costs. Evaluation includes recursive calls: do not
add nested spans to it. Reassembling reused plan results accounted for 20.1 s
of self time (37.5 s inclusive), and the highest RSS occurred in that operation
in the last batch. These are process peaks, not memory owned by the operation.
This single run identifies where to investigate next; it does not establish
how a multi-scenario or longer run will scale.

The same five-step workload was repeated with complete-plan reuse disabled in
every batch; the compact scoring lookup remained enabled. The fresh-evaluation
run took 399.03 versus 379.48 s with reuse, and traced peak Python RSS was
12.29 versus 14.75 GiB. The five final candidate-pool counts matched, and all
20 represented trip-count rows matched within 5.59e-9. The scenario evaluation
spans totaled 175.6 versus 180.3 s, while shared pool preparation was 137.2
versus 120.7 s and road-cost preparation 45.9 versus 40.1 s. These individual
runs do not establish a whole-run time advantage for either path. They show a
memory tradeoff in this sample, not exact equality of every modeled choice.
The diagnostic switch used for this comparison has since been removed along
with the complete-plan reuse path.

## Five full-replanning steps: mode-sampling comparison

Two single-scenario, seed-0 probes used one activity draw, one destination
draw, one inner batch and three versus six mode draws, with full replanning on
all five steps. Both included the opt-in step-five evaluation query diagnostic.
The three-mode run took 94.24 s and finished with 61,318 physical candidates;
the six-mode run took 109.90 s and finished with 129,765. Peak traced Python
RSS was 6.53 and 6.74 GiB, including initialization. Road-cost preparation took
39.13 and 40.76 s; candidate evaluation took 7.73 and 13.35 s, including
modeled-timing materialization and indexing at 2.28 and 5.04 s.

At step five, the compact scoring lookup produced 472,398 versus 543,154 rows,
while the timed, scored step table contained 1,975,137 versus 4,308,836 rows
(estimated 183.7 versus 400.7 MiB). Its collection took 0.205 versus 0.396 s;
the following time-sequence indexing took 0.344 versus 0.897 s. These are
single instrumented runs: their process RSS includes other live buffers, and
the timings do not establish scaling at other effort settings. The query plans,
trace and collected table summaries were written to the probe folders.

A subsequent capture of the six-mode step-five input had 4,308,836 step rows,
1,015,976 context-plan combinations and 129,765 physical plan keys. The 543,514
physical-plan step keys each had exactly one combination of activity, reference
times and modeled travel time in this capture. Hashing complete timing inputs
gave 129,765 distinct two-hash signatures. This does not prove the same
physical plan always has the same timing in every configuration.

Two isolated experiments used this capture. Updating timings on the deduplicated
physical steps and joining the results back produced an exactly equal table, but
took 0.71 versus 0.45 s and peaked at 3.12 versus 2.69 GiB sampled process RSS.
Hashing deduplicated physical schedules and then joining hashes back produced an
exactly equal mapping in 0.25 versus 0.38 s, with 1.43 versus 1.51 GiB sampled
RSS. These single runs include the deduplication and join, but the latter only
works for this no-conflict input and does not measure full schedule indexing.
Neither experiment has been incorporated into production: the first regressed,
and the second's small isolated saving does not establish a safe general win.

These measurements are diagnostic runs, not forecasts for a full experiment.
The later single-scenario road-path comparison found an older cached congestion
graph that did not match a fresh assignment for the same observed flows. The
iteration-state identity omitted adaptive search and the shared cohort, so
different searches could reuse the same flow-dependent graph key. Consequently,
earlier runs that used the ordinary graph cache, including the 20-step
single-scenario comparisons, cannot be treated as validated model results.
The state identity now includes the search and cohort, and old flow-dependent
graph caches are invalidated. With fresh graphs, a six-step ordinary-path run
matched the unified path's 23 trip-count rows within 6.6e-9 represented trips.

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

## Candidate attachment check

An attachment experiment checked whether each shared candidate had an eligible
activity profile using one row per candidate instead of candidate step rows.
With two scenarios, one seed, one outer step, three activity draws, one
destination draw, three mode draws and two batches (36 rough effort points),
the pool reached 44,958 candidates. Summed attachment time was 0.78 s before
and 0.82 s after; full-run time was 45.81 versus 46.38 s. Eight trip-count
rows matched within 5.6e-9 represented trips. These single runs do not rule
out a benefit for much larger late-step pools, but they show no benefit here;
the rewrite was removed. Scenario transit feeds were disabled in this probe.

Sharing destination and mode sequence indexes across scenarios with the same
previous index removed eight stable-index extensions from a five-scenario,
one-step, minimum-effort probe. All 15 trip-count rows matched the reference
within 1.9e-9 represented trips; total runtime was 39.10 versus 40.30 s,
which is too small a difference to attribute to this change. The earlier
13-step trace spent only 1.51 s in these pool-level index extensions across
140 calls, compared with 46.01 s extending indexes during modeled-timing
materialization and 41.45 s in candidate evaluation. The extra sharing code
was removed: those larger evaluation costs deserve priority. These times are
nested trace spans, not independent components of the total runtime.

Evaluation now adds mobile and stay-home plans in one index extension, keeping
the former assignment order: new mobile IDs first, then new stay-home IDs.
This avoids validating and sorting the existing index a second time. In a
two-scenario, one-step, two-batch probe with three activity and three mode
draws (36 rough effort points), all eight trip-count rows matched within
9.4e-10 represented trips. The run took 45.81 s before and 45.81 s after;
the indexed tables are small at this effort. A synthetic 600,000-row previous
index and 600,000 mobile keys took 0.099-0.141 s for two extensions versus
0.083-0.116 s combined across three runs; this is not a full simulation
benchmark. A two-step probe with the carbon-tax change at step two matched all
14 trip-count rows within 3.8e-9. Stay-home IDs are now available earlier in
evaluation, so run and checkpoint identities were changed. No conclusion is
claimed about the memory or runtime of a full high-effort run.

## Selecting occupied candidate history

Context-shared candidate history is now joined from occupied unit/plan pairs,
instead of expanding all history to every unit before selecting occupied plans.
The optimized Polars plan for the earlier query showed the full context-to-unit
join before the occupied-plan semi-join. A synthetic example with 100 contexts,
100 units per context and 500 plans per context returned the same 10,000 rows:
the old query took 0.022-0.048 s and the new query 0.009-0.011 s over three
runs. This is a focused query measurement, not a whole-simulation benchmark.
A two-scenario, two-step minimum-effort Geneva probe with carbon tax starting
at step two matched all 14 reference trip-count rows within 3.8e-9. Its full
runtime was 55.69 s versus 58.99 s for the earlier run, and the factored
scoring spans totaled 0.267 versus 0.273 s; ordinary run-to-run variation
prevents attributing the overall difference to this change.

## Sharing step scores between scenarios

Scenarios with the same fixed scoring inputs now share the existing bounded
step-score lookup. The grouping uses asset identity for transport costs and
shared table objects for population, activity durations and destination
saturation; activity settings, population segments and the utility settings
must also match. It does not hash the large tables. If every requested score
is already present, scoring reuses the lookup without rebuilding cost tables.
The lookup remains scoped to the outer step and the existing shared 512 MiB
retention budget. Different scenario costs or settings use separate lookups.

In a five-scenario, one-step probe with one activity and destination draw,
three mode draws and one batch (15 rough effort points), all five scenarios
shared scores. Summed step-scoring time fell from 2.10 to 0.63 s and the
coordinated run from 40.80 to 39.39 s. The 15 trip-count rows matched within
2.4e-9 represented trips. Scenario transit feeds were disabled, and the tax
change had not yet occurred. A two-step minimum-effort probe with carbon tax
starting at step two shared scores in step one and used separate lookups in
step two; all 14 trip-count rows matched within 4.7e-9.

A two-scenario, two-step probe with three activity draws, three mode draws and
two batches (72 points) completed in 102.62 s with a 6.68 GiB traced Python
RSS peak. Earlier matching runs ranged from 97.01 to 107.04 s and 6.82 to
6.87 GiB; all 16 trip-count rows matched within 7.5e-9. This comparison shows
no clear overall speedup, and does not establish scaling at 20 steps or with
real scenario transit feeds. Run and checkpoint identities were updated for
the changed scoring execution.

An additional two-step, five-scenario diagnostic found equal physical steps,
memberships, scoring contexts, modeled schedule tables and transport-cost
identities across the five scenarios at each step. It did not establish that
all history and evaluation inputs were equal, so it does not by itself justify
sharing complete evaluations. It does show where a common representation may
avoid more duplicated timing work.

## Reusing road route requests

Road congestion updates now reuse the static route-request table within one
simulation. It is keyed by the road graph, routing graph, transport zones and
maximum routing distance; different traffic-assignment settings can still use
the same requests. A temporary parquet file is renamed only after it is fully
written, so an interrupted write cannot be used as a completed table.

In a one-scenario, two-step minimum-effort probe, the two road preparations
took 7.82 and 7.41 s before, versus 8.19 and 6.70 s after. The first call
builds and saves the requests; the second reads them. These single runs do not
establish a full-run speedup. In a two-scenario, two-step probe with different
carbon-tax flows, all 14 trip-count rows matched within 1.4e-9. Replaying the
captured two-flow road inputs produced exactly equal tables for freshly built
and reused requests: 1,070,826 rows and both scenarios' distance and time
columns matched the original calculation exactly. The temporary request file
was about 3.1 MB in this probe.

## Building the shared pool directly

The shared candidate pool is now an ordinary batch-local builder. Previously it
was registered as an in-memory asset, so its constructor hashed each previous
history, schedule and sequence-index table, and the coordinator immediately
released the resulting asset after its one use. Building directly removes those
full-table hashes and the unused asset registration. This does not remove the
integer candidate IDs or the identities used for saved simulation state.

An opt-in reconstruction of the old table-hashing work in a five-scenario,
two-step, minimum-effort probe took 0.015 s for five tables in step one and
0.032 s for 30 tables in step two, representing 29 and 47 MB of logical table
sizes respectively. The table estimates can include shared buffers and are not
unique physical memory. This excludes the old asset registration and other
identity construction overhead; it is not a high-effort measurement.

In the 72-point two-scenario, two-step, two-batch comparison, all 16 trip-count
rows matched within 5.6e-9. The coordinated run took 98.19 s versus 102.62 s
in the earlier run, with traced Python RSS peaks of 6.65 and 6.68 GiB. The
earlier run also lacked road-request reuse, so this does not isolate the impact
of removing pool hashing. Focused integration checks also covered two-batch
candidate IDs, allocations, random state and checkpoint continuation.

## Sharing generated candidates

Within one shared pool, scenarios now generate and attach their new candidate
steps once when they use the same population, replication, previous-state
asset, destination-generation asset and mode-generation asset. Existing
asset identities establish that these inputs match; no candidate-table hash or
comparison is needed. Each scenario still merges its own candidate history.
The extra reuse reference is released after its last matching source.
Different replications or generation inputs are handled separately.

In a five-scenario, five-step, minimum-effort probe (25 rough effort points),
generation ran six times rather than 25: once per step while all scenarios had
matching inputs, and twice at step five when carbon-tax inputs differed.
Summed generation time fell from 2.53 to 0.57 s and shared-pool preparation
from 24.42 to 21.13 s. The coordinated run took 106.68 versus 109.30 s; the
traced Python RSS peaks were 6.46 and 6.47 GiB. All 95 trip-count rows matched
within 1.2e-8 represented trips. These are single runs with scenario transit
feeds disabled and do not establish performance at 20 steps or high sampling.
Unit comparisons also cover separate scenario histories and replications.

## Sharing occupied-plan reconstruction

Scenarios that refer to the same fixed previous state now reconstruct its
occupied plan rows and content IDs once per shared pool. Each scenario still
combines those rows with its own sampled history. Reuse requires the actual
history, allocation, sequence-index and schedule objects to match; locally
reconstructed fallback histories are not retained for this purpose. The extra
reuse references are released after the last matching state is processed.

In the same five-scenario, five-step minimum-effort probe, calls that assign
candidate-content IDs fell from 46 to 30, taking 1.74 versus 0.58 s in total.
Shared-pool preparation took 21.13 versus 19.61 s, while the coordinated run
took 106.68 versus 106.72 s. Traced Python RSS peaks were 6.46 and 6.31 GiB,
and all 95 trip-count rows matched within 7.5e-9 represented trips. These are
single runs and do not isolate a whole-run memory or runtime improvement.

A 72-point, two-scenario, two-step, two-batch run with a carbon-tax divergence
completed in 92.48 s with a 6.66 GiB traced Python peak; all 16 trip-count
rows matched the earlier run within 4.7e-9. The earlier run took 98.19 s and
also predates shared generation, so the difference includes both changes.
Unit comparisons cover different scenario histories with the same occupied
state; integration checks cover random state and checkpoint continuation.

## Sharing compact-history reconstruction

When scenarios use the same compact history, sequence indexes, schedule and
current allocations, pool construction now reduces that history and assigns
its physical content IDs once. Each scenario receives its own history summary.
The reuse keeps only narrow ID and history tables until the last matching
scenario; it does not retain the reconstructed wide step table. Histories that
differ, including per-run catalogue histories, continue to be processed
separately.

In the five-scenario, five-step minimum-effort probe, candidate-content ID
calls fell from 30 to 14 and pool-preparation time from 19.61 to 17.62 s.
Coordinated runtime was 104.58 versus 106.72 s, with traced Python RSS peaks
of 6.33 and 6.31 GiB. All 95 trip-count rows matched within 1.1e-8. A
72-point, two-scenario, two-step, two-batch comparison took 92.94 versus
92.48 s; all 16 trip-count rows matched within 3.8e-9. These single runs do
not establish high-effort or 20-step performance. Focused tests compare shared
and independent reconstruction, including histories with different dates.

## Sharing candidate attachment

When scenarios contribute the same history tables, pool construction now
merges that history once and reuses the resulting local candidate table. If
their previous-state asset, sequence indexes, schedule and profile assignments
also match, it attaches and indexes the candidates once. Multi-batch runs can
also reuse the resulting catalogue's physical tables while keeping a separate
membership for each run. Different histories or indexes remain separate. The
sharing uses existing asset and object identities; it does not hash or compare
complete candidate tables to detect a match.

In the five-scenario, five-step minimum-effort probe, candidate attachment ran
six times instead of 25 and took 0.94 versus 3.66 s in total. Shared-pool
preparation took 15.20 versus 17.62 s and the coordinated run 103.39 versus
104.58 s. The traced Python RSS peak was 6.56 versus 6.33 GiB; inner-loop
peaks varied by step, so these single runs do not establish the memory effect.
All 95 trip-count rows matched within 9.4e-9. In a 72-point, two-scenario,
two-step, two-batch run, attachment was shared in the first batch and separate
in the later batches, whose inputs did not share the required identities. All
16 trip-count rows matched within 8.4e-9; runtime was 91.95 versus 92.94 s,
with traced peaks of 6.78 versus 6.54 GiB. Real scenario transit feeds were
disabled in both probes.

## Sharing complete candidate evaluations

Scenarios now share a complete candidate evaluation when their attached
candidate tables, compact history, state tables, cost asset, scoring context,
schedule and evaluation settings match. The grouping uses existing asset and
object identities and compares the small settings; it does not hash or compare
candidate tables. Matching scenarios are evaluated together, so at most one
complete evaluation is retained for reuse at a time. A completed, compacted
evaluation is also shared when the behavioral scope matches. Different inputs
or scopes keep their own results. Run and checkpoint identities were updated.

In the five-scenario, five-step minimum-effort probe, complete evaluations
fell from 25 to six: one per step until carbon-tax inputs diverged, then two.
Summed candidate-evaluation time fell from 16.29 to 6.04 s; coordinated runtime
fell from 103.39 to 97.24 s. Traced Python peak RSS was 6.66 GiB with sharing
versus 6.56 GiB without; inner-loop peaks were lower in the shared run. All 95
trip-count rows matched within 8.4e-9 represented trips. This comparison
predates sharing completed history compaction, so it measures
complete-evaluation reuse only.

With ten scenarios and one minimum-effort step, evaluation and completed
history compaction each ran once instead of ten times. The coordinated run
took 37.17 versus 41.22 s and the traced Python peak was 6.53 versus 6.44 GiB.
All 30 trip-count rows matched within 3.3e-9. Five scenarios were synthetic
copies of the baseline, and the five declared scenarios also had identical
inputs at that step because added transit feeds were disabled and the tax had
not started. This measures scaling across equivalent scenarios, not ten
distinct production scenarios.

The same ten scenarios over five minimum-effort steps (50 rough effort
points) required six evaluations and six completed-history compactions with
sharing, versus 50 of each without. Summed candidate-evaluation time fell from
29.01 to 5.74 s, and coordinated runtime from 122.05 to 99.44 s. All 190
trip-count rows matched within 2.0e-8 and candidate-pool sizes matched at
every step. Traced Python peak RSS was 6.54 GiB with sharing versus 6.26 GiB
without; step-five inner-loop peaks were 3.95 and 4.74 GiB respectively. The
whole-process peak includes initialization and allocator-retained memory, so
these single runs do not establish the change's peak-memory effect. The five
synthetic scenarios and disabled additional transit feeds have the same limits
as the one-step comparison above.

A 72-point, two-scenario, two-step, two-batch probe with carbon tax diverging
at step two took 90.89 versus 91.76 s, with traced Python peaks of 6.66 and
6.67 GiB. Candidate IDs, plan IDs, allocations, utilities and random states
matched exactly at both steps; all 16 trip-count rows matched within 2.8e-9.
This comparison predates sharing completed history compaction. A separate
two-scenario, two-seed, one-step probe exercised that compaction sharing:
candidate IDs, plan IDs, allocations, utilities and random states matched
exactly for both seeds, whose candidate sets differed from each other. Its
coordinated runtimes totaled 52.84 versus 53.51 s and traced peaks were 7.60
versus 7.79 GiB. These single runs do not establish 20-step or high-effort
performance, and scenario transit feeds were disabled.

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

### Inner allocation sharing at minimum effort

Two identical scenarios, one seed, one outer step, one activity, destination
and mode draw, and two inner batches give four rough effort points. Sharing
the allocation and its flows when the complete evaluation and behavior scope
matched reduced reachable-allocation calls from four to three and flow
aggregation calls from four to three. Their measured totals fell from 0.107 s
to 0.097 s and from 0.031 s to 0.024 s respectively; the full runs took 40.02 s
and 39.21 s. Candidate IDs, final allocations, utilities and random states
matched exactly. The extra bookkeeping was removed: the measured saving does
not justify the added complexity at this effort level.

The first inner batch shared evaluation, but the second did not. Its current
plans, scoring contexts and compact indexes were shared; its candidate step
tables were distinct objects. The intermediate catalogue reconstructs each
run's previous candidates separately, so investigate whether equivalent
catalogue memberships can stay shared by construction across batches. Do not
assume the candidates are equal merely because scenarios started alike:
adaptive requests and per-run history can differ.

The catalogue now carries which runs were built from the same attachment, so
the next batch can reuse their reconstructed candidate rows and history. This
uses the existing construction inputs, not a comparison of plan contents.
Runs with different history or indexes remain separate. In the four-point
probe, candidate evaluations fell from three to two, and saved candidates,
allocations, utilities and random states matched exactly.

With ten equivalent scenarios, one seed, one outer step, one draw of each
kind and two inner batches (20 rough points), candidate evaluations fell from
11 to 2 and their measured time from 3.85 s to 1.43 s. Catalogue restorations
fell from ten to one. The full runs took 49.45 s and 50.32 s, so they do not
establish an end-to-end improvement at this scale. All ten saved allocations,
utilities and random states, and the candidate IDs, matched exactly. Five of
these scenarios were synthetic baseline copies and the real policy changes
were inactive in this one-step probe; this is an upper-bound sharing case.

A separate eight-point probe used two scenarios and two outer steps, with the
carbon tax introduced at step two. Candidates, allocations, utilities and
random states matched exactly at both steps; the scenarios were evaluated
separately after the tax took effect. The baseline and sharing runs took
73.71 s and 71.20 s, with traced peak process RSS of 6.48 and 6.68 GiB. These
single measurements are not a reliable estimate of the overall speedup or
memory difference.

A higher-sampling probe used five scenarios, one seed, one outer step, two
activity draws, one destination draw, two mode draws and two inner batches
(40 rough points). It generated 23,370 candidates. The catalogue change
reduced evaluations from six to two (5.02 s to 1.88 s), pool generation and
attachment from 6.60 s to 5.04 s, and the inner loop from 16.69 s to 10.91 s.
Total runtime was 49.92 s versus 43.35 s, and traced process peak RSS was
6.50 GiB versus 6.62 GiB. Candidate IDs, all five saved allocation and utility
tables, and all random states matched exactly. These are single runs, and the
scenario changes were not active at this step.

At 90 rough points (five scenarios, one seed and step, three activity draws,
one destination draw, three mode draws and two inner batches), the pool held
44,958 candidates. Evaluations fell from six to two and from 8.01 s to 2.62 s;
the inner loop fell from 20.17 s to 11.99 s, and total time from 53.09 s to
44.23 s. Traced peak process RSS rose from 6.31 GiB to 6.70 GiB in these
single runs. All saved candidate IDs, plan allocations, utilities and random
states matched exactly. The scenarios were equivalent at this first step, so
the timing gain should not be extrapolated to later, diverging scenarios.
Both whole-run peaks occurred during initial survey-plan preparation, before
the changed inner-loop work. The inner-loop peaks were 4.81 and 4.73 GiB.

### Final costs for diagnostic-only requests

In a one-scenario, one-step, minimum-effort probe requesting only adaptive
search diagnostics, the old execution also computed final post-update road
costs. That update took 9.0 s in the trace and was unnecessary for the
diagnostic. The demand-driven path skips it; the complete runs took 50.31 s
and 46.11 s respectively, so startup and measurement variation obscure part
of the saving. An integration comparison verifies that search diagnostics and
transitions are unchanged when final costs are requested separately.

In a separate two-step minimum-effort road trace, the traffic assignment took
about 0.7 s per update, while routing the full set of zone pairs took about
2.5–2.7 s. The second update reused the route requests, avoiding their initial
construction. These timings describe a single scenario; larger flows and
distinct scenario networks can change the balance.

Metrics requested for explicit steps, including `steps="all"`, read those
steps' live plan and cost tables. The result planner now recognizes that the
last selected step also needs no post-update final costs. A two-scenario
integration comparison confirmed equal trip counts and travel distances with
and without an additional final-cost request. In a one-scenario, two-step
minimum-effort Geneva probe requesting all-step trip counts and search
diagnostics, the last congestion update disappeared: total runtime went from
44.76 s to 37.22 s. The earlier run also recorded detailed road log timestamps,
so the totals are not a perfectly controlled timing comparison. All seven trip
counts matched within 4.2e-9 represented trips.

### Persistent R road-worker prototype

`tests/manual/benchmark_persistent_road_worker.py` compares the existing road
script with a single pymizu/mizu R channel. The worker loads the graph and
route requests once, then runs the same road calculation for successive flow
files. It still exchanges parquet files; this first experiment did not change production code. Both
packages were installed into a temporary directory for this experiment.

Using two captured, different Geneva flow tables, both worker outputs matched
the corresponding one-shot outputs exactly across 1,070,826 zone pairs. A
third worker request repeated the first flow after the second and also matched
exactly. In two single comparisons, one-shot updates took 7.06/6.65 s and
10.85/10.50 s. Worker startup took 2.14 and 2.13 s; worker updates took
4.91/4.86 s and 8.71/9.78/9.11 s. Sampled Python-plus-child peak RSS was
1.01/1.02 GiB for one-shot and 1.17/1.16 GiB for the worker. These are isolated
road calculations, not end-to-end simulations; the variation between runs is
large, and the memory figures sum process RSS rather than unique memory.
The following runs extended validation to full simulations and changing inputs.

The manual Geneva probe was then used to test the worker. Two baseline and two worker runs each used
one scenario, one seed, five outer steps, one draw per sampling dimension
and one inner batch. The fifth step used the script's no-transition phase.
No memory limit was applied. The runs still exchange parquet files.

| Run | Full time | Congestion updates (steps 2–5) | Sampled process-tree peak |
| --- | ---: | --- | ---: |
| Baseline 1 | 86.19 s | 12.92, 7.03, 7.37, 7.07 s | not captured |
| Worker 1 | 66.44 s | 8.30, 4.90, 4.88, 4.79 s | not captured |
| Baseline 2 | 72.34 s | 8.12, 7.07, 7.14, 7.16 s | 6.14 GiB |
| Worker 2 | 69.06 s | 8.44, 4.92, 5.25, 4.70 s | 6.26 GiB |

The first worker update includes startup and graph loading. In both pairs,
later updates were about two seconds faster, but whole-run times varied much
more than that. Candidate IDs, final allocations, utilities and random states
matched exactly at all five steps in both comparisons. In the second pair,
all 19 trip totals matched within 1.9e-9 represented trips. Process-tree RSS is
sampled every two seconds and sums process RSS, so it can miss short peaks and
double-count shared memory.

A three-step, two-scenario probe introduced a carbon tax at step two; step
three assigned two distinct scenario flow tables. Candidate IDs, allocations,
utilities and random states matched exactly at every step, and 22 trip totals
matched within 2.8e-9 represented trips. The baseline and worker runs took
67.88 s and 77.67 s; their shared road updates took 8.40/11.38 s and
12.16/9.49 s respectively. Sampled process-tree peaks were 5.41 and 5.37 GiB.
One comparison cannot establish a reliable speedup or memory difference.
Separately, an invalid flow-file request returned an error and a subsequent
valid request reproduced the one-shot road table exactly.

The road worker is now used by the execution resolver. It retains one network
at a time and closes it when routing or assignment inputs change, on failure,
or when the execution ends. Distinct flow tables are still assigned sequentially.
The ordinary R runner remains in use if its optional CPU-idle watchdog is
explicitly enabled. The Python package `pymizu` and R package `mizu` are new
dependencies; on Linux a C compiler may be needed to install the R package.

With the production worker, a five-step one-scenario probe took 70.76 s; the
previous one-shot run took 72.34 s. Their saved candidate IDs, plan allocations,
utilities and random states matched exactly at every step. Production-worker
congestion updates took 8.69, 5.30, 5.69 and 5.22 s, versus 8.12, 7.07, 7.14
and 7.16 s for one-shot. A three-step, two-scenario run with a carbon-tax change
also matched all saved state exactly. A checkpoint continuation reproduced the
uninterrupted fourth-step state exactly, although the existing resume path did
earlier-step work again. A copied graph with changed travel times produced the
same outputs as one-shot routing on that graph; switching original/changed/
original networks in the prototype preserved both results. A deliberately
killed channel peer was detected and closed. These checks do not establish
parallel-worker performance or a full Linux simulation benchmark.

The manual Geneva probe now saves CPU and RAM samples together every two
seconds in `resource-samples.json`, with trace origins for matching samples to
simulation spans. CPU percentages sum the Python process and observed child
processes, with 100% representing one logical CPU. A new child is primed on
first observation, so work before that sample is not counted. A two-step
worker smoke run on a 22-logical-CPU machine recorded a peak of 1,745% and a
sample average of 479%; its step-two samples averaged 285%. These are coarse
samples, not CPU-time accounting or evidence about a longer run.

### High-effort, five-step run

One default scenario and seed, six activity draws, two destination draws, six
mode draws and three inner batches completed all five steps. The script's fifth
step used its no-transition phase. The rough effort is 216 points per step,
or 1,080 across five steps; all 15 inner batches ran. There was no memory
limit. Total runtime was 760.16 s; preparation before step one took 25.82 s.

| Step | Time | Candidates | Inner loop | Candidate evaluation | Pool generation | Road update | Python peak RSS | Process-tree peak RSS | Average CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 55.10 s | 351,915 | 54.26 s | 29.32 s | 19.83 s | 0.00 s | 9.76 GiB | 9.76 GiB | 854% |
| 2 | 154.66 s | 803,088 | 141.90 s | 84.13 s | 42.54 s | 10.02 s | 18.08 GiB | 17.43 GiB | 946% |
| 3 | 171.85 s | 925,510 | 161.69 s | 95.36 s | 47.45 s | 5.59 s | 21.80 GiB | 19.84 GiB | 1,014% |
| 4 | 213.00 s | 1,035,891 | 200.80 s | 112.16 s | 63.07 s | 6.63 s | 24.00 GiB | 24.47 GiB | 964% |
| 5 | 139.52 s | 762,137 | 129.72 s | 65.04 s | 51.81 s | 6.37 s | 21.09 GiB | 20.34 GiB | 924% |

Evaluation and pool generation are included in the inner-loop time. CPU
percentages sum observed processes and 100% means one logical CPU; the machine
has 22 logical CPUs. Whole-run sampled mean and maximum were 942% and 1,775%.
The Python trace samples every 100 ms; the process-tree monitor samples every
two seconds, so its peaks can be lower even when it includes the Python process.
Summed process RSS can also count shared pages more than once. The tree sample
reported complete process enumeration. This one run does not establish that
20 iterations, additional seeds or additional scenarios fit in memory.


## Repeated candidate evaluation within an inner loop

An exploratory diagnostic fingerprinted ordered scoring inputs before modeled
timings were updated. It compared the complete
physical timing inputs and the scoring values within each scoring context;
it did not change the inputs passed to the simulation. Two seeded hashes
were used for each ordered sequence, so the counts carry the negligible
possibility of a hash collision. Timing fingerprints include mode and OD as
well as travel times, activity and the input schedule. Distinct timing inputs
are therefore a conservative count of potentially shareable calculations.

A cache-only Grand Geneve probe used one default scenario and seed, two outer
steps, two activity draws, one destination draw, two mode draws and two inner
batches: 8 rough effort points per step. On step two, both batches used full
replanning and shared the same outer state and transport costs.

| Step 2 | Batch 1 | Batch 2 |
| --- | ---: | ---: |
| Scored step rows | 1,447,550 | 1,778,547 |
| Context-plan combinations | 355,164 | 440,184 |
| Distinct physical timing inputs | 44,088 | 54,346 |
| Context/score inputs also seen in batch 1 | ? | 355,164 |
| New context/score inputs | 355,164 | 85,020 |

Thus 80.7% of the second batch's distinct context/score inputs had already
been evaluated in batch one. It contained 8.1 context-plan combinations per
distinct physical timing input. These are opportunities, not measured
speedups: preserving plan identities, history, infeasibility and allocation
still requires work, and retained results consume memory. This one modest run
does not establish the same ratios at higher effort or with other inputs.
The diagnostic fingerprints and extra grouping affect time and RAM, so its
57.23-second runtime and sampled peak are not a performance benchmark.

### Physical timing factoring experiment

An isolated prototype grouped scored plan rows by their physical plan and
ordered timing inputs, updated and indexed one representative per group,
then restored all context rows and their schedule mappings. On an existing
683,800-row captured table, it reduced the input to 84,264 representative
rows and took 0.25 s versus 0.48 s for the existing timing and indexing path.
On another previously captured 4,308,836-row table, it reduced the input to
543,514 rows but took 1.35 s versus 1.34 s. Both comparisons used a schedule
catalogue reconstructed from the capture; the catalogue, mappings and complete
row multisets matched exactly in each comparison. These are single isolated
measurements, not full-simulation performance results.

A temporary production implementation completed a five-step, one-scenario
minimum-effort Geneva probe. All five saved candidate-ID and plan tables and
the final random state matched the earlier run exactly. Its summed timing and
indexing spans were 0.97 s versus 1.13 s before, while whole-run times were
85.08 versus 83.18 s and traced Python peak RSS was 6.71 versus 6.40 GiB.
The runs were not controlled benchmarks. The implementation was removed:
the larger isolated comparison did not show a gain sufficient to justify
the extra grouping, joins and implementation complexity.

On the 4.3-million-row capture, three isolated passes grouping by existing
plan keys took 0.150?0.158 s; computing two ordered step-content fingerprints
and grouping by the same keys took 0.360?0.422 s. The comparison includes
ordered list aggregation and hashing together, and ordinary group-by itself
may use hashing internally. It does not isolate the cost of the hash function
or the subsequent joins. It does show that constructing new sequence identity
was a material part of this prototype.

## Scope of plan and sequence identities

The existing `StableKeyIndex` assigns persistent integer IDs to distinct key
rows, but those IDs do not all have experiment-wide scope. Survey activity
and survey timing sequences use persisted survey indexes shared by the
experiment's runs. Each run separately extends destination and mode sequence
indexes from pairs of ordered-content hashes. Each run also extends its own
modeled-time catalogue and demand-unit plan index. `plan_id` includes the
demand group and subgroup and is therefore not a physical-plan ID.

Within an inner search, `PlanCatalogue` shares physical candidate steps keyed
by two content hashes. It separates physical content, sequence variants, and
run-local membership/history. `variant_id` is assigned within the catalogue
and rebuilt when catalogues merge. At the end of the search the catalogue is
discarded; checkpoint/spill state instead retains run-local candidate history,
plan and sequence indexes, modeled-time schedules and random state. Thus a
new experiment-wide physical index would need a coordinated lifetime and
checkpoint representation, rather than simply persisting current variant IDs.

A physical key must preserve ordered activity, timings, destinations and
modes. A plan starts and ends at home, so its final destination identifies
the first origin; each later origin is the preceding destination. Generated
destination steps follow this rule, and survey preparation makes the final
activity home. The ordered destination sequence therefore contains the
origins as well. Different survey reference variants can still map to the
same physical content and need distinct variant/history records.

An exact shared index could assign integer IDs to canonical typed sequence
contents, and identify a plan using those shared component IDs. Full key equality avoids
content-fingerprint collisions, although its memory and lookup cost need
measurement. Assigning consecutive IDs in sorted batches is repeatable for a
fixed processing order and resumable when the index is checkpointed; the
numbers are not automatically invariant to changing batch or scenario order.
Achieving both immutable sequential IDs and order-independent assignment
requires a stronger design than the existing incremental index.

A low-risk first investigation is to carry the existing physical candidate
identity from the shared catalogue into scoring, without changing persistent
IDs or rehashing expanded context rows. This can test whether a physical
identity actually removes evaluation work and how much memory its reuse costs.
It retains the current two-hash collision properties; replacing them with
exact canonical keys is a separate change.

### Using existing plan keys for timing reuse

Before adding a new shared integer, the existing run-local physical sequence
keys were checked against the two captured scoring tables used above. Grouped
by activity, time, destination and mode IDs and step index, the 683,800-row
capture contained 84,264 distinct step keys; the 4,308,836-row capture
contained 543,514. In both, every key had exactly one set of activity, OD,
mode, input timing and modeled travel-time values. This is an observation
about these captures, not a guarantee for every input or the expanded fallback
path.

An isolated variant of the previous representative-timing experiment used
those existing keys directly, without extra ordered-content fingerprints.
Including restoration of all context rows and modeled schedule mappings,
it took 0.20 s versus 0.45 s for the current path on the smaller capture
and 1.26 s versus 1.62 s on the larger one. Both comparisons used reconstructed
schedule catalogues and matched the catalogue, mappings and complete row
multisets. These are single measurements, not a full-run benchmark.

The reuse diagnostic now also records utility profiles. In a lowest-effort
Geneva probe with two outer steps and two inner batches, step 2 batch 2 had
128,937 context-plan combinations, 15,686 distinct physical sequence-key
combinations and 29,451 distinct combinations of physical keys and utility
profile. No physical key, or physical-key/profile pair, had more than one
timing input in this run. The first batch's 102,449 context/score inputs
were all present in batch 2. Profile-aware factoring could therefore have
processed 29,451 timing inputs instead of 128,937 context-plan inputs in
this example; different generalized-cost profiles can in principle yield
different travel times, so this result does not justify ignoring profile.

The current factored scoring path already checks that each local physical
plan key maps to one set of steps before attaching contexts; its expanded
fallback does not provide that guarantee. No new production identity or
evaluation cache was introduced in this experiment. Carrying a shared
physical identity across batches remains a separate change and must also
handle run-local index translation, modeled timing changes and history.

## Incremental candidate-evaluation prototype

An exploratory benchmark compared timing,
schedule indexing, plan scoring and synthetic choice/flow calculations on a
previously captured, already-scored Geneva step table. It splits the distinct
context-plan keys deterministically into an 80% existing pool and 20% new
plans, reconstructs a starting schedule catalogue, and holds costs and step
inputs fixed. It is an isolated prototype, not the production inner loop: it
does not measure candidate attachment, transport scoring, behavioral
transitions, real demand weights, checkpoint handling or a full simulation.

On the 683,800-row capture (140,926 existing and 35,232 new context-plan
keys), three isolated process runs took 0.48?0.57 s to update and index
all timings, score all plans and calculate synthetic choices/flows, versus
0.25?0.33 s when retaining existing timings, mappings and plan scores and
processing only new plans. The cached tables' estimated sizes total 60.5 MB
(57.7 MiB); some buffers may be shared. Choices are recalculated over the
whole pool, and flow calculations still read all steps.

Exact comparisons matched the scored step-row multisets, schedule catalogue
and ID mappings; synthetic choice probabilities and flows matched to 1e-12.
The original split had no old/new modeled-key overlap. A separate run added
one deliberately constructed pair of input schedules that became the same
modeled plan; the prototype recalculated the overlapping plan and passed the
same checks. This does not establish how common or expensive such merges are
in a real growing candidate pool.

In separately started processes, sampled Python peak RSS during the second
batch was 2.29 GiB for full reevaluation and 1.91 GiB for incremental
evaluation. These single samples include the input and allocator-retained
memory and are not a comparison of full-simulation peaks. The retained
cache has a real cost even though the incremental sample was lower. No
production code was changed. Before integration, a prototype using actual
consecutive batch inputs must account for changing histories, compact context
membership and index persistence, and compare real allocations and flows.

### Consecutive real inner batches

An exploratory diagnostic recorded scored input steps, run-local timing
catalogue, scoring inputs and evaluation outputs for each inner batch. It
substituted the prototype utility table in a second call to the transition
allocation and compared allocation and OD-mode flows, without changing the
production result.

A lowest-effort Geneva run used one scenario and seed, two outer steps and
two inner batches. In step two, the first batch's 102,449 context-plan keys
and 393,069 step rows were all present in batch two with identical input
values, including retention history. Batch two added 26,488 plan keys and
96,899 step rows. Their modeled plans did not merge with existing ones in
this run. Reusing the first batch's utility rows and scoring only the new
rows reproduced the actual second-batch utility table, timing mappings and
catalogue exactly. The real transition algorithm produced exactly matching
37,672 allocation rows and 14,122 OD-mode flow rows. The prototype used
the existing complete step table for transition/flow evaluation; this is not
yet a test of an incremental step-table implementation.

Three offline comparisons of timing/indexing and utility aggregation starting
from the captured already-scored step rows took 0.54?0.58 s for the full
second batch and 0.18?0.23 s for the incremental calculation. They exclude
candidate construction, cost lookup, transition allocation, flows and the
extra validation/capture work, so they are not full-run speedups. Those
initial timings did not include maintaining candidate histories or detecting
changes; the following section reports a more complete comparison.

### Inner-batch updates and history

The real-batch prototype now uses plan membership to find additions and
removals; it does not compare the steps of existing plans. This relies on the
inner-loop invariant that transport costs and scoring inputs stay fixed. It
retains existing utilities, evaluates newly added plans, keeps one utility
when a new input becomes an existing modeled plan, and merges history using
the existing earliest/latest rules. An explicit history change or removal
rebuilds only the affected modeled plan's history. The manual test constructs
one example each of a modeled-plan merge, a history change and a removal; in
all three cases utilities, candidate steps and histories, schedule mappings
and catalogues matched a full reevaluation exactly. These examples are not
evidence of how often those cases occur.

In three comparisons on the unchanged captured batches, including candidate
history materialization, the full calculation from already-scored step rows
took 0.52?0.54 s and the prototype took 0.27?0.29 s. The prototype derives
plan membership from the captured rows; in a production implementation the
pool could supply its additions, removals and history updates directly. The
comparison excludes candidate generation, cost lookup, transitions and flows
and is not a full-simulation benchmark.

`max_shared_candidates` is an optional pool-size cap, defaulting to `None`;
the Geneva script does not set it. Its inactivity limit is separate and is
applied at the start of an outer step. There was no pool-size cap in these
runs. Production integration, multiple scenarios, checkpoint/resume and
peak-memory effects still require validation.

### High-effort incremental-evaluation experiment

A temporary, opt-in implementation was connected to the actual inner loop
and tested on the same Geneva setup as the earlier high-effort run: one
default scenario and seed, five outer steps, six activity draws, two
destination draws, six mode draws and up to three inner batches. There was
no memory limit. It retained timed context-plan rows, timing mappings and
utilities within each outer step, updated retention history, and scored
new plans before recalculating allocation over the whole pool. This was a
manual experiment, not a production implementation.

| Outer step | Earlier run | Incremental experiment | Earlier candidate evaluation | Incremental candidate evaluation | Earlier Python peak | Incremental Python peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 55.10 s | 54.44 s | 29.32 s | 28.30 s | 9.76 GiB | 10.13 GiB |
| 2 | 154.66 s | 177.22 s | 84.13 s | 99.66 s | 18.08 GiB | 20.78 GiB |
| 3 | 171.85 s | 189.14 s | 95.36 s | 107.77 s | 21.80 GiB | 24.65 GiB |
| 4 | 213.00 s | 217.26 s | 112.16 s | 124.42 s | 24.00 GiB | 27.65 GiB |

For steps 1?4, elapsed step time rose from 594.61 to 638.06 s and candidate
evaluation from 320.97 to 360.15 s. The overall traced Python peak rose from
24.00 to 27.65 GiB; sampled process-tree peak rose from 24.47 to 27.51 GiB.
CPU sample averages were approximately 942% in both runs, where 100% means one
logical CPU. These are two individual runs, not a controlled benchmark.
Candidate counts for steps 1?4 matched, and all 20 trip-count outputs across
the five steps matched within 6.6e-9 represented trips. Other saved state was
not compared in the high-effort run.

The fifth step is not comparable: since the earlier run, no-transition
steps were changed to avoid generating candidates. In the incremental run,
step five took 45.17 s with a 15.86 GiB traced Python peak and 20.73 s in
candidate evaluation. The total run took 710.26 s, but its shorter fifth
step should not be credited to incremental evaluation.

The experimental implementation still expanded context-plan rows and
rejoined wide tables to refresh history and restore timing IDs. Its extra
materialization, joins and retained data appear to outweigh the avoided
calculations in this workload. Trace spans overlap and individual operations
were not independently isolated, so this is not a precise attribution. The
experimental implementation was removed; the captured-table diagnostic
scripts remain.

### Scale check for a 0.1-person global allocation floor

The saved five-step, lowest-effort Geneva run represented 2,341,382 people.
Its saved allocations had already used the existing 95% per-unit pruning
starting at step 2. As a descriptive scale check, mobile allocations were
summed across demand units by the run-local activity, modeled-time,
destination and mode sequence IDs. These component keys are a proxy for
physical plans within this run; no new floor was applied to the simulation.

| Outer step | Mobile plan keys | Keys below 0.1 person | People on those keys | Unit-plan rows |
| --- | ---: | ---: | ---: | ---: |
| 2 | 5,112 | 13 | 0.673 | 34 |
| 3 | 7,023 | 14 | 0.782 | 29 |
| 4 | 8,456 | 14 | 0.830 | 36 |

For comparison, a 1.0-person floor at step 4 selects 150 keys carrying
64.13 people and 448 unit-plan rows. These counts describe a pool already
subject to the current pruning policy. They do not predict a replacement
global floor's effects on an unpruned allocation, flows, future steps or
high-effort memory. Step 5 was not included because the proposed floor is
skipped on a no-transition step. Threshold effects also depend on absolute
study-area population; evaluate small studies as well as Geneva.

### Global volume floor: low-effort model comparison

A manual experiment compared the existing per-unit 95% retained-share rule,
no low-mass pruning, and global floors of 0.1 and 1.0 represented person per
physical plan. Each run used one scenario and seed, five outer steps, one
activity/destination/mode draw and two inner batches: two rough effort points
per replanning step. All used the same initial inputs and had no memory limit.
The global floor was applied to each inner estimate and the final behavioral
allocation from step 2 onward; removed mobile volume was redirected to the
same demand unit's stay-home plan. The floor was skipped on the no-transition
fifth step. The floor code was an opt-in manual prototype; it does not support
transition-event output and is not production behavior.

| Policy | Step 4 allocated unit-plan rows | Step 4 mobile physical keys | Step 4 total trips | Difference from no pruning |
| --- | ---: | ---: | ---: | ---: |
| Existing 95% | 239,805 | 10,922 | 8,900,492 | +21,787 |
| No low-mass pruning | 266,708 | 11,908 | 8,878,706 | reference |
| Global floor 0.1 | 266,519 | 11,842 | 8,879,179 | +473 |
| Global floor 1 | 264,922 | 11,440 | 8,879,341 | +635 |

These later-step trip differences reflect the whole simulated trajectory,
including changed inner allocations and subsequent candidate sampling. They
are not the number of trips removed directly by that step's floor. In the
outer allocation, the 0.1 floor redirected 0.873, 1.525 and 2.754 persons at
steps 2, 3 and 4, removing 18, 22 and 40 physical keys respectively. The 1.0
floor redirected 66.104, 96.829 and 138.984 persons, removing 139, 175 and
223 keys. These are represented-person weights; the same person-equivalent
can appear in successive outer steps. Unit-level population was conserved to
within 4.3e-14 persons in all four runs.

For a same-origin check, separate two-step runs captured OD/mode flows at
step 2. Relative to no pruning, the 0.1 floor removed 3.129 represented trips
across 49 OD/mode cells above 1e-8 difference; the largest cell decrease was
0.173 trip. The 1.0 floor removed 236.569 trips across 397 such cells; the
largest cell decrease was 3.957 trips. No OD/mode cell increased in these
two-step comparisons. Some small cells became zero; the floor is on plan
volume, so several removed plans can contribute to the same OD/mode cell.

Wall times for the no-pruning, 0.1 and 1.0 runs were 104.5, 98.6 and 111.2 s;
their traced Python peaks were 6.26, 6.49 and 6.34 GiB. The separate
two-second process-tree samples peaked at 6.12, 5.32 and 5.37 GiB. These runs are
not controlled performance measurements; the floor prototype also performs
extra state/index and step reconstruction after the outer update. They do not
establish that either floor is faster. This low-effort sample shows that both
floors remove far fewer allocated rows than the current per-unit rule; a
higher-effort or longer run could differ. A 1-person floor is an interpretable
candidate, not yet a validated performance default.

The comparison uncovered a separate no-transition bug in the existing 95%
path: unit-level target pruning still moved people after self-transitions.
Before correction, step 5 redistributed about 13,684 represented people
between mobile plan identities even though no-transition was configured. The
corrected run disables that target pruning during no-transition steps; its
step-4 and step-5 mobile allocations matched after ignoring timing-ID changes.
Focused unit tests also cover a 99/1 allocation and the inner estimate. The
corrected 95% run overlapped the separate OD-capture runs, so its wall time is
not a fair performance comparison.


### Per-subgroup allocation floor: low-effort run

After replacing retained-share pruning with a floor of one represented person
per plan and demand subgroup, a cache-only Geneva run completed in 129.2 s.
It used one scenario and seed, five outer steps, one activity, destination
and mode draw, and two search batches per replanning step. No memory limit was
set; sampled process-tree peak RSS was 5.07 GiB. The last step used no
transitions and produced the same trip totals as step 4 to floating-point
precision. This checks that the run completes, not whether the floor is
accurate enough for a particular study.


### Effort comparison with five full-replanning steps

An initial comparison used one Geneva scenario and seed, five full-replanning
steps and the one-person per-subgroup allocation floor. The minimum run used
one activity, destination and mode draw and one batch (one rough effort point
per step); the larger run used two activity draws, one destination draw, two
mode draws and two batches (eight points per step). Both reused input caches,
used eight Polars threads and had no memory limit. The probe also saved
aggregated flows and counts, so timings include that instrumentation.

| Measure at step 5 | Minimum | Larger |
| --- | ---: | ---: |
| Whole run | 106.8 s | 177.9 s |
| Sampled process-tree peak RSS | 5.16 GiB | 5.06 GiB |
| Candidate pool | 17,961 | 93,734 |
| Allocated mobile plan keys | 10,000 | 22,598 |
| Mobile plan keys with over 10 people | 8,543 | 16,904 |
| Mobile demand-unit plan rows | 174,705 | 332,936 |

Candidate identities and modeled allocation keys describe different things;
their counts are not a share. To compare flows, use the sum of the absolute
differences in each cell divided by the larger run's total trips, including
cells absent from either run. Results over all five steps were:

| Step | Mode totals | Activity totals | OD and mode | OD, mode and activity |
| --- | ---: | ---: | ---: | ---: |
| 1 | 21.6% | 21.3% | 147.1% | 162.5% |
| 2 | 19.8% | 19.2% | 143.3% | 161.6% |
| 3 | 18.5% | 17.9% | 139.1% | 159.1% |
| 4 | 17.8% | 16.5% | 133.7% | 155.8% |
| 5 | 16.1% | 15.7% | 131.0% | 153.1% |

| Step | Minimum candidates | Larger candidates | Minimum allocated mobile plans | Larger allocated mobile plans |
| --- | ---: | ---: | ---: | ---: |
| 1 | 4,024 | 23,370 | 2,571 | 6,269 |
| 2 | 8,972 | 54,346 | 4,993 | 11,667 |
| 3 | 13,131 | 71,389 | 6,940 | 16,421 |
| 4 | 15,938 | 84,156 | 8,526 | 20,108 |
| 5 | 17,961 | 93,734 | 10,000 | 22,598 |

Mobile plans carrying more than 10 people numbered 2,426, 4,563, 6,222,
7,472 and 8,543 in the minimum run, and 5,196, 9,552, 12,992, 15,446 and
16,904 in the larger run, respectively.

The difference measure can exceed 100%. The detailed cells are sparse; neither the larger
run nor one seed establishes the true distribution. The results indicate a
need to examine spatial aggregation, additional seeds and a heavier reference
before treating the differences as convergence or model accuracy.

Set `MOBILITY_PROBE_FULL_REPLANNING=1` and `MOBILITY_PROBE_EFFORT_METRICS=1`
to reproduce the capture. The probe prints its output folder; compare its
`effort-metrics` directories with `tests/manual/compare_effort_metrics.py`.
The optional measurements add runtime and can increase peak memory. In that
folder, `search-diagnostics.parquet` includes the overall and per-mode and
per-activity flow-change indices and trip totals for each batch and step.
`transitions.jsonl` records people who changed modeled plans, were reassigned
because their original plan was infeasible, or were reassigned by the floor.
These categories can overlap. Forced reassignments cannot be separated using
the event adjustment factor when the revision probability is 1, so that
field is null in that case.

`candidate-batches.jsonl` counts candidate identities first observed in the
current step; the first batch also contains plans retained from earlier steps.
`allocated-options.jsonl` shows, at each batch, how many context-and-plan
allocations and how many represented people belong to options first allocated
in each batch. First-batch allocations also include previously occupied plans.
These are separate measurements: modeled timings can merge candidate identities
and the allocated volume cannot be attributed reliably to individual newly
generated candidates from these tables. Search estimates do not apply another
behavioral transition to the outer state.


### 81-point effort reference

A third run used one scenario and seed with five full-replanning steps and
three activity, destination and mode draws and three search batches: 81 rough
effort points per step. It reused input caches, used eight Polars threads and
no memory limit, and recorded the same flow and plan-pool metrics. It completed
in 525.6 s with 12.12 GiB sampled process-tree peak RSS. Mean sampled
process-tree CPU was 493% (100% is one logical CPU). Per-step wall times were
33.2, 102.6, 121.1, 121.5 and 120.7 s; initialization took about 26.5 s.

| Step | 1 vs 81: mode | 8 vs 81: mode | 1 vs 81: OD/mode/activity | 8 vs 81: OD/mode/activity |
| --- | ---: | ---: | ---: | ---: |
| 1 | 32.6% | 13.8% | 149.9% | 151.3% |
| 2 | 31.6% | 13.9% | 150.4% | 149.1% |
| 3 | 30.3% | 13.8% | 147.4% | 143.0% |
| 4 | 28.3% | 11.9% | 146.6% | 140.8% |
| 5 | 26.0% | 11.0% | 145.3% | 138.8% |

At step 5, relative absolute differences versus the 81-point run were 21.2%
and 8.7% for activity totals and 124.8% and 112.7% for OD-by-mode flows, for
the 1- and 8-point runs respectively. The reference candidate pool was
485,174 candidate identities; 39,916 modeled mobile plan keys carried
people, of which 25,629 carried more than 10. These are different identities
and their counts are not a percentage used. Total represented trips at step 5
were 8.56, 9.82 and 10.56 million for 1, 8 and 81 points respectively.

This is one seed, not a converged or validated ground truth. Changes in
sampling also affect later congestion and allocations, while very sparse OD
cells make exact OD agreement demanding. Comparing several seeds and coarser
zone aggregations is needed before drawing conclusions about convergence.


### Three effort runs with convergence diagnostics

All cases used one scenario, one seed, five full-replanning steps, a 40%
revision probability and the one-person allocation floor. Input caches were
reused, Polars used eight threads, and no memory limit was set. Diagnostics
were enabled, so the times include their additional aggregation and capture.

| Effort per step | Runtime | Sampled process-tree peak | Step 5 candidate pool | Step 5 allocated mobile plan keys |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 78.9 s | 4.96 GiB | 17,961 | 10,000 |
| 8 | 145.7 s | 5.30 GiB | 93,734 | 22,598 |
| 81 | 521.5 s | 12.98 GiB | 485,174 | 39,916 |

The symmetric OD/mode/activity flow-change index is the sum of absolute
cell changes divided by the combined flow volume. Inner values compare
successive estimates from the fixed pre-step state; outer values compare
successive completed behavioral allocations.

| Step | Outer 1 point | Outer 8 points | Outer 81 points | 8 points batch 1 to 2 | 81 points batch 1 to 2 | 81 points batch 2 to 3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | ? | ? | ? | 45.87% | 38.93% | 21.14% |
| 2 | 41.73% | 43.33% | 44.89% | 17.31% | 11.49% | 9.45% |
| 3 | 31.39% | 34.27% | 36.28% | 9.61% | 6.96% | 5.76% |
| 4 | 27.63% | 30.29% | 34.11% | 7.44% | 4.69% | 3.96% |
| 5 | 26.74% | 28.94% | 33.65% | 5.29% | 4.01% | 3.23% |

At step 5, the 81-point run's last batch changed flows by 4.23% for bicycle,
3.30% for car, 2.97% for walk and 3.41% for public transport. The shares of
people changing modeled plans in steps 1?5 were 38.86%, 35.19%, 32.75%,
31.15%, 30.54% at 1 point; 39.98%, 37.82%, 36.42%, 35.35%, 35.22% at
8 points; and 40.00%, 39.49%, 39.04%, 39.77%, 40.17% at 81 points.
Floor reassignment in step 5 involved 0.45%, 1.52% and 4.41% of the
population respectively, and can overlap with plan changes. No forced
reassignments were recorded.

At step 5, batch 3 of the 81-point run had 50,467 candidate identities not
previously seen in that step. Modeled options first allocated in batch 3
accounted for 80,696 people in its estimate, about 4.1% of the 1,966,565
mobile people. These are distinct measures: candidate identities can merge
when modeled timings are applied and cannot be attributed directly from the
captured data. The 81-point flow outputs matched the previous run at all five
steps to the displayed precision. A single seed does not establish convergence.


### Ten-step revision-probability comparison

Two cache-only Geneva runs used one scenario, seed 0, ten full-replanning
steps, one activity/destination/mode draw, one search batch and a one-person
plan floor. Their only intended model parameter difference was revision
probability, 0.4 versus 1.0. Both used eight Polars threads and no memory
limit; the probe saved diagnostics, costs and saturation each step. Runtimes
were 139.1 and 134.2 s; sampled process-tree peaks were 5.20 and 5.33 GiB.

The symmetric OD/mode/activity flow index is shown as a percentage below.
One-step compares consecutive completed steps; two-step compares steps two
apart. The latter can reveal alternation but zero is not guaranteed in an
unstable system with sampling.

| Step | 0.4: one-step | 0.4: two-step | 1.0: one-step | 1.0: two-step |
| --- | ---: | ---: | ---: | ---: |
| 2 | 41.73 | ? | 94.33 | ? |
| 3 | 31.39 | 56.81 | 91.61 | 75.53 |
| 4 | 27.63 | 42.79 | 93.66 | 64.73 |
| 5 | 26.74 | 36.45 | 94.01 | 57.24 |
| 6 | 24.90 | 32.39 | 93.21 | 58.07 |
| 7 | 24.93 | 30.22 | 93.11 | 49.57 |
| 8 | 24.18 | 28.50 | 92.69 | 50.46 |
| 9 | 24.52 | 27.42 | 93.49 | 44.15 |
| 10 | 24.82 | 27.70 | 93.86 | 46.95 |

At revision 1.0, represented car trips alternated between 7.53 million at
step 9 and 1.12 million at step 10; walk trips changed from 2.85 to 5.52
million. At 0.4, car trips were 4.57 million at both steps 9 and 10, and walk
trips were 3.97 and 4.08 million. Step-10 mode-total flow change was 0.76%
at 0.4 and 54.05% at 1.0. At the same step, the 0.4 and 1.0 distributions
differed by 32.93% across mode totals and 80.95% across OD/mode/activity.
These normalized symmetric indices are not accuracy measures.

The saved OD generalized-cost table at revision 1.0 contained NaN values
for 1,434, 715, 1,413 and 1,667 of 1,070,826 OD rows at steps 4, 6, 8 and
10 respectively; none were found in this table for the 0.4 run. The cost
table is not a direct measure of road congestion alone. The oscillation and
non-finite values merit separate investigation before interpreting this
run as a stable model result or attributing them to a specific feedback.


### Intermediate revision rate and non-finite OD costs

A matching lowest-effort, one-scenario, ten-step run at revision probability
0.7 took 130.8 s with a 5.23 GiB sampled process-tree peak. Step-10 symmetric
OD/mode/activity change from the previous step was 51.60%, compared with
24.82% at revision 0.4 and 93.86% at 1.0. Its step-8-to-10 change was
39.14%, and its mode-total change from step 9 to 10 was 6.27%. The 0.7 run
also showed alternating car volumes: 5.06 million trips at step 9 and
4.39 million at step 10. Its saved OD cost tables contained no NaNs. All
three comparisons were run before the numerical fix below and use one seed.

A targeted four-step replay of revision 1.0 inspected OD 634 to 495 at step 4.
Its only available mode was car with a finite generalized cost of 1727.65.
The unshifted `exp(-cost)` underflowed to zero, so normalizing the weights
produced 0/0. Both OD expected-cost implementations and the standalone mode
probability method now shift by the minimum OD cost before exponentiating,
matching the stable calculation already used for profile-based destination
sampling. Run, checkpoint and destination-sequence identities were changed to
prevent reuse of outputs computed under the old calculation. This fix removes
the demonstrated numerical failure; it does not establish the cause or remove
the observed congestion oscillation.

A four-step, lowest-effort replay of revision 1.0 after the fix returned a
finite cost of 1727.65 for OD 634?495 at step 4, with zero NaNs in its saved
OD cost tables for all four steps. Its OD/mode/activity flows matched the
pre-fix run through step 4 to floating-point precision, including the
alternation. This does not prove that the fix cannot affect later steps.


### Where the 0.4 revision run still changes

Using the saved ten-step, one-point Geneva run, the symmetric flow-change
index from step 9 to 10 was 0.46% for total trips, 0.76% by mode, 0.91% by
activity, 9.42% by origin, 9.42% by destination, 21.71% by OD, 22.27% by
OD/mode, 24.51% by OD/activity, and 24.82% by OD/mode/activity. The OD-only
change therefore remains large without any mode or activity distinction.
These nested aggregates are not a causal decomposition of individual moves.

Replacing both zone IDs by their local administrative unit reduces the
step-9-to-10 index to 13.53% for OD and 18.51% for OD/mode/activity.
Replacing them by the 5 km grid cell containing each zone centroid gives
9.82% and 15.68% respectively. The 1,110 transport zones map to 753
administrative units and 372 occupied grid cells. Aggregation allows changes
within the same area to cancel; it does not measure the distance between the
old and new destinations. Zone mapping was complete for these flows.

The OD-only series from steps 1 to 10 was 38.67%, 28.29%, 24.61%, 23.74%,
21.98%, 21.89%, 21.09%, 21.52% and 21.71% for successive changes.
Thus the detailed change had flattened over the last few observed steps.
Total opportunity occupation changed from 15.26 to 15.51 million hours; its
activity-by-zone symmetric change was 22.50%. Across all available OD pairs,
the mean expected generalized cost was 7.46 at step 9 and 7.52 at step 10.
The mean absolute change was 0.40 unweighted, or 0.057 when weighted by the
average trip volume of each OD over those two steps. Those expected costs
combine modes and do not isolate the impact of road congestion.

The saved outputs cannot attribute changes between new candidate sampling,
road congestion, opportunity feedback and the 40% revision rule. A controlled
comparison that holds candidate generation or costs fixed is needed. This
analysis describes the original pre-numerical-fix 0.4 run and one seed.


### Twenty-step lowest-effort trajectory

A cache-only Geneva run used one scenario, seed 0, 20 full-replanning
steps, revision probability 0.4, one activity/destination/mode draw and one
search batch. It used eight Polars threads and no memory limit. The run took
259.2 s with a 5.31 GiB sampled process-tree peak, including diagnostics.
The first ten steps reproduce the earlier lowest-effort run to the displayed
precision.

| Step | Detailed flow change | OD flow change | Mode-total change | People changing plans | Candidate pool |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 | 26.74% | 23.74% | 3.45% | 30.54% | 17,961 |
| 10 | 24.82% | 21.71% | 0.76% | 29.98% | 23,088 |
| 15 | 24.26% | 21.00% | 0.48% | 29.62% | 24,386 |
| 20 | 24.18% | 20.73% | 0.61% | 29.79% | 24,778 |

Flow changes compare consecutive completed steps using the symmetric index.
Detailed OD/mode/activity change fluctuated between 24.16% and 24.74%
through steps 11?20; it did not trend toward zero. Mode-total change stayed
below 1.5%, but represented trips still increased from 9.48 million at
step 10 to 10.24 million at step 20. Candidate generation, congestion,
opportunity feedback and behavioral revision all remained active; this run
does not identify which causes the persistent detailed change. One seed and
one effort setting do not support fitting a general effort/convergence law.


### Road and opportunity feedback at low effort

Four one-scenario, seed-0 cases used five full-replanning steps, revision
probability 0.4, one activity/destination/mode draw and one search batch.
The both-enabled case uses the first five steps of the 20-step post-fix run.
All used cached inputs and eight Polars threads, with no memory limit.
Road feedback was disabled with `n_steps_per_cost_update=0`. Opportunity
feedback was disabled by neutralizing utility and sampling effects while
continuing to calculate occupation. The newer public switch
`plan_update.enable_opportunity_congestion=False` reproduced the neutralized
both-off run's OD/mode/activity flows to displayed precision for all steps.

| Step | Both off | Road only | Opportunity only | Both on |
| --- | ---: | ---: | ---: | ---: |
| 2 | 32.77% | 33.30% | 42.99% | 41.73% |
| 3 | 19.85% | 22.09% | 33.12% | 31.39% |
| 4 | 15.38% | 16.55% | 29.22% | 27.63% |
| 5 | 12.73% | 14.25% | 27.60% | 26.74% |

Values are the symmetric OD/mode/activity flow-change index versus the
previous completed step. Shares of people changing plans at step 5 were
18.18%, 20.00%, 31.40% and 30.54%, respectively. Represented step-5 car
trips were 9.71m, 5.03m, 8.41m and 4.66m; walk trips were 0.98m, 3.78m,
0.76m and 3.12m. Thus road feedback greatly changes mode totals even when it
adds less to this particular stability index. Opportunity feedback is
associated with much greater continuing flow movement in these cases, but
the cases have different trajectories and candidate sets; these results do
not identify a single underlying mechanism or establish long-run convergence.

The three new runs took 49.9, 72.9 and 49.5 seconds respectively, including
diagnostic capture. A separate both-off verification using the new public
switch took 51.1 seconds.


### Fixed-state fresh-MNL batch experiment

A cache-only Geneva probe used one default scenario and seed, the initial
stay-home state, one outer step, fixed initial costs and opportunities, and
one activity/destination/mode draw per batch. It ran ten batches, preventing
the existing convergence rule from stopping the search early. The normal
adaptive scheduler still used reachable allocation to select requests; this
is an observational comparison, not a replacement production algorithm.
The fresh MNL estimate was evaluated after each batch without applying
revision, minimum utility gain or the per-subgroup allocation floor.

| Batch | Candidate pool | Fresh-MNL flow change | Fresh-MNL plan allocation change | Reachable flow change | Fresh-MNL flow difference from batch 10 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4,024 | ? | ? | ? | 74.80% |
| 2 | 7,213 | 43.35% | 48.88% | 43.43% | 61.09% |
| 3 | 10,395 | 25.29% | 29.88% | 25.71% | 52.42% |
| 4 | 13,560 | 18.97% | 22.89% | 19.18% | 45.18% |
| 5 | 16,706 | 15.38% | 18.57% | 15.24% | 37.43% |
| 6 | 19,869 | 14.11% | 16.67% | 14.42% | 29.13% |
| 7 | 23,061 | 11.91% | 14.15% | 12.38% | 21.17% |
| 8 | 26,233 | 9.70% | 11.93% | 9.97% | 13.95% |
| 9 | 29,441 | 8.27% | 10.45% | 8.04% | 6.82% |
| 10 | 32,578 | 6.82% | 8.55% | 6.76% | 0.00% |

Percentages use the symmetric absolute-change index, with OD/mode/activity
flows or context-and-modeled-plan allocations as appropriate. The last column
uses batch 10 of this same trajectory as a comparison, not as ground truth.
The 0.4 revision setting and initial stay-home state make the reachable and
fresh relative flow changes similar in this particular probe; they need not
agree in later steps or restricted behavior phases.

The experiment took 67.5 s with a 5.11 GiB sampled process-tree peak,
including extra allocation calculations and saved diagnostic tables. Fresh
MNL allocation alone took about 0.018?0.096 s per batch, and the reachable
allocation about 0.025?0.048 s; those are not complete alternatives for
the whole search, and this starting state has just one occupied plan per
demand unit. The test does not establish a general performance advantage.

Fresh MNL allocated about 97.7% of people to mobile plans in batch 1 and
99.994% by batch 10. Adding sampled alternatives can mechanically change
MNL probabilities, and stability of successive batches does not demonstrate
that the sampled plan set represents the complete distribution. Evaluate
several seeds, larger pools, mature states, and sampling/choice-set bias
before replacing the production stopping rule.

Set `MOBILITY_PROBE_MNL_BATCHES=1` to capture the diagnostics; the probe
sets the required convergence passes to the batch limit but may still stop
if generation requests are exhausted. It saves per-batch flows and
context-plan allocations in its printed temporary output folder.


### Higher effort per fixed-state search batch

The fresh-MNL experiment was repeated with the same one-step initial state,
default scenario and seed, but two activity, two destination and two mode
draws in each of ten batches (eight rough points per batch, versus one).
Both runs used eight Polars threads and no memory limit; both retained the
production scheduler, whose generation priorities use reachable allocation.
The higher-effort diagnostic run took 118.9 s with a 5.31 GiB sampled
process-tree peak, including saved flows and plan tables.

| Batch | Low: candidates | High: candidates | Low: fresh flow change | High: fresh flow change | Low: plan change | High: plan change |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 7,213 | 44,594 | 43.35% | 40.51% | 48.88% | 48.02% |
| 4 | 13,560 | 82,364 | 18.97% | 18.19% | 22.89% | 23.40% |
| 6 | 19,869 | 120,120 | 14.11% | 9.84% | 16.67% | 13.91% |
| 8 | 26,233 | 157,736 | 9.70% | 9.55% | 11.93% | 12.65% |
| 10 | 32,578 | 195,305 | 6.82% | 5.94% | 8.55% | 8.32% |

Changes use the symmetric absolute-change index relative to the preceding
batch, and are not monotonic. The high batch-10 pool is not an established
truth. Comparing the two final flow tables gives 3.83% by mode totals, 4.41%
by activity totals, 53.65% by OD, and 68.43% by OD/mode/activity. Using
local administrative units for both ends gives 42.44% for OD/mode/activity;
using 5 km zone-centroid cells gives 30.18%. These comparisons do not match
local plan IDs across runs. Aggregation permits within-area changes to cancel
and does not measure how far trips moved. One seed cannot separate sampling
noise from systematic effort effects.

The observed reduction in consecutive-batch change with higher batch effort
is modest compared with its cost and does not establish convergence. Modeled
flow distributions still differ sharply at detailed OD level. With a changing
candidate pool, a small next-batch change alone is not evidence that all
important options were sampled.


### Contributors to fresh-MNL batch-to-batch flow change

This analysis uses the one-step, lowest-effort, ten-batch fixed-state probe
above. For each transition between batches, each OD/mode/activity cell
contributes `abs(new volume - previous volume)` to the numerator of the
symmetric flow-change index. The denominator is the sum of both flow totals.
The table reports shares of the numerator, not percentages of all trips.

| New batch | Flow change | Largest 10 cells' share | Largest 100 cells' share | New flow cells' share | Car cells' share |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 43.35% | 2.48% | 12.98% | 46.3% | 90.4% |
| 3 | 25.29% | 3.02% | 14.62% | 39.5% | 90.7% |
| 4 | 18.97% | 4.27% | 18.59% | 31.0% | 91.4% |
| 5 | 15.38% | 4.33% | 20.51% | 26.1% | 86.4% |
| 6 | 14.11% | 5.27% | 23.96% | 25.6% | 89.1% |
| 7 | 11.91% | 4.03% | 20.77% | 21.7% | 90.3% |
| 8 | 9.70% | 4.39% | 22.80% | 23.4% | 90.0% |
| 9 | 8.27% | 5.59% | 22.90% | 20.9% | 90.5% |
| 10 | 6.82% | 4.56% | 20.22% | 17.8% | 92.1% |

At batch 10, 82.2% of absolute change was in previously nonzero flow cells,
and 17.8% in newly nonzero cells. A flow cell is not a plan: new plans can
change existing cells, and existing plans can lose weight when MNL is
renormalized. Car accounted for 92.1% of the absolute change while car made
up about 88.4% of the batch-10 flow volume. By destination activity, the
change shares were home 36.7%, other 20.3%, shopping 20.0%, leisure 16.8%,
studies 5.4% and work 0.8%. The largest 10 origin zones accounted for 11.4%
of the numerator. Origin means a trip leg origin, not necessarily home.

The largest cell at batch 10 was a new 13,347-person-trip car/home flow from
zone 909 to 907, both in Le Grand-Saconnex; it accounted for 0.74% of the
numerator. A new 10,010-person-trip car/shopping flow in the reverse
direction was next. These are changes to aggregate expected trips; the output
does not trace which individual plans or populations caused them. About 33.3%
of the batch-10 absolute change was intrazonal, versus 39.1% of flow volume.
The expected work-trip total decreased from about 771,000 at batch 2 to
336,000 at batch 10, while total trips rose from 11.09 to 13.30 million.
These findings describe one seed and a fixed initial state and do not prove
the sampler's behavior at other states or scenarios.


### Largest-subgroup fresh-MNL plan trace

A repeat of the one-step, lowest-effort, ten-batch run captured one
largest demand subgroup. Several subgroups tie near the 100-person cap; the
selection was deterministic by size and IDs. It selected demand group 8665,
subgroup 0, representing 99.999986 people, in home zone 328
(Injoux-G?nissiat). Its shared scoring context 2946 represents 174.54 people;
the percentages below are shared-context MNL probabilities, scaled to this
subgroup when expressed as persons. They are target shares, not actual outer
transitions.

| Final rank | Plan ID | Utility | First batch | Share batch 1 | Share batch 5 | Share batch 10 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10612 | 97.048 | 1 | 62.03% | 49.71% | 31.54% |
| 2 | 10611 | 95.001 | 1 | 37.18% | 29.80% | 18.91% |
| 3 | 210570 | 93.344 | 8 | ? | ? | 12.49% |
| 4 | 157573 | 91.294 | 6 | ? | ? | 7.48% |
| 5 | 183987 | 88.711 | 7 | ? | ? | 3.92% |
| 6 | 104853 | 87.926 | 4 | ? | 5.08% | 3.22% |
| 7 | 183988 | 87.924 | 7 | ? | ? | 3.22% |
| 8 | 131355 | 87.129 | 5 | ? | 4.16% | 2.64% |
| 9 | 210571 | 86.992 | 8 | ? | ? | 2.55% |
| 10 | 78469 | 86.744 | 3 | ? | 3.78% | 2.40% |

All 31 mobile plans in batch 10 used car; the 32nd option was stay-home,
with 0.015% of the target. This run requested one mode sequence per chain,
so the result does not establish the full mode-choice distribution. The top
ten account for 88.39% of MNL probability. No previously available plan
disappeared and the utilities of shared plans stayed exactly constant in
this trace. The top two were already present in batch 1; their joint share
fell from 99.21% to 50.45% as additional alternatives entered the MNL
normalization. In batch 8, three first-seen plans together took 16.24% of
probability; the new rank-3 plan alone took 13.16% initially.

The trace also exposed a schedule-feasibility issue. Rank-3 plan 210570's
fourth step is shopping: its next departure is 2.83 minutes before modeled
arrival. Rank-9 plan 210571's first shopping step departs 20.83 minutes
before modeled arrival. The schedule updater floors activity duration at
0.001 hours, rounded in the saved timings to four seconds, rather than
rejecting or repairing these overlapping schedules. Feasibility in the
scoring path checks finite travel costs, not this timing conflict. Across
all 32 choices, eight plans have at least one negative raw duration for a
non-home activity and together carry 17.64% of this subgroup's fresh-MNL
probability. The final home step's zero duration is a separate convention
and was excluded from that count. This is a trace of one tied subgroup,
not an estimate of prevalence across the population.

### Utility-guided destination sampling experiment

After fixing the schedule feasibility check, we compared the default radiation
sampler with the optional fastest/cheapest utility guide. Both used one Geneva
scenario, one full-replanning step, ten inner batches, one activity,
destination and mode draw per batch, seed 0, eight Polars threads and disabled
road and opportunity congestion. The same diagnostic work was enabled in both.

| After batch 10 | Radiation | Utility guide |
| --- | ---: | ---: |
| Mobile candidate rows across scoring contexts | 271,286 | 271,351 |
| Infeasible candidate rows | 19.8% | 15.6% |
| Feasible rows below 1% of their context's best feasible mobile plan weight | 49.3% | 53.3% |
| Flow change from batch 9 to 10 | 7.09% | 7.80% |
| Choice change from batch 9 to 10 | 8.84% | 9.26% |
| Destination sampling, summed across batches | 18.21 s | 27.41 s |
| Whole probe wall time | 67.23 s | 75.93 s |
| Peak process RSS | 5.76 GiB | 5.42 GiB |

Rows count a candidate once per scoring context, not once per physical plan or
represented person. The low-weight measure is relative to each run's best
feasible mobile plan in the same context; its increase does not by itself prove
that absolute utilities are lower. The final fresh MNL OD/mode/activity flow
change index between the two methods was 36.79%, so their candidate pools lead
to materially different results at this effort. This single seed does not
establish which distribution is closer to a well-sampled reference.

The guide uses the reference activity duration as a proxy for the population's
usual duration and keeps only a preliminary cost-and-capacity shortlist. It is
optimistic about onward mode choice and does not enforce vehicle continuity.
These approximations, and sampling modes only after destinations, can explain
remaining infeasible and low-weight candidates. Neither result supports making
the guide the default without further study.

### Anchor-first utility guide with actual activity-duration profiles

We revised the optional utility guide to use population-weighted typical
activity durations by travel-cost profile, destination-country coefficients,
arrival-time rigidity and the impact on the next activity when it is the next
anchor. It compares fastest and cheapest modes for both legs. Other activities
are still drawn toward their already sampled next anchor. While drawing anchors,
the onward proxy remains home because later anchors are not yet known. A
capacity-and-cost shortlist still bounds work, but the utility-guided draw no
longer multiplies its weight by capacity, which is not a direct plan-utility
term. We removed redundant OD joins and the unused expected-mode-cost calculation.

Using the same one-step, one-scenario, ten-batch fixed-congestion setup as
above, with seed 0 and one draw of each type per batch:

| After batch 10 | Radiation | Revised utility guide |
| --- | ---: | ---: |
| Mobile candidate rows across scoring contexts | 271,286 | 271,180 |
| Infeasible candidate rows | 19.81% | 11.28% |
| Feasible rows below 1% of their context's best feasible mobile plan weight | 49.26% | 57.96% |
| Population-weighted best feasible utility, including stay-home | 110.16 | 111.23 |
| Population-weighted fresh-MNL mean utility | 107.52 | 108.57 |
| Flow change from batch 9 to 10 | 7.09% | 7.55% |
| Destination sampling, summed across batches | 15.20 s | 19.76 s |
| Whole probe wall time | 60.68 s | 62.82 s |
| Peak process RSS | 5.75 GiB | 5.76 GiB |

For 44.2% of represented people, the best feasible utility was higher by more
than 0.01; for 29.1% it was lower by more than 0.01, and 26.7% were within
0.01. This comparison joins the same scoring contexts across both runs. The
low-weight share is relative to each method's own best mobile plan, so a
higher share can reflect discovering better plans rather than worse absolute
utilities. The two batch-10 fresh-MNL OD/mode/activity distributions differed
by 50.49% on the flow-change index. Neither is a known reference: higher best
utility alone does not prove better coverage of the whole choice distribution.

One earlier version retained a destination-capacity multiplier in the draw;
it produced 14.91% infeasible rows and a mean best utility of 109.89. Removing
that multiplier improved these two measures in this run. An optimization that
removed the unused expected-cost calculation reproduced all 250,493 batch-10
choice rows and allocated volumes exactly. A final repeat after deterministic
duration aggregation also reproduced those rows and volumes. Timings are
single-run measurements and vary between repetitions.

### Rigidity-aware timing filter

For utility-guided sampling, a non-anchor stop directly before its known anchor
can be checked before computing destination utilities. Using the fastest
available incoming and onward travel times and both activities' arrival-time
rigidities gives an optimistic activity window. If even that window ends more
than two seconds before it starts, the destination is excluded. The margin
protects modeled second-level rounding. The check is skipped when the next
stop is not the known anchor or a mode with finite cost has an unknown time.
It does not constrain anchor sampling, where intervening stops are not yet known.

In a separate first-batch diagnostic, the check removed 446,181 of 1,554,028
stop/destination/context combinations (28.7%) before utility scoring. For
stops immediately before an anchor, the reduction was 50.3% for shopping,
42.4% for other activities and 22.4% for leisure. These are combinations
examined by the sampler, not distinct complete plans or represented people.

| After ten batches | Utility guide before filter | With timing filter |
| --- | ---: | ---: |
| Mobile candidate rows across scoring contexts | 271,180 | 264,723 |
| Infeasible candidate rows | 11.28% | 3.66% |
| Population-weighted best feasible utility | 111.23 | 111.29 |
| Population-weighted fresh-MNL mean utility | 108.57 | 108.58 |
| Flow change from batch 9 to 10 | 7.55% | 7.55% |
| Destination sampling, summed across batches | 19.76 s | 20.36 s |
| Whole probe wall time | 62.82 s | 64.66 s |

The batch-10 flow change index between the two methods was 2.21%. The filter
did not reduce measured runtime in this single comparison: it still joins
both legs before it can filter and adds a check. Across ten batches, 614
incomplete destination-sequence draws were dropped when some stop had no
candidate left in its shortlist. This does not prove the schedule is globally
infeasible: another destination outside the shortlist, another anchor choice or
another travel-cost profile might work. The shared pool also means a plan not
sampled for one profile can otherwise be useful to another.

A separate manual probe retained fastest/cheapest OD options across batches,
keyed by scenario, step and profile. It reproduced all 264,945 batch-10 choice
rows and allocations exactly, and destination sampling fell from 20.33 to
17.73 seconds in those two runs. Three profiles retained 128.5 MB (decimal)
in one scenario.

The utility sampler now reuses these tables during each inner search. The cache
is shared across the scenario cohort, retains at most 256 MiB, and uses the
scenario, iteration transport-cost asset and resolved profile assets as its
key. If a table will not fit, it is recomputed; the simulation is not stopped.
The cache is cleared at the end of the inner search, before the next outer step
or a congestion update. Cache contents do not enter simulation identity.

In the same ten-batch run, the production cache built three option tables
instead of fifteen and reduced destination sampling from 20.36 to 17.29 seconds;
all 264,945 batch-10 choice rows and allocated volumes matched exactly. Whole
run time was 64.66 seconds without the cache and 61.95 seconds with it. These
are single timings. In a separate two-step, two-batch run with road and
opportunity congestion enabled, the cache was rebuilt after the road-cost
update. Cached and uncached batch-2 choice and flow tables matched exactly;
trip counts differed by at most 1.9e-9 due to floating-point aggregation.

### Earlier departures and scenario cache sharing

The utility guide now adds the change in preceding-activity utility caused by
an incoming travel-time change. For the first trip it estimates the change in
home-night utility. Both terms use the current activity's arrival rigidity and
profile-average typical durations. The preceding activity's arrival is held at
its reference time; when sampling anchors before intermediate stops, the
preceding destination can be unknown. The term is skipped for zero-rigidity
activities or when modeled timing updates are disabled. This shortcut exactly
reproduced 265,089 batch-10 choice rows and volumes in the fixed-cost probe.

| After ten batches | Previous guide | Including preceding activity |
| --- | ---: | ---: |
| Infeasible mobile candidate rows | 3.66% | 3.62% |
| Population-weighted best feasible utility | 111.292 | 111.355 |
| Population-weighted fresh-MNL mean utility | 108.579 | 108.645 |
| Flow change from batch 9 to 10 | 7.55% | 7.50% |
| Destination sampling, summed across batches | 17.29 s | 19.82 s |
| Whole probe wall time | 61.95 s | 65.19 s |

The batch-10 OD/mode/activity flow change index between these runs was 1.35%.
For 1.82% of represented people the best feasible utility increased by more
than 0.01, for 0.29% it decreased by more than 0.01, and 97.89% were within
0.01. These are one seed and one fixed-cost scenario; they do not establish
which candidate distribution is closer to a complete reference.

An earlier diagnostic compared the preceding-activity estimate with the
original survey schedule. That comparison is not reliable after a candidate
has already acquired modeled timings: the incoming schedule for a later batch
can differ from its survey reference. The corrected diagnostic uses the
evaluation's incoming-to-modeled time mapping and excludes ambiguous mappings.

A separate two-batch, three-scenario probe compared the default scenario, an
identical baseline copy and a carbon-tax scenario changed from step 1. The
identical scenarios shared candidate generation at the asset level; two cost
scenarios built four distinct 42.8 MB option tables across their active
profiles in that probe. Identical table contents were not observed across the
different cost scenarios. Thus a one-step lifetime alone would not bound peak
memory, and this small experiment does not justify removing the shared 256 MiB
retention limit. It also does not predict sharing across all possible scenarios.

### Estimated preceding arrival

The guide now estimates the arrival at the preceding activity using fastest
and cheapest options for its incoming trip, including their travel costs.
Sequential non-anchor sampling carries the previously sampled origin and
destination. During anchor-first sampling an intervening non-anchor location
is not yet known, so the last known anchor is used as a provisional location.
The first trip always starts at home; its home-night estimate does not require
a preceding-trip lookup. Unknown route times or unavailable OD options retain
the previous reference-arrival approximation. Mode continuity is still checked
only when the complete plan is evaluated.

Using the same one-scenario, one-step, ten-batch fixed-cost setup as above:

| After ten batches | Reference preceding arrival | Estimated preceding arrival |
| --- | ---: | ---: |
| Infeasible mobile candidate rows | 3.615% | 3.619% |
| Population-weighted best feasible utility | 111.355317 | 111.355315 |
| Population-weighted fresh-MNL mean utility | 108.645325 | 108.645902 |
| Flow change from batch 9 to 10 | 7.503% | 7.514% |
| Destination sampling, summed across batches | 19.82 s | 20.43 s |
| Whole probe wall time | 65.19 s | 64.69 s |

The flow-change index between the two batch-10 distributions was 0.036%.
These single-seed results do not establish whether either distribution is
closer to a fully sampled reference. Skipping the nonexistent preceding-trip
lookup for first trips preserved all 265,093 batch-10 choice rows and volumes
exactly in a repeat.

The first trip always departs from the home zone, including when the schedule
contains an explicit home activity. A separate cleanup stopped recomputing an
aggregate OD table that profile-based destination sampling does not use. It
removed ten expected-OD calculations totaling 1.44 seconds from the preceding
run's trace; all 265,093 batch-10 choice rows and volumes matched exactly. The
table above uses the run after this cleanup. Whole-run timings remain noisy.

For one work-heavy scoring context, the corrected diagnostic matched six
chosen plan/activity pairs to unambiguous incoming schedules. It compares
preceding-activity timing using the final context's utility coefficients and
the actual current-trip time, so it isolates the preceding-arrival estimate;
it is not the complete error in the sampler's profile-average score. Across
these pairs the person-weighted mean absolute duration discrepancy fell from
1.48 to 0.039 minutes, and the corresponding utility discrepancy fell from
0.0585 to 0.0016 units. The reconstructed location proxy matched the eventual
preceding OD in five of six pairs, representing 97.6% of their pair-weighted
volume. This is one selected context, not a population-wide accuracy estimate.
The diagnostic requires `MOBILITY_PROBE_RIGID_UTILITY_BATCH=10` and
`MOBILITY_PROBE_CAPTURE_TRAVEL_OPTIONS=1`.

In a separate first-batch location diagnostic, 1,097 rigid-anchor draws were
first trips, 257 had an anchor as their preceding activity and 319 had an
intervening non-anchor activity whose location was not yet sampled. These are
draw counts rather than people, and do not show how geographically close the
provisional and eventual locations are.

### Current radiation and utility runtimes

After removing the unused aggregate OD calculation from both paths, a fresh
one-scenario, one-step, ten-batch fixed-cost run measured:

| Timing | Radiation | Utility guide |
| --- | ---: | ---: |
| Destination sampling | 13.00 s | 20.43 s |
| Inner search | 34.08 s | 41.73 s |
| Candidate evaluation | 7.57 s | 7.57 s |
| Whole probe | 56.31 s | 64.69 s |

These steps overlap: destination sampling and candidate evaluation are part
of the inner search, which is part of the whole run. Both runs used one draw
of each type per batch, one seed, eight Polars threads and disabled road and
opportunity congestion. They are single timings with manual diagnostics
enabled. The batch-10 infeasible mobile candidate share was 19.81% with
radiation and 3.62% with the utility guide; this alone does not establish which
choice distribution is more accurate.

### Anchor-sampling collection diagnostic

A separate one-batch, one-scenario probe timed individual lazy-frame
collections inside anchor sampling. Both methods processed the same 1,097
requests at the first non-home anchor. Across all anchor positions:

| Collected work | Radiation | Utility guide |
| --- | ---: | ---: |
| Context–destination weight rows | 44,650 | 259,805 |
| Weight-building collections | 0.071 s | 1.111 s |
| Draw-winner collections | 0.020 s | 0.040 s |
| Other collections | 0.013 s | 0.011 s |

At the first anchor alone, radiation materialized 27,548 weight rows with
five columns in 0.009 s; the utility guide materialized 170,188 rows with
18 columns in 0.260 s. This probe includes instrumentation and is a single
timing. The radiation and utility methods use different shortlist rules,
and the utility method also separates contexts by timing and preceding-activity
information. Its weight calculation includes the modeled trip and activity
utilities. The trace does not break down the time among those joins, grouping
and arithmetic operations. Weight-row counts are not distinct physical plans.

### Destination sampling: useful plans per second

A fixed-state Geneva probe compared radiation and utility-based sampling with one scenario,
one outer step, ten inner batches, one draw of each kind, one seed, eight Polars threads,
and both road and opportunity congestion disabled. The pooled best utility for each
scoring context is the better of the two runs' ten-batch best values. This is a finite
sample benchmark, not the true best possible plan. The probe excludes its own diagnostic
work from the inner-search times below; full run times include initialization and diagnostics.

At an equal 32.9-second search budget, radiation completed ten batches and utility sampling
completed eight. Radiation had 60,651 feasible mobile plan-context combinations whose
unnormalized choice weight was at least 1% of the pooled best, versus 65,795 for utility
sampling. The population-weighted mean gap from that pooled best was 1.985 utility units
for radiation and 1.918 for utility sampling. These are plan-context combinations, not
unique physical plans or allocated people. The 1% cutoff is a relative utility threshold,
not a prediction that every qualifying plan receives a 1% choice probability.

At ten batches, radiation needed 32.9 seconds of search and utility sampling 42.3 seconds;
the latter reached a mean best-utility gap of 0.786. Radiation produced 53,729 infeasible
mobile plan-context combinations out of 271,286 (19.8%); utility produced 9,582 out of
264,777 (3.6%). Total process times were 57.2 and 67.0 seconds; peak process RSS was
5.89 and 5.69 GiB, respectively. These are single runs and do not establish variability
or how either method performs under congestion or other effort settings.

An experiment cached destination/activity values across batches to avoid repeatedly
extracting them from the OD table. It produced identical final allocations to the earlier
utility run, but destination sampling took 21.1 seconds versus 20.4 seconds before, so
the cache was removed. The comparison probe can be reproduced using
`tests/manual/compare_destination_sampling.py` and the `MOBILITY_PROBE_UTILITY_POOLS`
option of the Geneva manual probe.

### A simpler anchor score and a tighter shortlist

When none of the sampled anchors has a directly modeled next activity, the
onward travel time has no effect on the approximate utility. The sampler now
uses only the cheapest onward cost and does not group requests by unused next
activity timing. With the same seed and 0.9 shortlist cutoff, all final choice
rows and allocated volumes matched exactly in the ten-batch Geneva probe.
Anchor sampling took 8.64 seconds versus 9.22 seconds before; total destination
sampling took 20.25 versus 21.11 seconds. These are single timings, so the
size of the gain is uncertain.

A separate run kept the same model and sampling effort but reduced the existing
first-leg shortlist cutoff from 0.9 to 0.5. The cutoff is cumulative preliminary
weight, not a percentage of destinations. At ten batches, inner search excluding
diagnostic work took 38.91 versus 40.23 seconds, and destination sampling took
18.88 versus 20.25 seconds. Against the combined ten-batch best utility per
scoring context, the population-weighted best-utility gap was 0.603 versus
1.807 utility units. At a 38.91-second budget, the tighter cutoff completed ten
batches and retained 83,140 qualifying plan-context combinations; the original
completed nine and retained 66,722. Qualifying means feasible and within 1% of
the combined best's unnormalized choice weight, as above.

This does not establish that the tighter shortlist gives a better choice
distribution. Its ten-batch OD/mode/activity flow-change index versus the 0.9
run was 0.568; the radiation-versus-0.9 utility comparison was 0.506. These are
one-seed, fixed-state, low-effort runs, and a different sample naturally changes
flows. Check more seeds and distribution coverage before changing a default.
The manual probe accepts `MOBILITY_PROBE_DESTINATION_CUTOFF` for this comparison.

### Four utility-sampling optimization experiments

These probes used one Geneva scenario, seed 0, one outer step, ten inner
batches, one draw of each kind, eight Polars threads, and fixed costs with
road and opportunity congestion disabled. Timings are individual runs;
the diagnostic output is enabled for all comparisons and total search times
below exclude its separately measured work.

1. A first-trip-only score omitted the general preceding-activity join and
   calculation when every request was the first trip. Batch-10 allocations
   matched exactly. Anchor sampling was 8.56 seconds versus 8.64 seconds
   before, and total destination sampling was 20.88 versus 20.25 seconds.
   The extra branch was removed because the run did not show a useful gain.
2. The optimized Polars plan used common-subexpression elimination, but still
   performed several schedule-only activity lookups after joining each
   schedule to destination options. Moving typical duration, activity value,
   rigidity and reference travel time before that join preserved all batch-10
   allocations exactly. Destination sampling fell from 20.25 to 18.96 seconds;
   inner search fell from 40.23 to 39.16 seconds. This change was retained.
3. In saved Geneva OD tables, fastest and cheapest had the same time and cost
   for 97.4%, 97.4% and 93.1% of rows across the three utility profiles.
   Splitting identical and different options allowed the first set to be
   scored once and preserved batch-10 allocations. However, destination
   sampling increased from 18.96 to 22.43 seconds. Sharing and splitting the
   rows cost more than the saved arithmetic, so the split was removed.
4. An experimental first-pass cap retained the first eight destinations per
   origin/activity in the existing preliminary ranking before detailed utility
   scoring. This is an approximation, not an optimistic utility bound. It
   reduced destination sampling from 18.96 to 15.99 seconds and search from
   39.16 to 35.96 seconds. Against the combined ten-batch best per context,
   the population-weighted best-utility gap was 0.480 versus 1.601 for the
   uncapped run. At an equal 35.96-second budget it completed ten batches
   versus nine and retained 83,674 versus 69,514 qualifying plan-context
   combinations, using the 1% unnormalized weight threshold described above.
   The batch-10 OD/mode/activity flow-change index between the two runs was
   0.493. A better best utility does not prove the narrower sample represents
   the full choice distribution well. Do not change the default based on one
   seed and one fixed-state case. The one-off shortlist experiment was removed
   after measurement; it is not part of the sampler.

### What the eight-destination shortlist changes

A closer look compared the fixed-state seed-0, ten-batch utility runs above
with and without the eight-destination cap. All results in this section are
fresh MNL target allocations, not realized transition flows; both congestion
feedbacks were off. A separate uncapped seed-1 run provides a sampling-noise
comparison. Its 9,898 scoring contexts and their populations matched seed 0.

For one inspected profile, the existing 0.9 cumulative preliminary cutoff
retained 1,047,435 origin/activity/destination rows; the eight-destination cap
reduced this to 44,400 (4.2%). All 5,550 origin/activity groups were truncated,
and the original largest shortlist per activity ranged from 163 to 362. The
preliminary ordering uses `log(attraction) - cheapest_cost`; final detailed
sampling uses the subsequent utility score. The cap can therefore discard a
destination that the detailed score would prefer.

At batch ten, the uncapped and capped seed-0 targets had 13.73 and 13.93
million represented daily trips. Their symmetric flow-change index was 0.493
for OD/mode/activity, but only 0.0117 for mode totals and 0.0104 for activity
totals. The uncapped seed-0 versus uncapped seed-1 comparison had a much
larger detailed index of 0.763 (mode 0.0076, activity 0.0102). Thus a large
detailed index alone is not evidence that the cap caused worse results;
finite sampling and seed differences are already very large.

Geography showed a more directional difference. Volume-weighted straight-line
zone-centre distances, using the zone internal-distance proxy for intrazonal
trips, were 2.74 km for uncapped seed 0, 2.70 km for uncapped seed 1, and
2.13 km for capped seed 0. The seed-0 share under 2 km rose from 56.5% to
64.1%; represented trips with a proxy distance over 20 km fell from 107,227
to 14,041. These are geographic proxies, not network route distances. Work
trips' mean proxy fell from 6.44 to 3.83 km and their detailed flow-change
index was 0.897, though work accounts for a small share of the target trips.
Walking volume rose from 0.917 to 1.179 million and car volume fell from
12.768 to 12.710 million. Cross-border volume fell from 153,491 to 121,311,
but uncapped seed 1 had 111,449, so this comparison cannot establish a
systematic cross-border effect.

The cap's batch-10 mean best utility was 112.476 versus 111.355; mean chosen
utility was 109.756 versus 108.646. By represented population, the capped run
had a best utility higher by more than 0.01 for 48.9%, lower by more than 0.01
for 20.3%, and within 0.01 for 30.8%. The uncapped seed-1 means were 111.385
and 108.647. This supports the hypothesis that the cap targets attractive
short trips, but one paired seed does not establish that it improves coverage
of the full choice distribution. More seeds and heavier search, especially
for work and longer-distance travel, are needed before using it as a default.

### Direct home-zone survey-plan pool

An experimental builder reads survey schedules and constructs one candidate
per eligible schedule and single mode, with every origin and destination in
the home zone. It uses no activity, destination or mode search. Schedules are
restricted to the survey day/country and the context's allowed activities;
every non-home activity must have positive local opportunity capacity. The
experiment uses temporary IDs for one context and sends the resulting steps
through the ordinary candidate evaluator. It is not a persistent simulation
pool. Persistent sequence identities, scenario changes and behavioral scope
still need integration before using it in a simulation.

For Geneva's one-scenario initial state, 37,045 distinct weekday survey
schedules reduce to 58 country/activity-set combinations. Counting those sets
against all 9,898 scoring contexts, local opportunities and finite home-to-home
mode costs takes 0.33 seconds without expanding candidate rows. It yields
126,577,778 schedule/context pairs, 382,254,517 available-mode plan/context
candidates and 1,487,051,202 step rows before timing feasibility. Most contexts
have three available modes; 260 have four. These are compact counts, not
measurements of allocating the full pool in memory.

The prototype evaluated 21 selected contexts, spread across the population
size range in each country plus one previously examined context. This is a
diagnostic sample, not a representative estimate for Geneva. Preparing the
schedule/activity sets once took 0.028 seconds. Building the context-specific
rows took a median 0.008 seconds and ordinary evaluation a median 0.273 seconds
per context, after initial data and costs were loaded. The entire probe's
sampled Python peak RSS was 5.13 GiB, including initialization; it does not
measure the peak of a full pool. Checking finite mode costs first removed an
unavailable public-transport option in the three separately checked contexts;
comparing plans by physical content showed identical utilities and allocations
with or without the check. Reusing prepared survey schedules avoids rebuilding
the activity sets for every context.

For context 2946, the complete direct pool had 16,018 feasible alternatives
including stay-home. Ten batches of the existing utility sampler had 31, four
of which matched a direct plan by physical steps and had identical utility.
For the union, counted once by physical content, generated-only alternatives
held 0.0225% of fresh unrestricted MNL probability. Mean chosen utility rose
from 96.31 to 114.25 and trips per represented person from 4.45 to 7.26. The
union's OD/mode/activity flow-change index was 0.872. This comparison holds
costs and opportunities fixed, and is not an outer-step outcome.

All 21 sampled contexts had a higher best utility in the direct pool than in
the ten-batch generated pool. Even assuming no overlap, the generated-only
share of the combined fresh MNL was at most 0.217% in every sampled context;
the median was 0.0136%. The median number of direct plans required to retain
99% of fresh choice weight was 7,971 (maximum 25,825). For a hypothetical
100-person subgroup, plans each receiving under one person held a median
72.5% of fresh MNL weight. Dropping all such plans independently would lose
their combined mass. Those fresh shares are not transition allocations.

The actual full-replanning transition rules were also applied to a 100-person
stay-home origin in context 2946 using the same fixed choice sets: logit scale
0.25, revision probability 0.4, scaled utility window 1.0 and minimum gain
0.1. The generated-only pool moved 39.99 people to one mobile plan averaging
five trips; the union moved 40.00 people to five mobile plans averaging 9.00
trips. Applying the one-person allocation floor to this same hypothetical
origin removed none of those five targets. This is a single-origin sensitivity
check, not a simulated second iteration or a congestion response.

For pruning, comparing each direct plan against stay-home gives a conservative
upper bound on its fresh MNL probability in the current fixed state. At a
one-person threshold for 100 people, this bound identified only 37 plans out
of 776,694 feasible plans across the 21 contexts. Treating the first score as
a permanent upper bound would additionally require proving that later costs,
activity values, timings and opportunities cannot improve. The experiment
does not establish such a bound. Exhaustively adding survey schedules also
changes the choice set: unlike sampling, it does not use survey weights to
determine which schedules appear. High-utility plans in this experiment often
contain many activity episodes, so the resulting trip patterns merit further
investigation before introducing the pool by default.

The source scoring context can combine demand groups with different car
counts when their cost profile and other scoring keys match. In the first
context, the stored representative happens to say zero cars, but its members
include zero-, one- and two-or-more-car groups. These results should not be
interpreted as a choice estimate solely for zero-car households.

Reproduce the captures with `tests/manual/inspect_home_zone_pool.py`, the
per-context summary with `tests/manual/analyze_home_zone_pool.py`, and the
deduplicated union and transition check with
`tests/manual/compare_home_zone_pool.py`. The scripts use the existing
cache-only Geneva probe and its environment settings.

The same transition check was repeated for a hypothetical 100-person stay-home
origin in each of the 21 sampled contexts. The current Geneva utility window
kept a median of four mobile targets (maximum nine), and the one-person floor
removed none in that one-origin sensitivity. Each direct best utility exceeded
the best generated utility by at least 12.75, greater than the four-utility
transition window, so generated-only targets cannot pass that window for
these origins. This does not establish behavior for occupied mobile origins,
restricted replanning phases, other seeds, or after congestion. It does show why
the large fresh-MNL tail cannot be equated directly with what the current
behavioral transition actually retains.

For the same origin and fixed choice sets, the normal transition function was
then applied repeatedly for ten full-replanning steps, with the configured
revision probability, gain window and one-person floor. By step ten, 99.39 of
100 people were mobile in both cases. The generated-only pool retained one
mobile plan averaging five trips; the combined pool retained five mobile plans
averaging 9.05 trips. This holds the candidate sets and costs fixed throughout;
it does not simulate candidate discovery, congestion or scenario changes.

### Capping the direct pool to the top survey schedules

A follow-up experiment kept at most 10, 100 or 1,000 survey schedules per
scoring context, with each eligible single-mode plan for those schedules. It
used the same 21 fixed-state contexts and ten-batch utility-sampling baseline
as the previous section. Two rankings were compared:

* Survey weight: sum the survey plan weights by country and schedule, as in
  the existing schedule pool; filter activities and local opportunities first,
  then retain the highest weights. This is not a CSP-specific ranking.
* Home-zone utility: evaluate all eligible schedules' single-mode home-zone
  plans in a first pass, rank each survey schedule by its best feasible mode,
  then retain the top schedules and evaluate them normally. This is a fixed
  initial-state ranking, not a guarantee for future scenario or cost changes.

Applying a cap after local-opportunity and available-mode checks would reduce
Geneva's theoretical 382,254,517 plan/context candidates before timing checks
to 299,540 for k=10, 2,995,400 for k=100, or 29,953,748 for k=1,000. The
numbers count one context and mode per candidate; the stay-home alternative
is additional. Physical catalogue sharing can reduce storage, while infeasible
timings can reduce the final scored choice set.

In the 21 contexts, survey-weight k=10 had a median best-utility gap of 41.72
relative to the full home-zone pool and did not beat the generated pool's best
utility in any sampled context. Survey-weight k=100 beat the generated pool in
18 contexts; k=1,000 did so in all 21. The first-pass utility ranking matched
the full evaluator's score and ordering exactly in all 21 contexts for k=10,
100 and 1,000. It requires scoring all eligible home-zone plans once, even
though it retains only k schedules afterwards.

For a stay-home origin under the current Geneva full-replanning transition
window, utility-ranked k=10 included every target the full home-zone pool
would retain in all 21 contexts (at most nine targets per context). This is a
fixed-input, single-origin result, not evidence for restricted replanning,
forced reassignment or future cost changes. In context 2946, normal repeated
transitions on the deduplicated union of generated and utility-top-10 plans
produced the same five mobile targets and 9.05 trips per mobile person after
ten fixed-cost steps as the full home-zone pool. The unrestricted fresh-MNL
weights differ: utility-top-10 retained a median 32.2% of the full pool's
weight across the 21 contexts, top-100 65.9%, and top-1,000 92.5%.

The utility first pass uses the existing per-step scorer, timing update and
plan-utility formula. It avoids building the full modeled-time and plan index
until after selection, then the ordinary evaluator scores the retained plans.
A reusable index for the 54,724 existing survey timing sequences cost 0.033
seconds and about 1.1 MB to build; one context's physical plans, utilities and
allocations matched with and without reuse. A context-local scoring lookup can
also be reused between first pass and final evaluation while inputs are fixed.
The lookup is not valid across changed scenario costs or opportunity values.

In one cold 21-context run, the first-pass rank took a median 0.113 seconds per
context and the subsequent top-100 evaluation 0.054 seconds, excluding initial
input loading, mode-availability checks and candidate construction. This is
not a full-Geneva runtime measurement. By comparison, an uncapped evaluation
with the survey time index reused took a median 0.184 seconds in a separate
21-context run. Thus top-k substantially limits retained pool size but does not
by itself eliminate the initial work of scoring every schedule. The experiment
was run sequentially; its sampled Python peak RSS was about 5.14 GiB including
initialization. Some analysis overlapped the cold run, so timing comparisons
are indicative rather than controlled repetitions.

The manual probe accepts `MOBILITY_HOME_POOL_TOP_K_VALUES` for survey-weight
caps, `MOBILITY_HOME_POOL_UTILITY_K_VALUES` for the first-pass utility caps,
and the `MOBILITY_HOME_POOL_TIME_INDEX_CACHE` and
`MOBILITY_HOME_POOL_SCORE_CACHE` options for the fixed-input reuse experiment.
`tests/manual/analyze_home_zone_topk.py` compares retained plans with the full
pool and reports the tested transition targets. These remain manual experiments:
no top-k pool or ranking has been added to production simulation state.

A cold first-pass run, without a preceding full-pool evaluation, matched the
prewarmed rankings in all 21 contexts. For utility-top-10, all 21 sets of
feasible plan allocations also matched exactly. The cold first-pass ranking
had a median 0.113 seconds per context, followed by a 0.059-second top-10
normal evaluation using the just-built score lookup; the first availability
check and candidate-row construction add work. The precomputed time index is
reused only with the exact survey schedule table. Later scenario or congestion
changes require fresh step scores and may change which schedules rank highest.

### Full-Geneva home-zone ranking and lowest-effort simulation

The direct home-zone prototype was run over all 9,898 initial scoring contexts,
in batches of four, with eight Polars threads. It considered 382,254,517
plan/context combinations and 1,487,051,202 step rows before timing feasibility.
Building these batches took 39.6 s and ranking them took 637.7 s. The sampled
Python peak during the ranking was 5.24 GiB. The 21 contexts previously checked
against the full evaluator had no ranking differences. This is one measurement;
it does not include later normal evaluation or establish a future utility bound.

A separate experimental runner attached the selected top ten survey schedules
per context, with their available uniform modes, to the ordinary generated pool.
It checked four contexts against the simulation's actual initial scores before
running. The ranking was prepared with congestion disabled; the simulation
itself had road and opportunity congestion enabled. The prototype retains the
initial selection while later costs change. It is not a production feature and
its ordinary candidate-pool diagnostic does not include the directly attached
home plans.

One scenario and seed ran five full-replanning steps, revision probability 0.4,
one activity, destination and mode draw and one search batch (one rough effort
point per step). Input caches were reused and no memory limit was applied.
The home-pool run took 115.4 s after the separate ranking, versus 67.2 s for a
matching run without it. Sampled process-tree peak RSS was 7.72 versus 5.29 GiB.
Across the five steps, the candidate-evaluation trace totaled 27.6 versus 4.2 s;
these spans are nested within the overall runtime. The initial selection held
2,545,362 context-step rows, represented by 912,444 distinct physical step rows
after the run-local sequence IDs were assigned.

Step wall times with the home pool were 7.5, 28.8, 17.6, 18.7 and 19.1 s;
without it they were 3.5, 11.7, 8.8, 9.8 and 9.2 s. Two-second process-tree
CPU samples averaged 419% and 346%, respectively, where 100% is one logical
CPU. Initialization and final work are outside the step timings.

The choice results changed substantially. At step five, the home-pool run had
14.87 million trips versus 7.62 million in the baseline. Of those, 13.25 million
(89%) had origin and destination in the same zone, versus 1.93 million (25%)
in the baseline. This measure does not identify the traveler's home zone. The
absolute difference in OD/mode/activity cells was 244% of baseline trip volume.
This is a single seed and neither choice set is a validated reference. It does
show that these additional alternatives change behavior much more than a mere
performance optimization; earlier fixed-state checks found that high-utility
home alternatives often have many activity episodes.

Across the 98,980 selected context/schedule pairs, the unweighted mean was
8.50 survey steps and the median was nine; one third had ten steps. The
survey-weighted mean for all weekday survey schedules with steps was 3.66.
The two figures use different weighting and populations, and neither includes
a comparable count of stay-home days, but they show the selection's strong
preference for longer schedules.

The ranking trace showed repeated preparation of home-to-home profile costs in
every batch. A second full-Geneva pass computed cost, distance and time for all
required home-zone/profile pairs once, and reused those rows only for that fixed
initial state. Batch construction and ranking together took 374.8 s, versus
677.3 s without this reuse, approximately 45% less. The cached pass's sampled
Python peak during ranking was 5.08 GiB versus 5.24 GiB. All 98,980 retained
context/schedule pairs and their best utilities matched exactly between the two
full runs; the 21 previously checked full rankings also matched. These are one
run each. The saved costs must not be reused when profiles, scenario parameters
or congestion costs change. Set `MOBILITY_HOME_BATCH_CACHE_COSTS=1` in the
manual batch probe to try the reuse.

The ranking and run harnesses are `tests/manual/inspect_home_zone_batched.py`
and `tests/manual/run_home_zone_pool.py`. The latter is restricted to a fresh,
single-scenario run using a ranking prepared for the same inputs; it is not a
checkpoint/resume implementation.

### High-effort home-zone comparison

The same experiment was repeated with three activity, destination and mode draws
and three search batches: 81 rough effort points per step. Both runs used the
same settings as the low-effort comparison, including congestion, diagnostics,
one scenario, one seed and five full-replanning steps.

The baseline took 494.5 s and the home-pool run 648.1 s, excluding the separate
677 s ranking. The sampled process-tree RSS peaks were 12.35 and 15.43 GiB;
sampled mean CPU was 472% and 505%, respectively. Baseline step times were
35.4, 96.7, 103.8, 114.7 and 119.0 s; home-pool step times were 47.2, 147.8,
128.7, 144.5 and 155.8 s. Candidate evaluation totaled 203.1 and 305.8 s,
respectively, within those steps.

At step five, baseline and home-pool trips totaled 10.33 and 13.69 million;
the within-zone shares were 42.5% and 78.5%. The OD/mode/activity absolute
difference was 154.8% of the baseline's trip volume. At step one, both runs
allocated about 936,500 people to mobile plans, but trips per mobile person
averaged 5.92 without the pool and 9.15 with it.

The pool also reduced sensitivity to effort in this single-seed comparison.
At step five, the low-versus-high effort absolute OD/mode/activity difference
was 143.7% of the high-effort trip volume without the pool, versus 74.8% with
the pool. The reference volumes differ, so these percentages are not directly
comparable as absolute errors. Dividing by the sum of the two trip totals gives
the symmetric flow-change index: approximately 82.7% without the pool and
35.9% with it. Lower effort sensitivity does not establish accuracy: the
home-pool runs also shifted strongly toward long, within-zone plans.

### Why the home-zone plans favor many activities

The first-step fixed-state capture was analyzed further without running another
simulation. Of 21 sampled scoring contexts, every context's fresh MNL allocation
among mobile home-zone plans had a higher average trip count than the current
country-and-activity-profile-weighted survey schedule sampler. The medians were
7.51 and 3.73 trips, respectively. The best mobile plan had at least seven
trips in every context, with a median of ten. In these samples, the median share
of MNL mobile allocation on plans with eight or more trips was 51.4%; the
corresponding survey sampler share was 5.8%. The survey sampler considers
schedules compatible with allowed activities, whereas the home-zone pool also
requires local opportunities and finite costs. The contexts were selected for
coverage, not as a representative population sample.

In context 2946 (French CSP fr-5, home zone 328), the best mobile plan scored
127.10 compared with 66.41 for stay-home. Its nine within-zone car trips cost
31.00 utility units in total; activity episodes contributed 99.81 and remaining
home time contributed 58.29. The best two-trip plan scored 88.92: 23.64 from
activity episodes, minus 6.89 travel cost, plus 72.17 for remaining home time.
These are different survey schedules, not a controlled change in a single plan.

The top nine-trip plan contains four shopping episodes totaling 3.67 hours. The
normal activity formula scores each episode separately: together they contribute
49.63 utility units. Scoring the same total duration as a single shopping
episode with the same activity parameters gives 22.99, a difference of 26.64
before changing any travel, timing or other activities. Splitting episodes is
therefore rewarded by the existing formula in this example. Distinct shopping
visits can have distinct value, so this observation alone does not establish what
the correct activity model should be.

Within-zone travel is not represented as a zero-length trip. The road/walk/bike
path-cost preparation chooses five sampled buildings per zone for same-zone
pairs, excludes pairs using the same building, routes the remaining building
pairs and computes weighted averages. For zone 328, the captured per-leg values
were: car 4.28 km, 5.9 minutes and 3.44 generalized cost units; bicycle 4.04 km,
19.2 minutes and 4.32 units; walk 4.00 km, 66.8 minutes and 4.11 units. In this
Geneva profile, the car cost is a 3.0 constant plus 4.5 per hour, with zero
distance cost in the default scenario. Those are estimated zone averages, not
actual route lengths between the specific activities. One scoring context can
combine demand subgroups with different car ownership.

Survey frequency affects discovery in the current sampler, not the ordinary
utility calculation after a plan enters the choice set. Sampling weights are
aggregated by country and allowed activity profile, not by the context's CSP.
The top nine-trip schedule has about a 0.0095% chance in one such weighted
schedule draw; 15 independent draws would see it at least once with probability
about 0.14%, before destination and mode generation. The home-pool experiment
instead inserts it directly. The schedule was observed in France, but not in
the fr-5 survey rows; absence from a survey subsample does not establish that
the schedule is impossible for that group.

Two fixed-choice-set sensitivities help separate these effects. Both condition
on mobile plans, use the captured feasible home-zone alternatives and use the
Geneva logit scale of 0.25. Multiplying the mobile MNL weights by the country
survey schedule weight changed the average trips per mobile person in the six
checked contexts from 5.88–8.26 to
5.51–8.18. This is a different choice model and could double-count survey
frequency if combined naively with the current weighted sampler. In another
calculation, all plans, travel costs, clock times and home-night utility were
held fixed, but each non-home activity's durations were summed before applying
the activity formula. The six averages fell to 4.32–5.44 trips; combining home
episodes as well gave 3.85–4.89. In context 2946, the share of mobile MNL weight
on plans with at least eight trips fell from 48.8% to 0.9% when all activity
types were combined. Reconstructed original step utilities agreed to within
7.2e-15. These are diagnostic counterfactuals, not proposed changes: they do not
reschedule plans, allow for different values of distinct visits, run transitions,
or establish model accuracy.

The six detailed contexts were 2946, 1757, 5236, 6766, 7679 and 8927. Their
one-step captures used the existing cache-only probe with congestion disabled;
they are independent of the five-step simulation trajectories above. Reproduce
the summaries with `tests/manual/analyze_home_zone_utility.py` and
`tests/manual/analyze_home_zone_contexts.py` on the corresponding capture folder.

### MATSim scoring and the zero-utility duration

Mobility's activity-duration score uses the same logarithmic family as the
[MATSim Charypar-Nagel score](https://www.matsim.org/files/book/partOne-latest.pdf),
with activity-specific coefficients and opportunity terms. Both sum the scores
of separate activity episodes. The MATSim relative formulation sets the zero
utility duration to the typical duration multiplied by `exp(-1 / priority)`;
its documented default priority is 1. Mobility uses mean survey duration
multiplied by `exp(-min_activity_time_constant)`. Mobility's parameter defaults
to 1, but the Geneva scenario sets it to 2. Thus the threshold in that scenario
is about 13.5% of the reference duration instead of 36.8% with a coefficient
of 1. Above the threshold, increasing the coefficient also increases the fixed
component of the utility of every separately scored episode.

The [MATSim activity scorer](https://matsim.org/doxygen/core_2scoring_2functions_2_charypar_nagel_activity_scoring_8java_source.html)
also has a current linear penalty below the zero-utility duration; Mobility
clips the logarithmic activity payoff to zero instead. The short-duration rule,
time-window scoring, activity-specific coefficients and other model differences
mean that changing Mobility's coefficient alone does not reproduce MATSim.
MATSim's standard score does not automatically combine all repeated activities
of one type, so a split-episode incentive is not necessarily eliminated by using
the MATSim formula.

As a fixed-choice-set sensitivity, the six detailed captures were rescored with
the coefficient changed from 2 to 1 for both activity episodes and the residual
home-night term. All other parameters, plan timings, travel costs and the
captured feasible choice sets remained fixed. At logit scale 0.25, conditioning
on mobile plans, mean trips fell from 7.26 to 3.09 for context 2946, from 7.73
to 3.33 for 1757, from 5.88 to 2.55 for 5236, from 6.60 to 3.40 for 6766,
from 8.26 to 4.11 for 7679 and from 6.44 to 3.21 for 8927. Their shares of
mobile weight on eight-or-more-trip plans fell from 17–60% to 0–1.4%.
This is not a new ranking or simulation: it does not recompute the home pool,
stay-home probability, transitions, congestion or opportunity feedback. It
shows strong sensitivity to this parameter, not that 1 is calibrated or that
the split-episode effect is solved.

### One shared activity-duration coefficient

Seven shared coefficients, from 0.5 to 2 in steps of 0.25, were first tested
on the same six fixed-state home-zone choice sets. Both the residual home-night
term and stay-home utility were rescored. This is a fresh logit allocation,
not an outer transition. For coefficient 2, the calculated mobile shares
reproduced the captured allocations within 2.3e-16. The table compares trips
per mobile person with the survey sampler's weighted average for the context's
country and allowed activity profile:

| Context | Country | Survey sampler | 1.0 | 1.25 | 1.5 | 2.0 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 2946 | FR | 3.73 | 3.09 | 3.74 | 4.64 | 7.26 |
| 1757 | FR | 3.73 | 3.33 | 4.04 | 5.00 | 7.73 |
| 5236 | FR | 3.66 | 2.55 | 2.97 | 3.68 | 5.88 |
| 6766 | CH | 4.09 | 3.40 | 4.08 | 4.94 | 6.60 |
| 7679 | CH | 4.09 | 4.11 | 5.29 | 6.34 | 8.26 |
| 8927 | CH | 4.02 | 3.21 | 3.84 | 4.74 | 6.44 |

At 0.5 the six mobile averages ranged from 2.19 to 2.76, and at 0.75 they
ranged from 2.31 to 3.24. The exhaustive home-zone choice sets and the survey
sampler do not have identical eligibility: home-zone plans additionally need
local opportunities and available modes. The six contexts are illustrative,
not representative. The fresh logit allocation assigns virtually all volume to
mobile plans at every coefficient here, so matching mobile trip counts alone
does not establish a good model or a realistic stay-home share.

Three further runs used the ordinary candidate generator, without the home-zone
pool, at coefficients 1, 1.25 and 1.5. The coefficient-2 run is the matching
baseline above. Each had one scenario and seed, five full-replanning steps,
one activity, destination and mode draw and one search batch, revision 0.4,
both congestion effects enabled, eight Polars threads and no memory limit.
At step five:

| Coefficient | Trips | People mobile | Trips per mobile person | Trips within a zone | Wall time | Peak process-tree RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.0 | 4.63 million | 78.8% | 2.51 | 20.0% | 67.0 s | 5.00 GiB |
| 1.25 | 5.50 million | 83.6% | 2.81 | 22.2% | 72.2 s | 5.21 GiB |
| 1.5 | 6.35 million | 86.8% | 3.12 | 24.4% | 71.9 s | 5.13 GiB |
| 2.0 | 7.62 million | 89.1% | 3.65 | 25.3% | 67.2 s | 5.29 GiB |

All runs start at stay-home and continue to change through step five; they are
not converged or calibrated estimates. The survey averages in the table above
are for six activity profiles, not a population-wide Geneva reference. The
different choice sets matter: a coefficient that gives plausible trip counts
with the low-effort sampler may favor substantially longer schedules when the
exhaustive home-zone alternatives are included. Compare more effort levels and
survey metrics before selecting a value.

For a cache-only run, set `MOBILITY_PROBE_MIN_ACTIVITY_TIME_CONSTANT` alongside
the existing Geneva probe options. The fixed-state sweep is in
`tests/manual/analyze_home_zone_utility.py`; its reconstruction is limited to
coefficients no greater than the captured coefficient of 2.

### Rebuilt home-zone pool at coefficient 1.25

The home-zone schedules were ranked again for coefficient 1.25, rather than
reusing the selection made at 2. The cached-cost ranking covered all 9,898
scoring contexts in 383.8 s, with a 5.12 GiB sampled Python peak during ranking.
It evaluated the same 382,254,517 plan/context combinations before timing
feasibility. The top ten schedules per context contained 98,980 pairs, of which
14,349 (14.5%) also appeared in the coefficient-2 selection. Their mean length
was 5.84 trips, median six, and 7.4% had at least eight trips; at coefficient
2 the corresponding figures were 8.50, nine and 74.9%.

The rebuilt pool was attached to the normal generator for a one-scenario,
one-seed, five-step lowest-effort run. Each step had full replanning, one
activity/destination/mode draw and one search batch, a revision probability of
0.4, both congestion effects and no memory limit. The experimental runner
checked four contexts against the actual initial scores; later changes do not
re-rank the initial selection. The normal generator uses one draw per step,
so the resulting choice set is not a high-effort reference.

| Constant and pool | Step-5 trips | People mobile | Trips per mobile person | Trips within one zone | Simulation time | Peak process-tree RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.25, normal generator | 5.50 million | 83.6% | 2.81 | 22.2% | 72.2 s | 5.21 GiB |
| 1.25, plus home-zone pool | 6.63 million | 74.8% | 3.79 | 61.9% | 118.8 s | 6.85 GiB |
| 2.0, plus home-zone pool | 14.87 million | 87.5% | 7.26 | 89.1% | 115.4 s | 7.72 GiB |

The separate ranking is excluded from simulation times. For the coefficient-1.25
home-pool run, step times were 6.9, 27.7, 20.6, 20.0 and 18.9 s; candidate
evaluation took 23.8 s in total. At step one, 40% of people were mobile, with
6.22 trips per mobile person; virtually all trips were within a zone. At step
five, the 3.79 trips per mobile person is close to the survey sampler averages
in the six contexts above, but those contexts are not a population-wide survey
target. These five-step runs do not establish convergence or validity. The
ordinary candidate-pool diagnostic excludes the separately attached home plans.

### Timing reuse on consecutive real batches

At coefficient 1.25, a separate capture used one scenario, two full-replanning
steps, two activity draws, one destination draw, two mode draws and two inner
batches. The second outer step's first batch contained 1,391,068 scored rows;
the next batch contained all of them plus 318,631 rows. All repeated scoring
inputs were identical in this capture. The second batch had 442,162
context/plan combinations but only 54,357 physical plan keys. Its 1,709,699
context steps corresponded to 208,456 physical steps, and none of these had
conflicting timing inputs across contexts in this particular capture. Different
travel profiles can produce different timing inputs in other scenarios.

Comparing each incoming schedule against its rounded modeled timing showed
that 420,525 of 442,162 plans had at least one changed timing or duration. This
does not mean those timings changed between the two batches: the same incoming
survey-based schedule was scored again under the same fixed outer-step costs.

A narrow-cache prototype kept the first batch's input plan/step keys, modeled
schedule IDs and four timed values, while taking current scores and retention
history from the second batch. It matched the complete timed-row multisets,
schedule mappings and catalogue from the real second batch. Three offline
timing/indexing comparisons took 0.46–0.62 s for full recomputation and
0.37–0.42 s with the cache, excluding first-batch work. Building that cache
took 0.34 s and it occupied about 59.7 MiB, so it did not pay for itself for
this two-batch step. These tests do not include scoring, transition allocations,
flows or full-run memory.

A second prototype timed a physical plan once per travel profile, then attached
the result back to the scoring contexts. Using the three profile IDs from the
matching initial scoring contexts gave 389,993 physical/profile step rows.
On this capture, three full timing/indexing runs took 0.48–0.63 s; the factored
part took 0.14–0.15 s and reattachment 0.14–0.17 s. Constructing and validating
the factored rows from already-expanded inputs cost another 0.11 s; a future
evaluator that carries physical rows separately could avoid some of that work.
The reconstructed complete timed rows, mappings and catalogue matched exactly.
This is an isolated benchmark, and profile identity, changed times, reference
schedule collisions and the fallback for non-factored candidates still matter.
The capture and comparison scripts are `tests/manual/capture_repeated_timing.py`,
`analyze_repeated_timing.py`, `benchmark_repeated_timing.py` and
`benchmark_physical_timing.py` in the same directory.

### Live physical-timing experiment and activity-duration terms

An opt-in, process-local prototype applied profile-aware physical timing in
the real evaluator. It falls back to the ordinary calculation if a physical
step and profile have conflicting timing inputs. It retains the existing
scoring, context expansion, plan utility and transition paths. The prototype
is `tests/manual/run_factored_timing_probe.py`; it is not a production feature.

A one-scenario, one-seed Geneva comparison used five full-replanning steps,
coefficient 1.25, two activity draws, one destination draw, two mode draws and
two inner batches (eight effort points per step). Both runs enabled road and
opportunity congestion, used eight Polars threads and had no memory limit.
The baseline took 135.5 s and the factored prototype 125.4 s. Summed candidate
evaluation spans were 24.15 and 21.54 s, and their nested modeled-timing and
indexing spans were 7.76 and 6.52 s. Sampled process-tree peak RSS was 5.27 and
5.16 GiB; mean sampled CPU was 376% and 403%, where 100% is one logical CPU.
These are single runs, so the whole-run difference cannot be attributed solely
to the change. Candidate-pool counts matched for every batch. The five
OD/mode/activity flow tables had identical keys; the largest absolute cell
difference was 9.1e-13 represented trips.

A second, two-step run of the same setup captured state for both methods. The
candidate ID tables, sorted allocated plan tables and random-state digests
matched exactly at both steps. This does not establish checkpoint/resume or
multi-scenario equivalence, nor performance at high effort.

Another isolated test cached the duration-dependent logarithm by modeled time
sequence, step, country and CSP. The 1,534,994 deduplicated scored step rows
needed 1,534,816 distinct terms, occupying about 22 MiB. Three direct utility
aggregations took 0.10–0.11 s; joining the precomputed terms took 0.15–0.17 s,
excluding the 0.10 s to construct them. Results matched exactly. In this
capture, caching those terms adds work rather than saving it; changing the
physical representation could alter that conclusion. The diagnostic is
`tests/manual/benchmark_activity_terms.py`.

### What changes between outer steps

The initial survey-derived activity and home-night reference durations are
carried forward in the run state. They need not be recalculated on every outer
step. Actual plan timings and activity durations depend on the plan's incoming
schedule, modeled travel times and arrival-time rigidity; those can stay the
same for fixed-cost modes, but can change when a congested route or scenario
changes. The generalized travel cost can change even if travel time does not,
for example when a scenario changes a monetary cost. Opportunity saturation
and shadow prices affect activity utility separately from timing.

As a scale check, the previously recorded lowest-effort coefficient-2 Geneva
run with both feedbacks enabled had 4,952 destination/activity rows. Shadow
prices changed by more than 1e-9 in 1,079 rows between steps 1 and 2, 1,469
between steps 2 and 3 and 1,968 between steps 4 and 5. This counts table values,
not how many evaluated candidate rows depend on them. The `k_saturation_utility`
field changed in 2,212, 2,351 and 2,456 rows respectively. This is one older
trajectory, not evidence for all scenarios or coefficient settings.

Selective recalculation could update costs for changed mode/OD/profile keys,
timings for plans using changed travel times, and opportunity terms for changed
destination/activity keys. It would also need to account for parameter changes,
new candidates, stable schedule IDs and checkpoint/resume. The existing
`ScoringLookup` already reuses compact step scores while one outer step's
inputs are fixed. No cross-step selective cache has been added; its bookkeeping,
memory and end-to-end cost require measurement.

### Higher-effort physical-timing comparison

The live physical-timing prototype was also compared over two full-replanning
steps with one scenario and seed, coefficient 1.25, three activity, destination
and mode draws and three batches: 81 effort points per step. Both congestion
effects were enabled, Polars used eight threads, state and diagnostics were
captured and no memory limit was applied. These are single runs:

| Measure | Baseline | Physical timing |
| --- | ---: | ---: |
| Total time | 165.9 s | 157.1 s |
| Step times | 35.8, 105.9 s | 32.8, 100.4 s |
| Candidate evaluation, summed | 56.44 s | 54.43 s |
| Timing/indexing within evaluation | 22.83 s | 21.68 s |
| Sampled process-tree peak RSS | 10.21 GiB | 9.75 GiB |
| Sampled mean CPU | 460% | 478% |

The candidate ID tables, allocated plan tables and random-state digests matched
exactly at both steps; corresponding OD/mode/activity flow keys matched, with a
maximum cell difference of 2.3e-13. The final batch had 13,439,326 context-step
rows and 3,073,559 physical/profile rows. The evaluator timing difference is
small relative to the full run and the two individual measurements do not prove
a whole-run speedup. Since the prototype currently expands rows, checks their
physical inputs and rejoins them, the measured saving does not justify treating
it as a production optimization without broader scenarios and further work.

### Observed changes in per-mode scoring costs

A separate three-step, one-scenario, seed-0 capture used coefficient 1.25,
two activity draws, one destination draw, two mode draws and one search batch
per step (four effort points). Both congestion effects were enabled. It saved
the profile/OD/mode cost rows requested by the first evaluation of each step;
it did not capture every possible OD pair. Profile assignments matched exactly
between the three steps, so profile IDs can be compared for this capture.

| Common profile/OD/mode keys | Step 1 to 2 | Step 2 to 3 |
| --- | ---: | ---: |
| Walk: unchanged cost, time and distance | 7,859 / 7,859 | 10,242 / 10,242 |
| Bicycle: unchanged cost, time and distance | 23,991 / 23,991 | 31,176 / 31,176 |
| Walk/public transport/walk: unchanged cost, time and distance | 13,694 / 13,694 | 17,286 / 17,286 |
| Car: changed cost and time | 32,499 / 32,499 | 48,566 / 48,566 |

Many additional keys were requested in later steps, and the intersections do
not establish what happened to unobserved keys. Car distance also changed for
32,286 of 32,499 common keys in the first comparison, and 39,705 of 48,566 in
the second. Destination shadow prices changed by more than 1e-9 for 591 of
4,952 opportunity rows from step 1 to 2, and 1,126 from step 2 to 3; saturation
utility factors changed for 1,950 and 2,371 rows, respectively. Fixed costs in
this trajectory can change under other scenario parameters, profiles, modes or
network changes, so this is not a universal cache key.

In the 81-point baseline above, profile-cost preparation spans nested within
candidate evaluation totaled 2.61 s of the 56.44 s evaluation spans. The
profile-cost span does not include every downstream cost-related join; removing
that measured span alone is therefore a limited opportunity, not a bound on
every possible cost refactor. Timing/indexing was a larger measured component.
Use `tests/manual/capture_scoring_changes.py` and
`tests/manual/analyze_scoring_changes.py` to reproduce this diagnostic.

### Similar plans and the existing embedding

Two exploratory Geneva captures used one scenario, seed 0, both congestion
effects, full replanning, and activity-time constant 1.25. The second outer
step was captured after its final search batch. The smaller run used 2 activity
draws, 1 destination draw, 2 mode draws and 2 batches (8 effort points per
step); the larger run used 3 of each and 3 batches (81 points). These captures
include extra file writes, so their timings are not simulation benchmarks.

For each capture, 100 scoring contexts were selected across population-size
quantiles, 50 per country. This is a diagnostic sample, not a representative
estimate of all contexts. Only feasible mobile candidates are included. The
analysis also matches the final allocated plans to these candidates and checks
allocated neighbours within the same original demand unit.

The existing `PlanDistance` builds 24 hourly cumulative activity/mode and
centroid-position features. Its activity/mode values currently return to zero
in hours where a state is absent, even if time had already accumulated in that
state. This analysis carries each state total forward before comparing plans;
it does not change the production transition-distance model. Changing that
model needs separate validation and attention to saved state and cache identity.

Here two plans are close when their hourly activity/mode fractions differ by at
most the stated amount of time, and their centroid positions differ by at most
1 km on average over the full day. Hourly aggregation can hide within-hour
differences, and averaging over the entire day can hide large differences on a
short trip. Neither measure compares detailed routes.

| Sample | Time threshold | Candidates with a close neighbour | Allocated mobile people with a close neighbour in their demand unit |
| --- | ---: | ---: | ---: |
| 8-point | 15 minutes | 16.4% | 24.0% |
| 8-point | 30 minutes | 36.2% | 44.6% |
| 81-point | 5 minutes | 11.4% | 13.6% |
| 81-point | 15 minutes | 41.6% | 46.0% |
| 81-point | 30 minutes | 64.9% | 71.7% |

The 8-point sample had 2,725 candidates and 16,013 allocated mobile people;
the 81-point sample had 20,457 candidates and 14,312 allocated mobile people.
Shares with a neighbour count both sides of each pair and do not measure how
many plans can be removed. A greedy, utility-ordered grouping of candidates at
30 minutes would remove 18.2% and 39.6%, respectively, but this is an
order-dependent illustration, not evidence that the removal is valid or fast.

In the larger sample, 14,560 pairs met the 30-minute threshold. Of these,
14,388 had the same activity sequence, 6,926 the same destination sequence,
and 4,716 the same mode sequence; only 40 had both the same destination and
mode sequences. Requiring both leaves 77 of 20,457 candidates (0.38%) with a
neighbour and 29.5 of 14,312 allocated mobile people (0.21%) with a neighbour
in the same demand unit. Their activity sequences can still differ.

Close plans can have substantially different utility: for all close pairs in
the larger sample, the median absolute difference was 0.93, the 90th
percentile 8.06, and the maximum 97.97. In the smaller sample the corresponding
figures were 0.45, 1.95 and 7.60. For example, two car plans with distinct
destinations had a 7.60 utility difference despite being only 0.13 km apart on
average over the full day. Different destinations and modes must remain
distinct for utility, OD flows and congestion. Even the same mode and
destination sequence does not establish equivalent timing or utility.

The most promising use is therefore to identify shared computations within
families of plans, while retaining each exact alternative and its own utility
and flows. The embedding could also help diagnose sampling diversity; this
experiment did not measure the cost of constructing it for the full pool or
show a simulation speedup. Use `tests/manual/capture_embedding_pool.py` and
`tests/manual/analyze_embedding_pool.py` to reproduce the diagnostic; set
`MOBILITY_EMBEDDING_CAPTURE_BATCH` to select the batch to capture.

### Reuse of plan timing and utility components

This offline census uses the earlier two-step, 8-point Geneva capture: one
scenario, seed 0, coefficient 1.25, both congestion effects and full replanning.
It examines outer step 2, batch 2, before and after modeled timing. The scorer
adjusts each trip's reference departure and arrival using that trip's modeled
travel time and destination activity rigidity; the activity duration also uses
the following trip's adjusted departure. It does not currently propagate a
state through independent rigidity-separated chains.

The capture had 1,709,699 incoming step occurrences. There were 142,136
distinct inputs to the per-trip timing update, and 160,981 distinct conservative
keys containing a trip, its following trip and its reference activity end.
The latter keys include more than the minimum necessary for some rows. These
are counts of matching inputs, not measured runtime savings or a complete
specification of a reusable section's stored outputs.

After modeled-time deduplication, 1,534,994 context-step rows had 487,736
distinct combinations of duration, minimum activity duration and activity
utility scale. This key reproduces the activity term for this fixed capture;
it does not include the plan's remaining home-night term. Activity values and
costs can change across scenarios and outer steps.

Separately, a sample of 100 population-quantile-stratified contexts from the
81-point, step-2, batch-3 capture contained 20,457 feasible mobile plans and
75,179 steps. There were 17,335 profile/OD/mode keys, 49,226 modeled-step keys
(profile, activity, OD, mode and modeled times), and 55,460 adjacent pairs of
such modeled-step keys. The capture does not contain all inputs needed to prove
that each apparent match can share a complete scoring calculation. The sample
is diagnostic, not representative of the whole region.

A second run captured the missing reference and scoring inputs for the same
100 contexts at that high-effort batch. Its 137,442 incoming step occurrences
had 72,710 distinct trip-timing inputs and 88,257 conservative adjacent timing
keys. After modeled-time deduplication, 121,435 context steps had 38,900
distinct activity-utility inputs. This sample includes infeasible candidates;
the earlier 75,179-step sample above contains feasible mobile plans only.

For the 8-point capture, rounding **only the duration used in activity
utility** to the nearest bin yielded the following. Modeled times, feasibility,
travel utility and home-night utility were left unchanged. The fresh-MNL
column is the population-weighted total variation between the original and
perturbed plan-choice distributions at logit scale 0.25, including stay-home;
it is not a flow or outer-convergence error. The separate captures were matched
on all feasible plan keys and on all 960,723 corresponding modeled steps.

| Duration bin | Distinct activity utility inputs | Median / 90th percentile absolute plan utility error | Weighted fresh-MNL change |
| ---: | ---: | ---: | ---: |
| Exact | 487,736 | 0 / 0 | 0 |
| 1 minute | 182,316 | 0.014 / 0.069 | 0.27% |
| 5 minutes | 109,839 | 0.072 / 0.343 | 1.33% |
| 15 minutes | 69,115 | 0.233 / 1.048 | 4.13% |

The same analysis on the 100 high-effort contexts matched all 20,457 feasible
mobile plans and all 75,179 corresponding modeled steps. One-, five- and
fifteen-minute duration bins gave population-weighted fresh-MNL changes of
0.29%, 1.41% and 4.12%, respectively, within this non-representative sample.
Their 90th-percentile absolute plan-utility errors were 0.068, 0.351 and 1.035.

For comparison, linearly interpolating between neighboring duration-bin
utility values instead of using the nearest bin reduced the 90th-percentile
absolute plan-utility error to 0.025 with five-minute bins and 0.283 with
15-minute bins in the 8-point capture. The corresponding high-effort sample
values were 0.026 and 0.280. It requires two values and interpolation per
lookup, and larger errors remain near utility clipping boundaries; this is an
accuracy result, not proof of a speedup. The home-night term and exact timing
remain unchanged in this experiment.

Naively rounding both arrival and next departure before testing timing
feasibility is different from rounding only the utility input. On 1,142,364
non-final steps, one-, five- and fifteen-minute rounding made respectively
3,055, 14,287 and 39,670 individually infeasible timing checks appear feasible.
These counts include steps on plans that may be infeasible for other reasons;
they are not counts of newly accepted plans. Preserve exact feasibility in an
approximation unless a full comparison demonstrates another safe approach.

Run `tests/manual/analyze_plan_component_reuse.py` on a capture from
`capture_repeated_timing.py`, optionally supplying `--utility-capture` with the
matching `capture_embedding_pool.py` folder. The 81-point sample is counted by
`tests/manual/analyze_high_effort_component_reuse.py`, with its detailed
inputs captured by `tests/manual/capture_high_effort_components.py`. These
scripts are diagnostics, not production optimizations or end-to-end
performance tests.

### Time-grid and hierarchical scoring prototypes

`tests/manual/benchmark_time_components.py` compares the current timing
calculation with a proposed nearest-minute grid, including coarser grids of 5
and 15 minutes. Reference clock times and travel times are rounded to the
grid; rigidity adjustment is rounded to that grid; activity duration follows
from modeled arrival and the next departure. The existing 0.001-hour duration
floor remains. This is an approximation for investigation, not an adopted
time model. It reports activity/travel step-utility differences without the
home-night term and counts incoming plan records before modeled-ID deduplication.

In the 8-point capture, 442,162 incoming plan records contained 1,709,699
steps. The one-, five- and fifteen-minute grids had respectively 96,896,
33,451 and 6,575 distinct trip-timing inputs. Relative to second-resolution
timing, 1,855 / 8,772 / 24,351 input plans became timing-feasible and
8 / 102 / 1,281 became infeasible. Among plans feasible in both calculations,
the 90th-percentile absolute change in summed activity/travel step utility was
0.091 / 0.466 / 1.605. This does not include the whole-day home term or later
changes in sampling and congestion.

The first trial built timing components after expanding all context-step rows,
then joined them back. In three five-minute repetitions it took 0.56–0.59 s,
versus 0.57–0.63 s for direct grid timing, including its own input preparation.
This experiment demonstrates the cost of factoring already-expanded rows; it
does not test the intended hierarchical representation.

`tests/manual/benchmark_hierarchical_utility.py` instead builds a shared trip
catalogue and arrays of plan-to-trip references and plan offsets, then scores
without rejoining wide step tables. It includes the separately calculated
home-night term when given matching home and utility captures. On those 1.71
million steps and 442,162 incoming plan records, the 5-minute grid yielded
33,451 trip components. Building the representation from the expanded capture,
including reading and indexing it, took 0.58 s. Across five repetitions,
direct array timing and full utility took 0.135–0.167 s, and shared components
took 0.081–0.102 s. The compact numerical arrays were at least 63.9 MiB,
excluding the input dataframe, construction tables, output and allocator
overhead. Both methods agreed on feasibility; shared utility differed from the
hybrid Polars scorer by at most 3.6e-6 in the capture. These are isolated
measurements, not end-to-end speedups; originating references directly during
generation may avoid rebuilding them each batch, but was not tested.

A separate one-scenario, seed-0, five-step lowest-effort simulation compared
normal timing with the 5-minute hybrid via `run_time_grid_probe.py`. Both used
coefficient 1.25, 0.4 revision probability, full replanning and both congestion
effects. Only the scoring-time schedule update and the travel time used by
home-night utility were rounded; candidate generation, routing and saved
schedule-ID schema retained their existing resolution. The comparison is thus
not a complete grid-based model. The baseline took 65.9 s and peaked at 4.76
GiB sampled process-tree RSS; the grid run took 67.5 s and peaked at 5.10 GiB.
Both include flow capture and are single runs.

At step 5, total trips were 5.498 million versus 5.529 million (+0.56%); the
sum of absolute OD/mode/activity cell differences divided by baseline trips was
36.0%. The latter rose from 4.9% at step 1 to 11.5%, 19.5% and 28.7% at
steps 2–4. The runs can discover different candidates and follow different
congestion trajectories after timing changes, so these are not isolated
rounding errors and cannot be interpreted as a comparison to a higher-effort
accuracy reference. Use `compare_time_grid_flows.py` on the captured flow
folders to reproduce the comparison. A higher-effort full run was not started
because this hybrid did not yet improve end-to-end performance; the isolated
hierarchical kernel warrants a design with references built before expansion.

A matching one-minute hybrid run took 73.7 s with 5.39 GiB sampled process-tree
RSS. It produced 5.530 million trips at step 5 (+0.58% versus baseline). Its
OD/mode/activity absolute differences over baseline were 0.74%, 2.56%, 5.81%,
13.62% and 21.25% at steps 1–5, respectively. Like the five-minute comparison,
these are complete evolving simulations, not a fixed-candidate rounding test;
candidate discovery and congestion can amplify early differences.

### Propagating allocation through shared components

An offline comparison uses the actual post-transition allocations from the
same second-step Geneva captures. It maps original demand units to scoring
contexts, matches each allocation to its scored plan and checks that every
allocated plan has captured steps. The exact-duration comparisons also check
that each allocated modeled step agrees with the separate timing capture.
Stay-home allocations have no road trips or non-home opportunity occupation
and are excluded from both paths.

The expanded path joins every unit/plan allocation to its step occurrences,
then aggregates OD/mode person flows and destination/activity occupation. The
shared path builds one OD/mode component and one activity/destination/duration
component per distinct input, with per-plan references and occurrence counts.
It first sums plan allocation volumes, propagates them to components and
aggregates components to the same outputs. This is a flat component hierarchy;
it does not yet measure reuse of longer shared subplans. Both paths use only
columns needed for these outputs and preserve repeated occurrences in a plan.

The full 81-point capture does not include modeled activity duration for all
contexts; reconstructing it by subtracting the saved rounded clock times is not
exact. Therefore its full-region benchmark is **OD only**, while OD and
occupation are compared on the 100 contexts with exact scored durations. The
8-point comparison covers all 9,898 contexts with exact durations.

| Capture | Allocated unit/plans | Expanded unit/steps | Shared trip components | Shared activity components | Build | Expanded aggregation | Shared aggregation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 81-point, full, OD only | 491,151 | 1,818,687 | 31,906 | — | 23 ms | 54–57 ms | 23–25 ms |
| 81-point, 100 contexts, OD and occupation | 5,908 | 21,447 | 1,755 | 1,903 | 3 ms | 3.5–4.0 ms | 3.5–5.4 ms |
| 8-point, full, OD and occupation | 252,710 | 834,228 | 20,477 | 21,549 | 16 ms | 25.7–27.4 ms | 21.2–21.5 ms |

The full 81-point trip index contained 319,863 distinct plan/component
references representing 416,087 context-plan trip occurrences; the remaining
expansion was across demand units. It occupied about 3.93 MiB as measured by
Polars, excluding allocations, input steps and working buffers. Fresh-process
sampled peak RSS was 1.24 GiB for the expanded method and 1.15 GiB for the
shared method, including capture loading and validation. These RSS peaks are
not incremental hierarchy memory or simulation peaks. Input loading and
matching, identical for both, took 0.23–0.24 s and is excluded from the table.
The shared build is shown separately and would need updating when plan
membership changes; repeated measurements reuse the same captured allocation.

All OD cell keys matched. For the full 81-point OD table, the maximum absolute
cell difference was 2.4e-10 persons, and total absolute difference was 1.13e-15
of baseline volume. In the full 8-point occupation table, the maximum cell
difference was 0.000385 person-hours and total absolute difference was
3.19e-9 of occupation. The existing path casts each allocated row's weighted
duration to Float32 before summing; multiplying the already aggregated volume
by duration does not reproduce that per-row rounding. Both methods sum in
stable order but have different grouping, so their outputs are not bit-identical.

Reproduce with `tests/manual/benchmark_component_flows.py` in separate
processes using `--method expanded` and `--method shared`, and compare their
outputs with `tests/manual/compare_component_flows.py`. Use `--od-only` for the
full 81-point embedding capture. The timings do not establish a simulation
speedup: generating components before expansion and integrating with choice,
congestion, scenario/profile dimensions and saved results remain untested.

### Shared-prefix plan graph and profile-at-a-time scoring

`tests/manual/benchmark_subplan_graph.py` builds a graph of complete candidate
plans from the existing captured modeled steps. A prefix node references its
parent prefix and one exact component; complete plans point to their terminal
nodes. Plans remain distinct choices, even if they share a terminal node.
Scoring walks the prefixes, adds activity utility and trip cost, and computes
the home-night term at the leaf. Allocated volume is propagated back through
the graph, and shared components produce OD/mode flows and non-home
destination/activity occupation. The input is an already-expanded capture;
the benchmark does not yet generate plans or update modeled timing directly
in this representation.

The first graph treats all scored component inputs, including travel cost and
activity utility scale, as part of a component's identity. A second graph shares
physical timing and activity/trip structure separately from scoring inputs.
Its component key includes route feasibility and exact modeled timing. The
prototype uses country, socioeconomic category and transport utility profile
to attach cost and activity parameters, and checks every captured key for
conflicting scores. That key is validated only for this scenario and fixed cost
state; other inputs or scenarios may require additional distinctions.

| Capture | Context/plan candidates | Captured step occurrences | Exact scored prefixes | Physical prefixes | Profile-specific prefix scores |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8-point, all 9,898 contexts | 392,630 | 1,534,994 | 1,314,542 | 160,283 | 1,314,661 |
| 81-point, 100 selected contexts | 29,432 | 121,435 | 98,159 | 95,718 | 98,159 |

For the full 8-point capture, 139,115 physical components support the 160,283
physical prefixes. Structural sharing across scoring contexts is substantial,
but caching every profile-specific prefix score still creates about 1.31
million score records: sharing physical structure alone does not remove the
context-specific work. The 81-point sample consists of population-quantile
contexts and is not representative of the full region.

For that 8-point capture, the fully scored prefix graph matched all captured
feasibility flags and feasible utilities to 4.3e-14 maximum absolute utility
difference, including home-night utility. Graph OD flows matched the expanded
output to 6.8e-16 of total volume in sum of absolute differences; occupation
differed by 3.2e-9 of total occupation, mainly because the existing path casts
each allocated unit's weighted duration to Float32 before summation. The graph
keeps all allocated plan volumes distinct until propagation. It does not
produce per-origin transition events or requested plan-step tables.

Three repeated scoring passes on this capture took 39–52 ms using direct
compact NumPy arrays, 63–75 ms using the fully scored prefix graph, and 43–58
ms with the physical graph and all profile-specific prefix scores already
built. Input preparation from expanded rows took 0.54 s, scored-prefix graph
construction 0.19 s, physical-prefix census 0.17 s, and profile-prefix table
construction 0.35 s; these phases overlap in the diagnostic and are not a
production pipeline. Flow propagation through 172,129 occupied prefixes and
aggregation took 29–30 ms per repetition. These are isolated runs on one
capture, not end-to-end or cross-machine benchmarks.

The prototype also scores one profile at a time on the physical graph and then
discards its temporary scores. Nineteen profile groups reproduced captured
feasibility and utility; the largest needed 107,450 prefixes and 94,098
components at once. The selected temporary arrays and profile component table
were at least 9.9 MiB, excluding the shared graph, source, outputs and other
working memory. This implementation took 1.71 s because it repeatedly filters
and joins the already-expanded input. Its process peak, about 1.92 GiB, also
includes the earlier full-table experiments and cannot be interpreted as the
peak of an independent streaming implementation. It demonstrates a bounded
calculation, not a measured memory or runtime win over the current evaluator.

The main architectural question is whether candidate generation can construct
and retain physical prefixes and profile-specific cost lookups directly, so
that the per-profile calculation does not repeatedly reconstruct them from
expanded rows. Updated congestion, allocation, plan identity, reproducibility,
checkpoint/resume and on-demand detailed results remain to be designed and
measured. No minute rounding is used in this exact experiment.

### Cutting captured plans into reusable sections

`tests/manual/benchmark_plan_sections.py` compares exact section cuts on the
same modeled candidate captures. It treats each captured trip and the activity
at its destination as separate physical atoms. An activity with arrival
rigidity exactly 1 is a standalone anchor; all remaining consecutive atoms
between anchors form a section. Trip timing, activity duration, feasibility
and destination remain exact. Scoring factors are kept separately by country,
socioeconomic category and travel utility profile, and duplicate atom/profile
scores are checked for conflicting inputs. The method only represents existing
complete plans: it does not create alternatives by swapping sections.

Three cuts were compared: `rigid` groups all consecutive non-anchor atoms
between standalone anchors; `atomic` makes every trip and activity its own
section; `repeated_pairs` greedily groups pairs of consecutive atoms that
appear in at least two captured plans, without crossing an anchor boundary.
The last is one simple pair heuristic, not an optimized search for all repeated
subsequences. All strategies start with already-expanded modeled rows.

| 8-point capture, all 9,898 contexts | Rigid | Every atom | Repeated pairs |
| --- | ---: | ---: | ---: |
| Candidate plans | 392,630 | 392,630 | 392,630 |
| Distinct sections | 130,288 | 255,107 | 191,462 |
| Plan-to-section references | 1,449,520 | 3,069,988 | 2,259,754 |
| Section-to-atom references | 290,856 | 255,107 | 260,240 |
| Profile/section scores | 1,017,609 | 2,103,719 | 1,548,301 |
| Section/reference bytes, excluding component tables and scores | 19.9 MiB | 38.1 MiB | 28.8 MiB |
| Section-cut construction | 0.18 s | 0.14 s | 1.75 s |
| Cached section-score construction | 0.23 s | 0.23 s | 0.22 s |
| Plan scoring, reused cached sections | 83–95 ms | 147–160 ms | 109–116 ms |
| Allocated-volume propagation and flow aggregation | 14–15 ms | 15–18 ms | 17–18 ms |

Input preparation, including reading expanded rows, indexing the atoms and
building all-profile atom scores, took another 1.18–1.23 s. Peak process RSS
was 2.12–2.25 GiB across the three fresh-process runs. The section/reference
byte counts do not include atom tables, profile scores, allocations, temporary
joins or the expanded source. Neither the timings nor these counts prove that
the same work can be avoided in the current simulation's candidate generator.

For 100 selected contexts from the 81-point capture, rigid cuts produced
48,378 sections, 112,206 plan references and 49,635 profile/section scores;
atomic cuts produced 117,603, 242,870 and 121,451 respectively. The sample is
not representative of all contexts. In all six runs, complete-plan feasibility
matched the capture and maximum feasible utility error was below 6e-14,
including the home-night term. OD keys matched and relative absolute flow
differences were below 7e-16. Full 8-point occupation differed by 3.2e-9 of
total occupation because the existing expanded path rounds weighted durations
per demand-unit row to Float32 before summing.

An additional `--stream-profiles` run scores rigid sections one profile at a
time, discarding the section scores before processing the next profile. In the
full 8-point capture, its largest of 19 profile batches needed 86,534 section
scores and 198,758 section/atom evaluations at once, instead of caching all
1,017,609 profile/section scores. Its one scoring pass took 0.52 s including
building each profile's scores, versus 0.23 s to build all cached scores plus
83–95 ms per scoring pass in the all-profile runs. Sampled process peak RSS was
1.63 GiB versus 2.12 GiB in the separate rigid all-profile process; both include
the expanded capture and all common construction, so these are diagnostic
process peaks, not peak memory for a production streaming implementation.
Utilities, feasibility, OD flows and occupation still matched as above.

On these captures, rigid cuts are the most compact of the tested section
representations and the simple repeated-pair heuristic does not justify its
extra construction work. The section plan scorer is a prototype and was not
faster than the previously measured 39–52 ms direct compact-array plan score;
the input, output and operations differ, so it is not an end-to-end comparison.
Creating sections and profile lookups during candidate generation, rather than
deduplicating expanded rows, remains the key untested architecture change.

The same prototype additionally factors the complete ordered physical section
sequence from context membership. In the full 8-point capture, the 392,630
context/plan memberships refer to 47,994 distinct physical plans. Their
1,449,520 context/section occurrences become 185,844 physical-plan/section
references plus 392,630 context/plan memberships. These two factored index
tables occupied 5.91 MiB, excluding section definitions, components, scores,
allocations and temporary construction. Constructing them from the expanded
capture took about 0.06 s. There were still 392,630 distinct profile/physical
plan pairs, so this did not remove a plan-score calculation in this capture.

The profile-at-a-time rigid scorer was also run against these factored plan
references. It held at most 86,534 section scores per profile, reconstructed all
captured feasibility and utilities within 3.6e-14, and propagated allocations
through physical plans and sections to matching OD and occupation outputs. One
pass took 0.53 s for scoring and 0.014 s for flow aggregation after construction;
sampled process peak was 1.65 GiB. The implementation still starts from and
retains expanded captured rows, and builds sections after the fact; these
figures demonstrate the representation and correctness rather than the memory
or runtime of a fully integrated, pre-expansion generator.

### Prefixes inside non-rigid sections

`tests/manual/benchmark_plan_sections.py --strategy prefix` starts from the
same rigid-bounded sections as `--strategy rigid`, then shares left-to-right
prefixes inside each section. Standalone rigid activities are single-node
sections. A node contains one physical trip or activity token plus a link to
its preceding prefix. A section points to its last node. Prefixes share only
exact captured modeled inputs, and profile-specific scoring is kept separate.
Both variants use the same captured input, Polars-based aggregation, profile
keys, complete-plan membership, and home-night formula. Neither creates new
plans or changes timing precision. Both begin with expanded captured rows.

| Full 8-point capture | Whole rigid sections | Prefixes inside sections |
| --- | ---: | ---: |
| Distinct sections | 130,288 | 130,288 |
| Physical section/atom occurrences or prefix nodes | 290,856 | 271,392 |
| Profile-specific section/atom rows or prefix nodes | 2,412,609 | 2,242,208 |
| Section/index build | 0.18 s | 0.35 s |
| Build section/profile scores | 0.26 s | 0.47 s |
| Repeated plan-score aggregation | 83–90 ms | 95–100 ms |
| Repeated OD and occupation aggregation | 13–15 ms | 49–54 ms |
| Sampled peak process RSS | 2.13 GiB | 2.20 GiB |

The profile-specific counts describe rows used to accumulate already-computed
atom scores, not independent evaluations of the logarithmic utility formula.
Both methods precompute atom scores from the expanded capture. Prefixes reduce
those rows by about 7%, but this Polars prototype uses extra joins and per-depth
aggregation. Its prefix-node and section-leaf tables occupy about 6.17 MiB;
its reported process peak also includes the ordinary section lists and
temporary prefix paths used to build and validate the diagnostic. This does
not measure an optimized production representation that stores only prefixes.

The prefix flow calculation propagates volume from each complete section
backwards through parent links, summing shared-node volumes. A separate run
with `--prefix-flow-direct` retained the direct section/atom links for flow
aggregation: flows then took 15–16 ms, but the prefix scoring and index build
remained slower than whole sections. Retaining both links also uses additional
storage. All complete-plan feasibility flags matched; maximum feasible utility
error was below 4e-14. OD flows matched to floating-point precision and
occupation differed by 3.2e-9 of total occupation, due mainly to the current
per-unit Float32 weighted-duration rounding.

On the selected 100-context 81-point sample, the profile-specific rows fell
from 152,865 to 134,741. Section/index build rose from 0.019 s to 0.086 s,
section-score construction from 0.012 s to 0.043 s, and flow aggregation from
about 5 ms to 28–31 ms. The sample is not representative of all contexts.
These are single diagnostic processes with three repeated aggregation passes,
not full-simulation timings. The observed reduction does not currently justify
this prefix implementation; a generator-native or differently implemented
index could have different trade-offs.

### Prefix flow traversal using indexed arrays

The same captured prefix graph was tested with direct array propagation instead
of one Polars join and aggregation per tree depth. On the full 8-point capture,
processing all nodes with `numpy.add.at` took 30–34 ms for complete flow
aggregation, per-depth `bincount` took 24–30 ms, and visiting only nonzero
nodes with `add.at` took 21–22 ms. The original Polars tree path took 49–54 ms;
direct whole-section links took 13–15 ms. These include allocation aggregation
and final OD/occupation tables, not only the tree walk.

An experimental Rust kernel performs one reverse pass over node arrays. Node
IDs are depth-ordered, so each child's volume is added to its parent before
the parent is processed, and each node contributes to its physical component.
On the full capture, complete flow aggregation took 14.4–16.4 ms; the measured
array traversal including conversion of the result to a Polars table took
4.0–4.6 ms. Three passes of one fresh process are reported. The prefix index
construction still took 0.36 s and section-score construction 0.47 s, versus
0.18 s and 0.26 s for whole sections; the kernel does not optimize scoring,
candidate generation or those joins. It is not integrated into production.

On the 100-context 81-point sample, the Rust flow totals also matched the
expanded baseline to floating-point precision for OD and the same small
Float32-related occupation difference. Use `--prefix-flow-array` or compile
`tests/manual/prefix_flow_kernel.rs` and use `--prefix-flow-rust` in
`benchmark_plan_sections.py` to reproduce. These examples demonstrate that
the Polars per-level overhead is not intrinsic to prefix propagation; they do
not establish a full-run benefit or justify selecting a production language.

### Sections built before scoring-context step expansion

`tests/manual/capture_preexpanded_sections.py` observes the inputs and outputs
of the existing factored-candidate path in a two-step, 8-point, one-scenario
Geneva run. The selected second batch is captured immediately after physical
steps and memberships have been combined with active and retained candidates,
but before the context-to-step join. It also saves the evaluator's retained
step-scoring lookup and outputs. Profiling and file capture make this a
correctness dataset, not a simulation runtime benchmark.

The capture contains 208,456 shared input steps, 54,357 physical candidate
plans and 442,162 context/plan memberships. The existing join expands these
to 1,709,699 context-step rows. There are 102,114 physical-plan/travel-profile
pairs and 442,162 physical-plan/scoring-profile pairs. In this capture, keys
`travel profile + OD + mode` uniquely determine travel cost, time, distance
and route feasibility; `country + socioeconomic category + activity +
destination` uniquely determine the activity utility parameters. These facts
were checked against the captured retained lookup, not assumed for other
scenarios. The prototypes reject conflicting keys instead of substituting a
score.

`benchmark_preexpanded_baseline.py` expands the captured memberships and
steps, attaches the retained scoring lookup, updates modeled timing and
calculates complete-plan utilities. `benchmark_preexpanded_sections.py`
instead attaches travel profiles to physical plans, updates timing on 389,993
physical/profile steps, cuts them into rigid-bounded sections and then scores
sections and complete plans. Its `prefix` variant shares prefixes inside those
sections; `rigid` stores whole sections. Both calculate the home-night term and
map plan results back to all context memberships. Neither benchmark constructs
the retained lookup, generates candidates, assigns new modeled schedule IDs,
performs behavioral transitions or produces flows. The captured modeled-ID
mapping is used only to check output against the existing evaluator.

For a bounded comparison, the section scorer can process one profile at a
time with `--stream-profiles`; it discards section scores between profiles.
The table reports one fresh process for each method, with input file loading
and the one-time factoring of the already-retained scoring lookup reported
separately. Times before validation do not include the extensive comparison
joins. A 10 ms process-RSS sampler reports peak memory before validation; these
are prototype-process peaks, not simulation or incremental-memory bounds.

| Measured step | Expanded baseline | Whole sections, streamed | Section prefixes, streamed |
| --- | ---: | ---: | ---: |
| Read input / factor retained lookup | 0.13 s | 0.22 s | 0.20 s |
| Expand and attach lookup / physical timing | 0.19 s expansion and lookup | — | — |
| Timing and feasibility | 0.27 s | 0.14 s | 0.12 s |
| Section/index construction | — | 0.39 s | 0.55 s |
| Section scoring, including per-profile plan totals | — | 0.98 s | 1.71 s |
| Final plan scoring / context attachment | 0.25 s | 0.06 s | 0.07 s |
| Measured work excluding input read/factoring | 0.71 s | 1.57 s | 2.45 s |
| Peak process RSS before validation | 1.76 GiB | 1.23 GiB | 1.24 GiB |

The prototypes timed 389,993 physical/profile steps rather than 1,709,699
context steps and built 192,212 distinct sections with 378,974
physical-plan/section references. The prefix variant used 463,965 prefix
nodes. Across all profiles, whole sections processed 2,412,609 section-atom
references and prefixes processed 2,242,208 profile-prefix references; both
needed 2,103,719 distinct profile/atom scores. The largest streamed profile
held 86,534 section scores at once. The repeated per-profile joins and
aggregations dominate the section prototypes; these counts are not independent
expensive logarithm evaluations.

Both section variants matched all 442,162 captured input plans and feasibility
flags, and their maximum feasible plan-utility difference was 3.6e-14. All
1,709,699 context-step timing and duration values matched the existing
evaluation. The expanded baseline also matched all captured scoring inputs and
plan results. Equality of new schedule IDs, stable reduction order,
checkpoint/resume, multiple scenarios and successive cost changes have not
been demonstrated. The bounded prototypes demonstrate a memory/time trade-off
on this capture, not a production-ready evaluator or a full-simulation speedup.

#### Dense section/profile scoring census

The pre-expansion capture contains 192,212 timed physical sections and 19
scoring profiles, with 1,017,609 section/profile pairs actually requested by
the candidates. A fully dense table has 3,652,028 slots. The prototype embeds
the transport utility profile ID in each trip section, so restricting to that
same ID leaves at most 2,373,857 slots. This is a property of the prototype's
key, not evidence that travel times differ between profiles. Among 54,785
OD/mode keys observed for more than one profile in this captured lookup, none
had different travel times. Unobserved combinations and other scenarios have
not been checked. Some counted slots are still unavailable because this census
does not check whether all required activity or cost inputs exist per profile.

One dense Float64 utility matrix for all 3,652,028 slots would use about
27.9 MiB of raw values, plus eligibility and feasibility data and working
buffers. Sharing timed sections across utility profiles with identical travel
times could reduce the distinct physical section count; this census did not
measure that. Different route availability or travel times still require the
appropriate timing and feasibility state. The census does not benchmark dense
scoring or show that filling the table is faster. `benchmark_preexpanded_sections.py --strategy rigid
--dense-census` reproduces the counts without running the scoring comparison.

#### Shared timing with sparse and dense section scoring

`tests/manual/benchmark_dense_sparse_sections.py` compares sparse and dense
section/profile tables on the pre-expansion capture. Unlike the earlier
prototype, it groups the same physical plan across utility profiles when its
complete ordered travel-time inputs are identical. Route availability and
cost remain profile-specific, and a dense entry is marked unavailable if a
profile lacks a required input or has a different travel time. Feasibility is
not inferred from a missing score. The capture's retained scoring lookup is
an input; the existing modeled-ID mapping is used only for verification.

The 102,114 physical-plan/transport-profile pairs collapsed to 54,357 distinct
physical-plan/timing combinations. Timing was updated for 208,456 steps rather
than 389,993 in the prior profile-keyed timing prototype or 1,709,699 expanded
context steps. Rigid cuts then produced 130,288 physical sections and 208,550
physical-plan/section references. All 442,162 context/plan memberships remain
distinct choices. This factoring was checked against all captured modeled
clock/duration values; it does not assume other cost states or scenarios have
identical travel times.

The sparse method indexes the 1,017,609 section/profile pairs needed by the
candidate plans, scores only their 2,103,719 distinct profile/atom pairs and
retains the sparse scores. The dense method fills 2,475,472 section/profile
slots and evaluates 4,847,033 profile/atom pairs, with unavailable entries
masked. Both process profile inputs sequentially, reuse the same timed
sections, and calculate complete-plan/home-night utility. The sparse index is
rebuilt in this experiment; incremental maintenance when candidates are added
has not been implemented.

| Measure | Sparse | Dense |
| --- | ---: | ---: |
| Raw score table/index arrays, including feasibility | 16.5 MiB | 21.2 MiB |
| Build needed-pair index | 0.13–0.15 s | not needed |
| Initial profile scoring and plan sums | 1.84–1.87 s | 0.68–0.77 s |
| Subsequent plan sums from retained scores | 88–125 ms | 79–150 ms |
| Total initial work after loading, including timing and construction | 2.70–2.71 s | 1.37–1.56 s |
| Sampled process peak RSS before verification | 1.21–1.23 GiB | 1.09–1.13 GiB |

These are two fresh processes per variant, each with three cached plan-sum
passes, on one captured 8-point batch. The total initial work excludes input
file loading and the three extra cached-sum checks, but includes identifying
timing equivalence, section construction, score-table filling and plan
attachment. The prior expanded baseline for this capture took approximately
0.71 s for its measured evaluation slice and 1.76 GiB process peak in one
comparable run, excluding lookup construction and modeled-ID assignment.
Process peaks include other captured input tables and are not just score-table
memory. Sparse uses less raw score-table storage, but its repeated discovery,
filtering and `unique` operations make this first implementation slower;
this does not prove that an incrementally maintained sparse index would be
slower. Dense was faster in this diagnostic, but neither variant is a measured
full-evaluator or simulation speedup.

Both variants matched all 442,162 captured input-plan feasibility flags and
feasible utilities within 2.9e-14, and all 1,709,699 captured context-step
timings and durations. This checks one fixed state and scoring configuration;
modeled-ID generation, cache invalidation, added candidates, transitions,
multiple scenarios and checkpoint/resume are not implemented or tested here.

### Reusing dense sections across consecutive inner batches

A new diagnostic captured two consecutive real batches in outer step 2 of the
same Grand Geneve run, at 2 activity x 1 destination x 2 mode x 2 batch effort.
It retained the real physical inputs, memberships and scoring lookups, including
occupied and retained candidates. The experiment checks that timing settings,
shared physical plan rows and all existing score lookup entries agree across
batches before reusing anything. The second lookup adds entries; missing inputs
are therefore marked unknown and reconsidered later, not treated as permanently
infeasible. The cache is local to this captured outer step.

| Measure | Batch 1 | Batch 2 |
| --- | ---: | ---: |
| Context/plan memberships | 360,161 | 442,162 |
| Timed physical plans, total | 44,483 | 54,357 |
| New timed physical plans with reuse | 44,483 | 9,874 |
| Distinct sections, total | 105,318 | 130,288 |
| New sections with reuse | 105,318 | 24,970 |
| Section/profile slots evaluated, retained table | 2,001,042 | 1,164,340 |
| Section/profile slots evaluated, rebuilding | 2,001,042 | 2,475,472 |

One measured run per approach, in seconds, excluding capture, lookup
construction, modeled-ID assignment and validation:

| Batch 2 work | Retained dense table | Rebuilt dense table | Expanded calculation |
| --- | ---: | ---: | ---: |
| Identify timing and update schedules | 0.10 | 0.08 | 0.17 |
| Build section index | 0.23 | 0.20 | � |
| Score profiles and aggregate section utility | 1.02 | 2.21 | � |
| Attach home utility and plan results | 0.05 | 0.04 | � |
| Expand, attach scoring inputs and score plans | � | � | 0.30 |
| Total measured work | 1.43 | 2.60 | 0.46 |

The retained table saves work compared with this prototype's rebuild, but it
is still slower than the expanded calculation for these inputs. Its per-batch
section index is rebuilt from all retained timed plans, and its lookup filtering
and per-profile joins are Python/Polars prototype work; these measurements do
not establish the performance of a compiled or fully incremental index. The
raw retained score, feasibility and known-state arrays occupy 23.6 MiB in batch
2, excluding the timed plans, section index and other inputs. Process peak RSS
is not comparable here: verifying batch 1 raises the retained process peak
before batch 2 starts, while the expanded batches were measured separately.

Both retained batches reproduce all captured context/plan feasibility flags,
feasible utilities within 2.9e-14, and all modeled context-step times and
durations. This is an offline experiment on one fixed cost state, not a change
to the simulator or a full simulation benchmark. It does not evaluate persistence
across outer steps, scenarios, or checkpoints.

### High-effort consecutive batch scoring

Captured all contexts in the first two actual inner batches of outer step 2,
with one scenario and 3 activity x 3 destination x 3 mode draws, configured
for three batches. The capture stopped after the second batch. Diagnostic I/O
makes its wall-clock times unsuitable as production timings. Batch 1 contained
2,493,976 context/plan memberships and 10,080,791 expanded steps; batch 2
contained 2,917,575 memberships and 11,798,280 expanded steps.

The section prototype still constructs and times physical plan-step tables
before cutting them into sections. It retains timed plans, section IDs,
plan-to-section references and dense scores across these two batches. It is
not a prototype that generates candidates directly as section references.
The second batch adds 50,187 of 353,005 timed plans and 78,233 of 536,526
sections; 302,818 timed plans and 458,293 sections were retained from batch 1.
All previously present physical rows and scoring lookup entries were checked
for equality before reuse. Scores lacking inputs are not treated as valid and
are recalculated when inputs appear.

Measured slices at eight Polars threads (seconds, excluding loading, modeled-ID
assignment, lookup construction, and validation):

| Approach | Batch 1 | Batch 2 |
| --- | ---: | ---: |
| Expanded scoring; separate utility/aggregation stages | 3.37�4.90 | 4.06�5.47 |
| Dense sections, rebuilding each batch | 4.38 | 5.35 |
| Dense sections, retaining timings, references and scores | 4.29�6.94 | 3.80�3.80 |

These are a few sequential runs under variable machine load, not a controlled
end-to-end speedup measurement. Separate timings for the expanded path impose
a materialization boundary between utility and aggregation. One batch-2 run
spent 0.49 s expanding, 0.59 s joining scoring inputs, 1.47 s updating times
and checking feasibility, 0.02 s calculating step utility, 1.33 s aggregating
by plan, and 0.16 s attaching home utility. The expensive work is organizing,
timing and aggregating plans, not the formula alone.

The retained section prototype initially spent 8.7 s in batch 2 because it
repeatedly selected and sorted atoms. Scoring the dense atom inputs and
retaining plan-to-section references reduced this to about 3.8 s. A small Rust
loop over already-scored section atoms and plan references matched all plans,
but its total batch times (4.83 s at eight threads and 3.77 s at 16 in single
runs) do not establish a useful gain over the NumPy reductions under variable
load. The loop is a diagnostic, not production code.

For the expanded batch-2 slice, one run each with 8, 16 and 22 Polars threads
took 4.06, 3.14 and 3.02 s respectively; changing thread counts has not been
benchmarked in a full simulation. The real evaluator has additional work:
for example, the uninstrumented first outer step's three candidate evaluations
were 4.14, 6.11 and 5.85 s, with materializing and indexing modeled timings
accounting for 1.56, 2.42 and 2.38 s respectively. These are nested timings,
not extra time to add to evaluation. Capturing batch inputs adds I/O to step 2,
so its complete evaluation time cannot be compared directly.

Each approach matched all captured feasibility flags and all modeled step
clock/duration values. Maximum feasible utility error was below 5e-14,
including the compiled summation; the compiled summation changes floating-point
addition order. Results cover one captured fixed cost state; modeled-ID
assignment, choices, candidate generation and transitions were not replaced.
The current evidence does not decide the performance of a representation that
generates or maintains sections directly, nor its integration cost.

### Integer-minute section timing experiment

`tests/manual/benchmark_minute_sections.py` evaluates the same two captured
inner batches with integer minutes. Survey reference departure, arrival and
next-departure times and modeled travel times are rounded to the nearest
minute (ties to even). Each modeled departure is the reference departure minus
the rounded rigidity adjustment; modeled arrival is that departure plus the
rounded travel time. The next modeled departure determines activity duration,
with zero as the minimum. Feasibility uses these same integer clock values.
Activity utility uses minutes divided by 60 and a zero-duration activity has
zero activity utility. Home-night utility uses the minute-based activity and
travel totals. The per-step generalized costs and opportunity inputs remain
those captured from the existing second-resolution simulation. Candidate
generation, routing, transport generalized-cost construction and behavioral
transitions are not changed; this is not yet a full minute-resolution run.

In the 3 x 3 x 3, three-batch-configured capture, the second batch contained
2,917,575 input context/plan alternatives. Relative to captured second-based
results, 11,827 inputs changed from infeasible to feasible and 18 changed in
the opposite direction; these count input alternatives before modeled-plan
deduplication. Of the 2,118,390 feasible in both, the median, 90th and 99th
percentile absolute plan-utility changes were 0.018, 0.088 and 0.222.

After deduplicating modeled schedules under each timing model, fresh logit
probabilities were aggregated to activity/destination/mode sequences (ignoring
schedule differences) for each scoring context. Population-weighted total
variation was 0.55% in batch 2 and 0.53% in batch 1, using coefficient 0.25
and the same captured candidates and costs. This is not an OD-flow difference
or a simulation-convergence estimate. The corresponding batch-2 feasible mobile
modeled choices were 1,773,041 on the minute grid versus 1,761,813 in the
captured second-based evaluator.

The second batch retained 457,613 existing timed sections and added 78,112,
for 535,725 in total, versus 536,526 in the second-based prototype. Both
retained 302,818 timed physical plans and added 50,187. This prototype still
keys timing plans by original physical plan identity and does not merge all
possible minute-equivalent structures, so the result does not measure the
potential reuse of a new representation built directly at minute resolution.
Neither experiment retains sections across outer steps.

In batch 2, 38 physical timed step rows had at least one clock value outside
0�1,440 minutes; observed clocks ranged from -2 to 1,499. These include
candidate steps that may be infeasible. A minute type must permit values
outside the modeled day unless explicit day-boundary rules are adopted. The
small capture ranged from -22 to 1,477. Do not silently clamp these values.

For each batch, a sample of 100 input plans was independently checked with
plain Python integer timing, feasibility and utility arithmetic. Comparisons
to the captured model cover every candidate in both batches; the scoring
sample is a check of the diagnostic, not proof of a full-model implementation.

With eight Polars threads, repeated minute-prototype measurements varied from
4.64 to 7.81 s for batch 1 and 3.66 to 5.92 s for batch 2; a sequential
second-resolution run took 4.29 s and 3.71 s. This does not demonstrate a
speedup from integer minutes. These slices include section indexing but not
candidate generation, lookup construction, modeled-ID assignment, or result
validation, and machine load varied. The minute prototype computes minute
values from captured second-based inputs on every batch; an implementation
that stores and generates minutes from the beginning would have different
costs and has not been measured.

### Two-sided activity rigidity and cross-step sections

Activity timing now divides a trip's travel-time change using both the origin
and destination activity rigidities. The origin's share is `(1 + destination -
origin) / 2`; the destination absorbs the rest. Home defaults to zero, and
anchors other than home default to one. Equal rigidity gives an even split.
The utility-based destination guide and its conservative feasibility filter
use the same rule, including the prior activity and, when available, its
preceding trip. `activity_time_rigidity` replaces the earlier arrival-only
parameter; old run and checkpoint identities are invalidated.

An instrumented lowest-effort Geneva run captured the first inner batch in
outer steps 1 and 2. It used one scenario, one seed, 1 activity x 1
destination x 1 mode draw and one inner batch per step. Capture includes disk
I/O and is not an end-to-end timing benchmark. The direct expanded scoring
check reproduced 32,798 and 70,722 input context/plan results; all feasibility
flags matched and feasible utility error was at most 1.5e-14.

The structural diagnostic cuts immediately before a rigidity-1 activity:
the arriving trip belongs to the preceding section and the activity belongs
to the following section. At each activity, duration still uses both the
incoming trip's arrival and the outgoing trip's departure, even when those
trips are in neighboring sections. An independent expanded-row calculation
uses retained section references to aggregate trip and activity contributions;
it reproduced all 123,528 and 266,320 captured modeled step times and
all 32,798 and 70,722 input-plan feasibility flags. Feasible utility error
was at most 2.9e-14. This is a correctness check and does not yet time or
score unique sections independently.

The first capture contained 4,024 physical plans and 5,697 structural sections.
The second contained 8,689 physical plans; mapping the first step's reference
schedule IDs to its modeled IDs allowed all 4,024 retained plans and their
5,697 section references to be reused, with 4,665 new structural plans and
6,686 new sections added. A naive comparison of current time IDs and current
reference rows found no retained plans, because the previous evaluation had
assigned modeled schedule IDs and changed times. A cross-step structure must
therefore keep reference identity separately from the current modeled timing;
the experiment uses the actual captured mapping and checks retained physical
activities, locations and modes. It stops if a modeled key maps to multiple
prior references, so merged-reference handling remains unresolved.

In a single diagnostic run, building the second-step structural index took
about 0.038 s with reuse, including 0.013 s reference mapping, versus 0.040 s
to rebuild from the second-step rows. These are very small, noisy index-only
measurements. Reuse and rebuild also describe reference-time and modeled-time
structures respectively, so they are not evidence of a full evaluation speedup.
Retiming and scoring in the check still operate on expanded rows, and the
index is neither integrated into the simulator nor retained through checkpoints.

### Five-scenario early-divergence memory diagnostic

A cache-only, two-step probe activated the carbon-tax and transit interventions
at step 1, used full replanning, 3 activity x 3 destination x 3 mode draws,
three search batches, one seed and eight Polars threads. Both congestion effects
were enabled. A 24 GiB process-tree guard and 10 GiB available-memory reserve
were used. This is a stress test, not the production intervention schedule or a
convergence assessment. Instrumentation adds overhead. The first process was
stopped after writing the step 2 checkpoint and before final cost calculation.

The step 2 candidate pool reached 588,825 identities. During the second batch,
after the last scenario evaluation, Python RSS was 18.06 GiB and the sum of
reachable Polars dataframe estimates was 10.90 GiB. Before the next candidate
pool, these were 16.48 GiB and 4.23 GiB respectively. After the completed
behavioral update, RSS was 9.83 GiB and the dataframe estimates totaled 5.76 GiB;
explicit Python garbage collection did not materially change either number.
The trace recorded a 23.06 GiB Python RSS peak in step 2 between these snapshots.
Dataframe estimates may double-count shared buffers and do not include all
native allocations, lazy plans or other process memory, so their difference from
RSS is not a measurement of allocator retention.

A fresh process attempting to restore the completed checkpoint was stopped by
the same guard at 24.31 GiB process-tree RSS, before finalization. The restore
trace includes candidate checkpoint loading and rebuilding initial states. Each
scenario's saved candidate file expands compact history to roughly 85 million
candidate-step rows. In an isolated process, loading one 84,891,825-row file
used 4.52 GiB of estimated dataframe memory and raised RSS from 0.07 to 5.49 GiB.
This confirms a substantial live-data and checkpoint-expansion problem; it does
not quantify how much of the remaining RSS is allocator retention. The traced simulation steps took approximately 16 minutes, including tracing,
inventories and checkpoint writes. This excludes setup and final output work.

A credible pre-run cost estimate needs calibration from comparable complete runs,
separate accounting for physical candidates, context memberships, retained
scenario evaluations and checkpoint expansion, and an uncertainty range. The
observed candidate and memory growth changes with scenario divergence and
replanning phases; one run cannot support a reliable estimate for 20 steps.
