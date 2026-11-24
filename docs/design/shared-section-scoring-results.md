# Shared section scoring results

The evaluator can score timed sections by transport and activity profile, then
attach the resulting utilities to scoring contexts. It keeps shared physical
steps separate when computing inner flows, and expands only selected plan
details for the outer transition. This path is used when the compact candidate
and score lookups are available and transition distances and event output do
not require the full step table. Other cases use the existing evaluator. The
candidate builder supplies physical steps and context memberships separately;
the scorer builds distinct trip/profile inputs without expanding every context
to its steps. The expanded form remains available for fallback cases. A bounded
cache can retain exact timed-section identities across outer steps; scores are
discarded when the step changes. Scored sections can be reused between batches
with the same costs and opportunities.

On 16 November, matched two-step Geneva runs with one scenario and effort
2 activities × 1 destination × 2 modes × 2 batches produced identical saved
candidate IDs, plan allocations, utilities and random states at both steps.
The section run took 55.3 s and the expanded run 53.7 s. These single runs do
not show a speed benefit at this size.

A 3 × 3 × 3 × 3 run completed two steps in 124.8 s with 385,492 final candidates
and an externally sampled process-tree peak of 11.25 GiB. Section scoring took
5.2 s in step 1 and 16.9 s in step 2, within total evaluation times of 11.9 s
and 34.6 s. The matched expanded run was stopped by the external 12 GiB guard
during the second step at 12.10 GiB. Its first-step candidate IDs, plan
allocations, utilities and random state matched the section run exactly. The
two full-run times therefore cannot be compared. An earlier section-scoring
attempt also hit the guard; limiting the temporary expansion of section/profile
combinations to batches of 100,000 enabled this run to finish.

Removing the initial context-step expansion preserved all saved candidate IDs,
plan allocations, utilities and random states in the same two-step high-effort
case. The rerun took 125.0 s and the external guard sampled 10.88 GiB; the
previous section run took 124.8 s and sampled 11.25 GiB. These single runs
suggest a memory reduction but do not demonstrate a timing improvement.

The experimental home-zone pool was also rerun with this path, using the same
previously selected survey schedules as the earlier smoke test. The two-step
run finished in 68.8 s with a 7.42 GiB externally sampled peak, versus 76.2 s
and 8.82 GiB before section scoring. Aggregate trip counts agreed to floating
point precision. This tests integration with the selected pool; it is not a
new ranking of all possible home-zone plans.

In a two-step 2 × 1 × 2 × 2 diagnostic, 6,395 of 61,936 section references in
the first batch of step 2 matched identities retained from step 1. In the next
batch of step 2, 62,303 of 74,218 references matched the existing cache. The
cross-step cache reuses identities, not scores, and does not avoid rebuilding
the current plan-to-section references. A matched high-effort run produced
exactly the same saved plan states and random states; it took 129.9 s versus
125.0 s without the cross-step cache and sampled 10.91 versus 10.88 GiB. These
single measurements do not establish a speedup from cross-step identity reuse.

An experiment added section totals rather than flattening and sorting all
step contributions for each plan. In isolated medium-effort batches, the
largest measured utility difference was 8.6e-14 and the aggregation itself
was faster. In a complete two-step run, first-step allocation differences
were at most 6.8e-14, yet the next step generated eight additional candidate
IDs and its mode trip totals changed by up to about three trips. The random
state still matched. The full run took 52.3 s versus 52.2 s for the existing
aggregation, so the section-total variant was not retained. This result does
not identify which later calculation amplified the small initial differences.

In the same medium-effort setup, 30,467 of 375,799 section/profile scores in
the first batch of step 2 also occurred in step 1, and 24,418 of those had
unchanged score components. Thus at most 6.5% of that batch's scores could
have been reused across steps, before accounting for the cost of detecting
changed inputs. Scores continue to be recalculated after outer-step updates.

A separate exact timing-reuse prototype used two captured inner batches from a
high-effort step. Of 671,311 plan/profile combinations in batch 2, 571,069
were already present in batch 1, with identical modeled step times and schedule
IDs. Timing only the 100,242 new combinations and rebuilding the measured
plan/profile step outputs took 0.92 s, compared with 1.97 s for the current
full timing call; the compared steps and schedule IDs were identical. This is
an isolated prototype, not a simulation speedup. In the live simulation the
schedule catalogue is a new object between batches, so a production cache must
also establish that retained schedule IDs still refer to the same schedules.

A bounded implementation of that cache was then tested in the simulator. It
checked the schedule index and compared physical steps before reuse. Saved
candidate IDs, plan allocations, utilities and random states matched exactly
in both medium- and high-effort two-step runs. In the medium case, total
time increased from 52.2 to 55.5 s and shared timing from 0.74 to 0.94 s. In
the high case, shared timing fell from 7.18 to 6.55 s across both steps; total
time was 120.9 s versus 129.9 s in the previous run, but that larger difference
cannot be attributed to timing alone. These are single runs. The cache was not
retained because the measured timing gain was small and the exact comparisons
and retained tables increased complexity and slowed the medium case.

Activity durations and travel times are now aggregated once per timed physical
plan instead of once per scoring profile. The sorted addition order is unchanged.
On captured high-effort batches, the section-score cache fell from 379 MB to
177 MB and the diagnostic's sampled peak fell from 8.86 to 6.01 GiB. A matched
two-step simulation produced identical candidate IDs, allocations, utilities and
random states. It took 121.9 s with an 8.11 GiB sampled peak, compared with
129.9 s and 10.91 GiB in the previous single run. The large reduction in cache
size is measured directly; whole-run timings and peaks can vary between runs.

In a five-step, one-scenario 3 × 3 × 3 × 3 run, the 12 GiB external guard
initially stopped the final batch: the retained section cache left too little
budget for the compact trip lookup, causing a fallback to expanded candidates.
The lookup now takes priority and can evict reusable section scores. The rerun
completed in 366.5 s with a 10.93 GiB sampled process-tree peak and 481,911
final candidates. Steps 1–4 matched the stopped run exactly for candidate IDs,
allocations, utilities and random states. Step times were 27.0, 69.4, 75.1,
82.0 and 89.9 s. This is one run, not an estimate of convergence or accuracy.

At 4 × 4 × 4 samples and three batches, the first two-step attempt hit the
12 GiB guard during compact section scoring. Plan aggregation now processes
one profile and at most 100,000 plans at a time, preserving sorted step sums.
The rerun completed in 222.7 s with 785,451 final candidates and an 11.76 GiB
sampled peak. Its first-step candidate IDs, allocations, utilities and random
state exactly matched the stopped run; there was no completed second-step
state from that run to compare. The observed peak leaves little headroom under
this guard and is not evidence that substantially larger pools will fit.

If a trip lookup itself cannot be retained within the shared cache budget, it
is now used temporarily for the current section evaluation instead of forcing
expanded candidate rows. A two-step 2 × 1 × 2 × 2 run with the retention
budget deliberately set to zero matched the normal run exactly for saved
candidate IDs, allocations, utilities and random states. It took 52.7 s with
a 6.54 GiB sampled peak. The budget controls retained lookups; it is not a
hard limit on the memory needed by an individual evaluation.

The compact lookup is also available for a single inner batch. In a two-step
lowest-effort comparison, section scoring was used in both steps and matched
the expanded evaluator exactly for saved candidate IDs, allocations, utilities
and random states. The section run took 40.1 s versus 39.6 s for the expanded
run; these single measurements do not show a low-effort speed improvement.

A two-scenario, two-step 2 × 1 × 2 × 2 run activated the carbon-tax scenario
in step 2. The scenarios then had different trip flows. Their saved candidate
IDs, plan allocations, utilities and random states exactly matched a run using
the expanded evaluator. The section run took 67.5 s with a 6.40 GiB sampled
peak; the expanded comparison took 69.0 s and 6.63 GiB. These are single runs.

Further changes reduce temporary work at larger pool sizes. Candidate content IDs
for infeasible physical plans are calculated once per physical plan instead of
once per scoring context. Reachable allocations are processed in demand-unit
batches, then combined before context aggregation to preserve the order of
floating-point sums. Temporary context plan IDs are rebuilt for each evaluation;
the persistent demand-unit IDs remain separate. A two-scenario, two-step run
matched the prior saved candidates, allocations, utilities and random states
exactly after these changes. These steps reduce intermediate memory, but they
do not eliminate context-level candidate tables.

A one-scenario, two-step run at 5 x 5 x 5 samples and three batches completed
with the section representation: 323.0 s total, 12.98 GiB sampled process-tree
peak, and 1,331,128 candidates at the end. The first step matched the earlier
stopped attempt exactly for candidate IDs, allocations, utilities and random
state; there is no completed second-step baseline at this size. The second
step took 226.7 s, including 211.7 s in the inner loop. A previous attempt
was stopped by a 12 GiB process-tree guard; the completed run used a 16 GiB
limit and a 12 GiB system-available reserve. These single runs establish that
this case completed, not a speed or scalability guarantee.

A direct aggregation for plans consisting of one section was tested on captured
3 x 3 x 3 batches. About three quarters of distinct timed plans had one section.
The direct path produced exactly equal results in an isolated comparison and
reduced captured scorer times in single measurements, but a two-step run took
124.4 s versus 121.9 s in the earlier run, with scorer totals of 20.4 versus
19.3 s. Other changes separate those runs, so this is not an isolated timing
comparison. Saved candidate IDs, allocations, utilities and random states
matched exactly. The extra branch was removed because a whole-run benefit was
not established.

In a captured first inner batch, 2,357,873 profile-section scores expanded to
8,246,511 profile-step rows. Those represented 5,396,582 distinct profile-step
combinations and 643,101 distinct physical steps. Factoring profile-step work
could reduce repeated calculations, but would require another index and join;
these counts alone do not establish a speed or memory benefit.
A prototype that computed each distinct profile-step once and joined it back
to sections was compared on 500,000 captured profile sections. Its scores
matched exactly, but it took 0.37 s and peaked at 1.28 GiB versus 0.26 s and
1.09 GiB for direct scoring. This single captured comparison does not justify
adding the extra index and join.

The experimental home-pool runner can now construct physical steps and context
memberships directly, rather than constructing the 1,749,156 context-step rows
first. Its temporary destination IDs are local to each home zone; the runner
includes the zone when reconstructing samples and converts to canonical IDs
before joining the shared pool. A two-zone reconstruction check matched the
expanded builder exactly.

The old selected ranking no longer matched current rigidity inputs. The 9,898
contexts' previously selected schedules were rescored; this refresh considered
only those schedules, not all eligible survey plans. With the refreshed pool,
one scenario and two steps at 1 x 1 x 1 samples and two batches produced the
same saved candidate IDs, allocations, utilities and random states with direct
and expanded construction. Each produced 556,800 distinct physical step rows
and 299,540 memberships. Preparation took 1.03 s direct versus 0.98 s expanded;
total run time was 73.7 versus 68.3 s and the sampled process-tree peak was
6.70 versus 6.54 GiB. These single runs do not demonstrate a speed or peak-memory
benefit. This is a manual experiment, not a production home-pool feature.

A large captured second-step batch had 8,144,287 context/plan rows across
9,898 scoring contexts. No two contexts shared an identical complete choice
set within a scoring profile, and each profile/timing-plan combination occurred
in only one context. Whole-choice-set sharing would therefore not compress this
capture. This does not rule out shared subsets in other populations or runs.

Different incoming schedules nevertheless sometimes converged to the same
modeled plan: 1,253,553 rows mapped to 369,135 repeated modeled plans in that
capture. Their utility and feasibility values agreed exactly. An experiment
removed duplicate modeled plans before section scoring while retaining the
incoming mappings for history. Captured scores and a complete two-step
5 x 5 x 5, three-batch run matched the previous saved candidate IDs,
allocations, utilities and random states exactly. The full run took 337.8 s
and scored sections in 113.3 s, compared with 323.0 s and 105.0 s in the prior
run; peak process-tree RSS was 12.91 versus 12.98 GiB. These single runs do not
isolate system variability, but did not justify keeping the additional early
deduplication. The current evaluator still merges these plans after scoring.

Complete modeled-plan scores were also tested as a per-outer-step cache. In a
captured 3 x 3 x 3 second batch, 2.12 million of 2.53 million distinct modeled
plan/profile scores repeated exactly from the first batch. The cache reduced
single captured second-batch scoring from about 5.0 to 3.5 s. A two-step run
matched the earlier saved states exactly and took 118.4 s versus 124.4 s in a
comparison run that included a separate aggregation experiment.

At 5 x 5 x 5, the complete-plan cache again matched saved candidate IDs,
allocations, utilities and random states exactly, but the full run took 334.2 s
and peaked at 13.82 GiB versus 323.0 s and 12.98 GiB without it. Section
scoring totaled 111.2 versus 105.0 s. These are single measurements, but the
large-run result did not justify the added cache, memory and local-ID ownership
complexity, so the experiment was removed. The existing section-score cache
remains scoped to fixed costs and opportunities.

An exact two-trip timing-window diagnostic tested sharing modeled activity
clocks before scoring. A window contains the preceding and following activity
rigidity inputs, the current and next reference trip times, modeled travel
times, and the final departure fallback. This accounts for the following trip's
effect on the current activity duration. The diagnostic keeps full plan rows
only to compare against the existing updater; it does not integrate section
references into timing or represent an end-to-end simulation.

In captured 3 x 3 x 3 batches, 1,424,581 second-batch timed step rows contained
936,552 distinct windows, of which 797,013 occurred in the previous batch. All
modeled departure, arrival, next-departure and duration values matched exactly.
Building, caching and reattaching windows took 1.13 s in batch two, compared
with 0.22 s for the existing updater. This does not establish the performance
of generating sections directly and never rebuilding full timed rows, but the
prototype itself is slower and was not added to the production path. Reproduce
it with `tests/manual/benchmark_timing_windows.py` using captured timing inputs.

A compressed context-to-timing mapping was measured on a captured large batch.
The original 8,144,287-row table, including plan keys and profile columns,
occupied 309 MB. Storing context-to-timing references, one key record per
992,340 timing plans, and one profile record per context occupied 122 MB;
the reconstructed rows had the same row count and aggregate hash. The timing
result now retains context links and timing-plan keys separately, reconstructing
full keys only for callers that need them. The scorer attaches profiles directly
to those references and the outer updater reuses the scorer's mapping.

Captured-batch checks and a two-step 3 x 3 x 3 run matched saved candidate IDs,
allocations, utilities and random states exactly. That run took 129.8 s and
peaked at 7.05 GiB versus 121.9 s and 8.11 GiB in an earlier run; unit checks
ran concurrently with the newer run, so the timing is not a clean comparison.
