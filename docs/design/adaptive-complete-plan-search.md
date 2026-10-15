# Sequential Shared Plan Generation

**Status:** Survey schedules are pooled by country and day type. Sequential generation is shared by home zone and utility profile, and surviving complete plans are attached to demand units after generation. The destination top-K search path has been removed for this implementation. Adaptive batches and convergence-based stopping are future work.

## Flow

1. Build weighted activity support sets by country and CSP. A demand unit uses its CSP set, with a country set as fallback.
2. Pool survey plan weight mass across demographic segments. For each home zone, sample schedules with replacement from the union of schedules compatible with at least one activity profile in that zone. If a finite draw batch misses a profile, add a weighted schedule draw for coverage.
3. Sample destinations for each zone and schedule, then generate mode sequences for each zone and utility profile.
4. Before expanding complete plans to group and subgroup IDs, score them once for each distinct context: country, CSP, home zone, activity profile, and utility profile. The score includes transport cost, activity-duration utility, destination saturation, modeled travel time, and home-night utility.
5. Apply the configured transition utility window to this compact candidate set. The window is grouped by the alternatives allowed under the current behavior scope: all plans for full replanning, the same schedule for destination replanning, and the same schedule and destination for mode replanning. The no-transition scope keeps generated candidates unchanged.
6. Attach surviving complete plans to demand units when the schedule activities are supported and the utility profile matches. Existing candidate memory keeps current and previously generated plans.
7. Recompute utilities and exact transition MNL probabilities per demand unit. These probabilities depend on each unit's current plans, transition scope, and optional plan-distance effects, so they remain in the existing choice step. Apply transitions and allocate population as before.

## Why weights and activity profiles are used

Survey weights define the proposal distribution for schedule draws. The activity profile is an eligibility rule: every non-home activity in a schedule must be in the profile's allowed set. Activity-profile and CSP-specific utility differences are represented in the scoring context, so matching activity sets do not force CSPs to share utilities.

## Pruning meaning

The pre-attachment filter reuses `transition_utility_pruning_delta`; it does not add a probability cutoff. For a given choice context, a candidate outside the utility window cannot pass the later utility-window filter. The compact prefilter can retain extra plans when previously stored or current plans raise the later maximum. The existing MNL and transition rules then run on the attached units.

## Scope

This implementation uses the sequential destination sampler and shared schedule generation as the only path. Mode alternatives still use the existing top-K mode-sequence search. The first version still materializes step rows for surviving candidates after attachment; a normalized complete-plan catalogue and adaptive search batches can be considered after measuring this path.
