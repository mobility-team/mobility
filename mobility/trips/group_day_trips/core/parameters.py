from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator


class BehaviorChangeScope(str, Enum):
    """Highest adaptation layer allowed during one behavior-change phase.

    Attributes:
        FULL_REPLANNING: Resample motive sequences, then dependent destination
            and mode sequences. Stay-home transitions remain available.
        DESTINATION_REPLANNING: Keep each currently occupied non-stay-home
            motive sequence fixed and resample destination sequences plus
            dependent mode sequences. Stay-home is frozen.
        MODE_REPLANNING: Keep each currently occupied non-stay-home motive and
            destination sequence fixed and resample mode sequences only.
            Stay-home is frozen.
    """

    FULL_REPLANNING = "full_replanning"
    DESTINATION_REPLANNING = "destination_replanning"
    MODE_REPLANNING = "mode_replanning"


class BehaviorChangePhase(BaseModel):
    """Behavior-change phase applied from ``start_iteration`` onward.

    Attributes:
        start_iteration: First simulation iteration where this phase applies.
        scope: Highest adaptation layer allowed during the phase.
    """

    model_config = ConfigDict(extra="forbid")

    start_iteration: Annotated[
        int,
        Field(
            ge=1,
            title="Start iteration",
            description="First simulation iteration where this behavior-change phase applies.",
        ),
    ]

    scope: Annotated[
        BehaviorChangeScope,
        Field(
            title="Behavior change scope",
            description=(
                "Highest adaptation layer allowed during the phase. "
                "`full_replanning` resamples motive, destination, and mode "
                "sequences. `destination_replanning` keeps each currently "
                "occupied non-stay-home motive sequence fixed and resamples "
                "destination plus mode sequences. `mode_replanning` keeps each "
                "currently occupied non-stay-home motive and destination "
                "sequence fixed and resamples mode sequences only. "
                "Stay-home is frozen in restricted phases."
            ),
        ),
    ]


class Parameters(BaseModel):

    model_config = ConfigDict(extra="forbid")

    n_iterations: Annotated[
        int,
        Field(
            default=1,
            ge=1,
            title="Number of iterations",
            description=(
                "Number of simulation iterations used to compute the population "
                "trips. Increase this to get more diverse programmes and to allow "
                "congestion feedbacks to propagate."
            ),
        ),
    ]

    alpha: Annotated[
        float,
        Field(
            default=0.01,
            ge=0.0,
            title="Next anchor cost weighting",
            description=(
                "Weight of the cost to get to the next anchor destination in the "
                "chain when considering destination options (shopping place when "
                "the next anchor is work, for example) and computing probabilities."
            ),
        ),
    ]

    k_activity_sequences: Annotated[
        int | None,
        Field(
            default=3,
            ge=1,
            title="Number of activity-sequence seeds",
            description=(
                "Maximum number of timed survey activity-sequence seeds sampled "
                "without replacement per demand group during full replanning. "
                "If omitted, all available seeds are admitted."
            ),
        ),
    ]

    k_destination_sequences: Annotated[
        int,
        Field(
            default=3,
            ge=1,
            title="Number of destination sequences",
            description=(
                "Number of destination-chain sequences generated per admitted "
                "activity sequence. Sequences branch at the anchor-destination "
                "sampling stage and then complete the remaining destinations "
                "conditionally."
            ),
        ),
    ]
    
    k_mode_sequences: Annotated[
        int,
        Field(
            default=3,
            ge=1,
            title="Number of mode combinations",
            description=(
                "Number of mode combinations considered in the simulation, for "
                "a given destination sequence. Only the top k combinations are "
                "considered."
            ),
        ),
    ]

    n_warmup_iterations: Annotated[
        int,
        Field(
            default=1,
            ge=0,
            title="Candidate memory warm-up iterations",
            description=(
                "Number of initial iterations during which no candidate-plan "
                "memory is forgotten. This gives newly generated plans at "
                "least one iteration to become active before age-based pruning "
                "starts."
            ),
        ),
    ]

    max_inactive_age: Annotated[
        int,
        Field(
            default=2,
            ge=0,
            title="Maximum inactive candidate-plan age",
            description=(
                "Maximum number of iterations an inactive candidate plan is "
                "kept in memory after it was last active. Plans that were "
                "never active use their first-seen iteration as the age "
                "reference."
            ),
        ),
    ]

    dest_prob_cutoff: Annotated[
        float,
        Field(
            default=0.99,
            gt=0.0,
            le=1.0,
            title="Destination probability distribution cutoff",
            description=(
                "Cutoff used to prune the less probable destinations for a given "
                "origin. Only the first dest_prob_cutoff % of the cumulative "
                "distribution is considered."
            ),
        ),
    ]

    n_iter_per_cost_update: Annotated[
        int,
        Field(
            default=3,
            ge=0,
            title="Travel costs update period",
            description=(
                "The simulation will update the travel costs every n_iter_per_cost_update. ",
                "Set n_iter_per_cost_update to zero to ignore congestion in the "
                "simulation, set to 1 to update congestion at each iteration, " 
                "set to a higher level to speed up the simulation."
            ),
        ),
    ]

    cost_uncertainty_sd: Annotated[
        float,
        Field(
            default=1.0,
            gt=0.0,
            title="Standard deviation of travel costs estimates",
            description=( 
                "Travel costs are estimated between specific representative points " 
                "located in transport zones, but the actual travel costs between "
                "all origins and all destinations in the transport zones will be "
                "slightly different than these point estimates. "
                "cost_uncertainty_sd controls how uncertain are these estimates, "
                "and spreads the opportunities in the destination transport zone "
                "based on a normal distribution centered on the point estimates "
                "and a standard deviation of cost_uncertainty_sd."
            ),
        ),
    ]

    seed: Annotated[
        int,
        Field(
            default=0,
            ge=0,
            title="Simulation seed",
            description=(
                "Seed used to get reproducible results for stochastic simulation "
                "steps (like destination sampling when building destination chains). "
                "Change this value to get new programmes and results for a given " 
                "set of inputs."
            ),
        ),
    ]

    mode_sequence_search_parallel: Annotated[
        bool,
        Field(
            default=True,
            title="Parallel mode for the top k mode sequence search",
            description=(
                "Set to False to debug or for small simulations, otherwise set " 
                "to True to speed up the simulation."
            ),
        ),
    ]

    use_rust_mode_sequence_search: Annotated[
        bool,
        Field(
            default=False,
            title="Use Rust mode-sequence search backend",
            description=(
                "Whether to use the in-process Rust backend for top-k mode "
                "sequence search instead of the legacy Python implementation. "
                "This requires the `mobility_mode_sequence_search` package to "
                "be installed and importable."
            ),
        ),
    ]

    save_transition_events: Annotated[
        bool,
        Field(
            default=False,
            title="Save transition events",
            description=(
                "Whether to persist per-iteration transition-event logs. "
                "These logs are useful for post-run analysis but are not "
                "required to advance the simulation state."
            ),
        ),
    ]

    persist_iteration_artifacts: Annotated[
        bool,
        Field(
            default=False,
            title="Persist iteration artifacts",
            description=(
                "Whether to persist resumable per-iteration run-state artifacts "
                "such as current plans, current plan steps, candidate plan steps, "
                "destination saturation, RNG state, and completion markers. "
                "Disable this to keep only the final outputs."
            ),
        ),
    ]

    min_activity_time_constant: Annotated[
        float,
        Field(
            default=1.0,
            ge=0.0,
            title="Minimum activity time coefficient",
            description=(
                "Coefficient controlling the minimum activity time necessary to "
                "get a positive utility from the activity. This minimum time is "
                "equal to average_activity_time x exp(-min_activity_time_constant)."
            ),
        ),
    ]

    update_plan_timings_from_modeled_travel_times: Annotated[
        bool,
        Field(
            default=False,
            title="Update plan timings from modeled travel times",
            description=(
                "Whether to adjust candidate-plan departure, arrival, and activity "
                "times from survey reference schedules using the modeled travel "
                "times at each iteration before recomputing utilities."
            ),
        ),
    ]

    transition_distance_threshold: Annotated[
        float,
        Field(
            default=float("inf"),
            ge=0.0,
            title="Transition distance threshold",
            description=(
                "Maximum embedding distance allowed between a current plan state "
                "and a candidate plan state. Candidates beyond this threshold are "
                "excluded from the transition choice set."
            ),
        ),
    ]

    enable_transition_distance_model: Annotated[
        bool,
        Field(
            default=False,
            title="Enable transition distance model",
            description=(
                "Whether to compute plan-embedding distances during transition "
                "probability calculation. When disabled, the distance threshold "
                "and distance friction are ignored to avoid the costly distance "
                "join and pairwise distance calculation."
            ),
        ),
    ]

    transition_revision_probability: Annotated[
        float,
        Field(
            default=0.3,
            ge=0.0,
            le=1.0,
            title="Transition revision probability",
            description=(
                "Share of persons in a current plan state who reconsider their "
                "plan at each iteration. Revising persons are redistributed over "
                "the filtered candidate states according to MNL probabilities."
            ),
        ),
    ]

    transition_logit_scale: Annotated[
        float,
        Field(
            default=1.0,
            ge=0.0,
            title="Transition logit scale",
            description=(
                "Scale applied to plan utilities in the day-to-day plan "
                "revision multinomial logit. Lower values flatten "
                "revision probabilities without changing the underlying "
                "plan utility formulation."
            ),
        ),
    ]

    transition_utility_pruning_delta: Annotated[
        float,
        Field(
            default=3.0,
            ge=0.0,
            title="Transition utility pruning delta",
            description=(
                "Keep only candidate plans whose scaled utility is within this "
                "delta of the best candidate utility for a given current plan "
                "state before computing transition probabilities. Lower values "
                "shrink the transition choice set and speed up the simulation."
            ),
        ),
    ]

    transition_distance_friction: Annotated[
        float,
        Field(
            default=0.5,
            ge=0.0,
            title="Transition distance friction",
            description=(
                "Penalty applied per unit of plan-embedding distance in the "
                "day-to-day plan revision model. Higher values reduce the "
                "probability of large jumps between daily programmes."
            ),
        ),
    ]

    plan_embedding_dimension_weights: Annotated[
        list[float] | None,
        Field(
            default=None,
            title="Plan embedding dimension weights",
            description=(
                "Optional per-dimension weights used when computing weighted "
                "Euclidean distances between plan embeddings. If omitted, "
                "state dimensions use a weight of 1.0 and spatial dimensions "
                "use the default PlanDistance spatial scaling."
            ),
        ),
    ]

    simulate_weekend: Annotated[
        bool,
        Field(
            default=False,
            title="Week day only or week day + weekend day mode",
            description="Wether to simulate a weekend day or only a week day.",
        ),
    ]

    behavior_change_phases: Annotated[
        list[BehaviorChangePhase] | None,
        Field(
            default=None,
            title="Behavior change phases",
            description=(
                "Optional per-iteration adaptation policy. Each phase starts at a "
                "given iteration and selects the highest state layer that may "
                "adapt: `mode_replanning`, `destination_replanning`, or "
                "`full_replanning`. Restricted phases apply to currently "
                "occupied non-stay-home states and freeze stay-home. If omitted, "
                "all iterations use `full_replanning`."
            ),
        ),
    ]

    @model_validator(mode="after")
    def validate_behavior_change_phases(self) -> "Parameters":
        """Ensure phase definitions are sorted and non-overlapping.

        Returns:
            The validated parameter object.

        Raises:
            ValueError: If behavior-change phases are not sorted by
                ``start_iteration`` or if two phases start on the same
                iteration.
        """
        if self.persist_iteration_artifacts is False and self.save_transition_events:
            raise ValueError(
                "Parameters.save_transition_events cannot be True when "
                "Parameters.persist_iteration_artifacts is False."
            )

        if self.behavior_change_phases is None:
            return self

        start_iterations = [phase.start_iteration for phase in self.behavior_change_phases]
        if start_iterations != sorted(start_iterations):
            raise ValueError("Parameters.behavior_change_phases must be sorted by start_iteration.")

        if len(start_iterations) != len(set(start_iterations)):
            raise ValueError("Parameters.behavior_change_phases cannot define the same start_iteration twice.")

        return self

    def get_behavior_change_scope(self, iteration: int) -> BehaviorChangeScope:
        """Return the active behavior-change scope for a given iteration.

        Args:
            iteration: Current simulation iteration (1-based).

        Returns:
            The active behavior-change scope. If no phase applies yet, returns
            ``BehaviorChangeScope.FULL_REPLANNING``.
        """
        if self.behavior_change_phases is None:
            return BehaviorChangeScope.FULL_REPLANNING

        active_phase = None
        for phase in self.behavior_change_phases:
            if phase.start_iteration > iteration:
                break
            active_phase = phase

        if active_phase is None:
            return BehaviorChangeScope.FULL_REPLANNING

        return active_phase.scope
