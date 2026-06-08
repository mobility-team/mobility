from __future__ import annotations

from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator


class BehaviorChangeScope(str, Enum):
    """Highest adaptation layer allowed during one behavior-change phase."""

    FULL_REPLANNING = "full_replanning"
    DESTINATION_REPLANNING = "destination_replanning"
    MODE_REPLANNING = "mode_replanning"
    NO_TRANSITIONS = "no_transitions"


class BehaviorChangePhase(BaseModel):
    """Behavior-change phase applied from ``start_iteration`` onward."""

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
                "`no_transitions` keeps each plan fixed and only refreshes its "
                "costs and utility. Stay-home is frozen in restricted phases."
            ),
        ),
    ]


class GroupDayTripsRunParameters(BaseModel):
    """Run length, cost refresh, and stochastic replication settings."""

    model_config = ConfigDict(extra="forbid")

    n_iterations: Annotated[
        int,
        Field(
            default=1,
            ge=1,
            title="Number of iterations",
            description=(
                "Number of simulation iterations used to compute the population "
                "trips."
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
                "The simulation updates travel costs every n_iter_per_cost_update "
                "iterations. Set it to zero to ignore congestion feedback."
            ),
        ),
    ]
    seed: Annotated[
        int,
        Field(
            default=0,
            ge=0,
            title="Simulation seed",
            description="Seed used to get reproducible stochastic simulation steps.",
        ),
    ]
    n_replications: Annotated[
        int,
        Field(
            default=1,
            ge=1,
            title="Number of stochastic replications",
            description="Number of repeated model runs for the same scenario.",
        ),
    ]
    seeds: Annotated[
        list[int] | None,
        Field(
            default=None,
            title="Replication seeds",
            description="Optional list of seeds, one per stochastic replication.",
        ),
    ]

    @model_validator(mode="after")
    def validate_replication_seeds(self) -> "GroupDayTripsRunParameters":
        """Validate single-run and multi-run seed settings."""
        if self.seeds is not None:
            if len(self.seeds) != self.n_replications:
                raise ValueError(
                    "GroupDayTripsRunParameters.seeds should contain one seed per replication."
                )
            if any(seed < 0 for seed in self.seeds):
                raise ValueError(
                    "GroupDayTripsRunParameters.seeds values should be greater than or equal to 0."
                )

        if self.seed != 0 and self.n_replications != 1:
            raise ValueError(
                "GroupDayTripsRunParameters.seed is the single-replication seed. "
                "Use GroupDayTripsRunParameters.seeds when n_replications is greater than 1."
            )

        return self

    def seed_for_replication(self, replication: int) -> int:
        """Return the seed used by one stochastic replication."""
        if replication < 0 or replication >= self.n_replications:
            raise ValueError("replication should be between 0 and n_replications - 1.")
        if self.seeds is not None:
            return self.seeds[replication]
        if self.n_replications == 1:
            return self.seed
        return replication

    def with_replication(self, replication: int) -> "GroupDayTripsRunParameters":
        """Return single-replication run parameters for one replication."""
        data = self.model_dump(mode="python")
        data.update(
            seed=self.seed_for_replication(replication),
            n_replications=1,
            seeds=None,
        )
        return self.__class__.model_validate(data)


class GroupDayTripsPeriodParameters(BaseModel):
    """Day-type and reporting-period settings."""

    model_config = ConfigDict(extra="forbid")

    simulate_weekend: Annotated[
        bool,
        Field(
            default=False,
            title="Weekday and weekend simulation",
            description="Whether to simulate a weekend day in addition to the weekday.",
        ),
    ]
    weekday_weight: Annotated[
        float,
        Field(
            default=5.0 / 7.0,
            gt=0.0,
            title="Weekday result weight",
            description="Weight used when weekday and weekend results are combined.",
        ),
    ]
    weekend_weight: Annotated[
        float,
        Field(
            default=2.0 / 7.0,
            gt=0.0,
            title="Weekend result weight",
            description="Weight used when weekday and weekend results are combined.",
        ),
    ]
    annual_weight: Annotated[
        float,
        Field(
            default=365.0,
            gt=0.0,
            title="Annual result weight",
            description="Number of days used to scale typical-day results to a year.",
        ),
    ]


class GroupDayTripsOutputParameters(BaseModel):
    """Settings for optional cached outputs from the iteration loop."""

    model_config = ConfigDict(extra="forbid")

    cache_iteration_events: Annotated[
        bool,
        Field(
            default=False,
            title="Cache iteration events",
            description=(
                "Whether to cache detailed transition events for each model "
                "iteration and include them in the final transitions output. "
                "Core iteration state is always cached."
            ),
        ),
    ]


class GroupDayTripsBehaviorChangeParameters(BaseModel):
    """Per-iteration behavior-change policy."""

    model_config = ConfigDict(extra="forbid")

    phases: Annotated[
        list[BehaviorChangePhase] | None,
        Field(
            default=None,
            title="Behavior change phases",
            description=(
                "Optional per-iteration adaptation policy. If omitted, all "
                "iterations use full replanning."
            ),
        ),
    ]

    @model_validator(mode="after")
    def validate_phases(self) -> "GroupDayTripsBehaviorChangeParameters":
        """Ensure phase definitions are sorted and non-overlapping."""
        if self.phases is None:
            return self

        start_iterations = [phase.start_iteration for phase in self.phases]
        if start_iterations != sorted(start_iterations):
            raise ValueError(
                "GroupDayTripsBehaviorChangeParameters.phases must be sorted by start_iteration."
            )
        if len(start_iterations) != len(set(start_iterations)):
            raise ValueError(
                "GroupDayTripsBehaviorChangeParameters.phases cannot define the same start_iteration twice."
            )
        return self

    def scope_at(self, iteration: int) -> BehaviorChangeScope:
        """Return the active behavior-change scope for one iteration."""
        if self.phases is None:
            return BehaviorChangeScope.FULL_REPLANNING

        active_phase = None
        for phase in self.phases:
            if phase.start_iteration > iteration:
                break
            active_phase = phase

        if active_phase is None:
            return BehaviorChangeScope.FULL_REPLANNING
        return active_phase.scope


class GroupDayTripsActivitySequenceParameters(BaseModel):
    """Settings used when sampling activity sequences."""

    model_config = ConfigDict(extra="forbid")

    k_activity_sequences: Annotated[
        int | None,
        Field(
            default=3,
            ge=1,
            title="Number of activity-sequence seeds",
            description=(
                "Maximum number of timed survey activity-sequence seeds sampled "
                "without replacement per demand group during full replanning."
            ),
        ),
    ]


class GroupDayTripsDestinationSequenceParameters(BaseModel):
    """Settings used when sampling destination sequences."""

    model_config = ConfigDict(extra="forbid")

    alpha: Annotated[
        float,
        Field(
            default=0.5,
            ge=0.0,
            title="Chain cost weighting",
            description="Weight of the chain cost used to penalize intermediate destination options.",
        ),
    ]
    k_destination_sequences: Annotated[
        int,
        Field(
            default=3,
            ge=1,
            title="Number of destination sequences",
            description="Number of destination-chain sequences generated per admitted activity sequence.",
        ),
    ]
    refresh_active_mode_alternatives: Annotated[
        bool,
        Field(
            default=False,
            title="Refresh active mode alternatives",
            description="Whether to append active destination chains to the iteration candidates.",
        ),
    ]
    dest_prob_cutoff: Annotated[
        float,
        Field(
            default=0.99,
            gt=0.0,
            le=1.0,
            title="Destination probability distribution cutoff",
            description="Cutoff used to prune less probable destinations.",
        ),
    ]
    cost_uncertainty_sd: Annotated[
        float,
        Field(
            default=1.0,
            gt=0.0,
            title="Standard deviation of travel cost estimates",
            description="Standard deviation used to spread destination opportunities around OD point costs.",
        ),
    ]


class GroupDayTripsModeSequenceParameters(BaseModel):
    """Settings used when searching mode sequences."""

    model_config = ConfigDict(extra="forbid")

    k_mode_sequences: Annotated[
        int,
        Field(
            default=3,
            ge=1,
            title="Number of mode combinations",
            description="Number of mode combinations considered for one destination sequence.",
        ),
    ]
    mode_sequence_search_parallel: Annotated[
        bool,
        Field(
            default=True,
            title="Parallel mode-sequence search",
            description="Set to False to debug or for small simulations.",
        ),
    ]
    use_rust_mode_sequence_search: Annotated[
        bool,
        Field(
            default=False,
            title="Use Rust mode-sequence search backend",
            description="Whether to use the in-process Rust backend for top-k mode sequence search.",
        ),
    ]


class GroupDayTripsPlanUpdateParameters(BaseModel):
    """Settings used when updating day-to-day plans."""

    model_config = ConfigDict(extra="forbid")

    n_warmup_iterations: Annotated[
        int,
        Field(
            default=1,
            ge=0,
            title="Candidate memory warm-up iterations",
            description="Number of first iterations where inactive candidate plans are not removed.",
        ),
    ]
    max_inactive_age: Annotated[
        int,
        Field(
            default=2,
            ge=0,
            title="Maximum inactive candidate-plan age",
            description="Number of iterations an inactive candidate plan can stay in memory.",
        ),
    ]
    min_activity_time_constant: Annotated[
        float,
        Field(
            default=1.0,
            ge=0.0,
            title="Minimum activity time coefficient",
            description="Coefficient used to compute the minimum useful duration of an activity.",
        ),
    ]
    update_plan_timings_from_modeled_travel_times: Annotated[
        bool,
        Field(
            default=False,
            title="Update plan timings from modeled travel times",
            description="Whether to adjust plan timings with modeled travel times at each iteration.",
        ),
    ]
    use_destination_shadow_prices: Annotated[
        bool,
        Field(
            default=False,
            title="Use destination shadow prices",
            description="Whether to use destination shadow prices for opportunity-capacity feedback.",
        ),
    ]
    transition_distance_threshold: Annotated[
        float,
        Field(
            default=float("inf"),
            ge=0.0,
            title="Transition distance threshold",
            description="Maximum plan-distance allowed when a person changes plan.",
        ),
    ]
    enable_transition_distance_model: Annotated[
        bool,
        Field(
            default=False,
            title="Enable transition distance model",
            description="Whether plan distance affects day-to-day plan changes.",
        ),
    ]
    transition_revision_probability: Annotated[
        float,
        Field(
            default=0.3,
            ge=0.0,
            le=1.0,
            title="Transition revision probability",
            description="Share of persons who reconsider their plan at each iteration.",
        ),
    ]
    transition_logit_scale: Annotated[
        float,
        Field(
            default=1.0,
            ge=0.0,
            title="Transition logit scale",
            description="Scale applied to plan utilities when choosing a new plan.",
        ),
    ]
    transition_utility_pruning_delta: Annotated[
        float,
        Field(
            default=3.0,
            ge=0.0,
            title="Transition utility pruning delta",
            description="Maximum utility gap used to keep candidate plans in the transition choice set.",
        ),
    ]
    min_transition_utility_gain: Annotated[
        float,
        Field(
            default=0.0,
            ge=0.0,
            title="Minimum transition utility gain",
            description="Minimum utility improvement needed before a person can change plan.",
        ),
    ]
    transition_distance_friction: Annotated[
        float,
        Field(
            default=0.5,
            ge=0.0,
            title="Transition distance friction",
            description="Penalty applied to larger plan changes when the distance model is enabled.",
        ),
    ]
    plan_embedding_dimension_weights: Annotated[
        list[float] | None,
        Field(
            default=None,
            title="Plan embedding dimension weights",
            description="Optional weights used when computing distances between plans.",
        ),
    ]
    plan_probability_pruning_retained_share: Annotated[
        float,
        Field(
            default=1.0,
            gt=0.0,
            le=1.0,
            title="Current-plan retained probability share",
            description="Share of transition probability kept as separate target plans.",
        ),
    ]
    plan_probability_pruning_min_iteration: Annotated[
        int,
        Field(
            default=2,
            ge=1,
            title="Current-plan probability pruning start iteration",
            description="First iteration where low-probability target plans may be merged.",
        ),
    ]


class GroupDayTripsParameters(BaseModel):
    """Root settings for the grouped day-trips model."""

    model_config = ConfigDict(extra="forbid")

    run: Annotated[
        GroupDayTripsRunParameters,
        Field(default_factory=GroupDayTripsRunParameters),
    ]
    periods: Annotated[
        GroupDayTripsPeriodParameters,
        Field(default_factory=GroupDayTripsPeriodParameters),
    ]
    outputs: Annotated[
        GroupDayTripsOutputParameters,
        Field(default_factory=GroupDayTripsOutputParameters),
    ]
    behavior_change: Annotated[
        GroupDayTripsBehaviorChangeParameters,
        Field(default_factory=GroupDayTripsBehaviorChangeParameters),
    ]
    activity_sequences: Annotated[
        GroupDayTripsActivitySequenceParameters,
        Field(default_factory=GroupDayTripsActivitySequenceParameters),
    ]
    destination_sequences: Annotated[
        GroupDayTripsDestinationSequenceParameters,
        Field(default_factory=GroupDayTripsDestinationSequenceParameters),
    ]
    mode_sequences: Annotated[
        GroupDayTripsModeSequenceParameters,
        Field(default_factory=GroupDayTripsModeSequenceParameters),
    ]
    plan_update: Annotated[
        GroupDayTripsPlanUpdateParameters,
        Field(default_factory=GroupDayTripsPlanUpdateParameters),
    ]

    def with_replication(self, replication: int) -> "GroupDayTripsParameters":
        """Return validated single-replication parameters for one replication."""
        data = self.model_dump(mode="python")
        data["run"] = self.run.with_replication(replication)
        return self.__class__.model_validate(data)
