from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator


class BehaviorChangeScope(str, Enum):
    """Highest adaptation layer allowed during one behavior-change phase.

    Attributes:
        FULL_REPLANNING: Allow any currently modeled state layer to change:
            activity sequence, destination sequence, and mode sequence.
            Stay-home transitions remain available.
        DESTINATION_REPLANNING: Keep each currently occupied non-stay-home
            activity sequence fixed and resample destination sequences plus
            dependent mode sequences. Stay-home is frozen.
        MODE_REPLANNING: Keep each currently occupied non-stay-home activity and
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
                "`full_replanning` allows activity, destination, and mode "
                "sequences to change within the current model scope. "
                "`destination_replanning` keeps each currently occupied "
                "non-stay-home activity sequence fixed and resamples "
                "destination plus mode sequences. `mode_replanning` keeps each "
                "currently occupied non-stay-home activity and destination "
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

    k_mode_sequences: Annotated[
        int,
        Field(
            default=6,
            ge=1,
            title="Number of mode combinations",
            description=(
                "Number of mode combinations considered in the simulation, for "
                "a given destination sequence. Only the top k combinations are "
                "considered."
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
                "occupied non-stay-home states and freeze stay-home. In the "
                "current model, `full_replanning` means that activity, "
                "destination, and mode sequences may all change. If omitted, "
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
