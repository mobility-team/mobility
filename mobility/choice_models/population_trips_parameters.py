from pydantic import BaseModel, Field, ConfigDict
from typing import Annotated


class PopulationTripsParameters(BaseModel):

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
