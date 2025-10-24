from dataclasses import dataclass

@dataclass(frozen=True)
class PopulationTripsParameters:
    """
    Immutable container for parameters controlling population trip generation and update processes.
    
    Attributes
    ----------
    n_iterations : int
        Number of global iterations in the population trip simulation.
    alpha : float
        Learning or convergence rate controlling parameter update strength.
    k_mode_sequences : int
        Number of alternative mode sequences considered for each individual.
    dest_prob_cutoff : float
        Cumulative probability threshold for destination sampling (0–1].
    activity_utility_coeff : float
        Coefficient weighting the utility of performing an activity.
    stay_home_utility_coeff : float
        Coefficient weighting the utility of staying home.
    n_iter_per_cost_update : int
        Number of iterations between travel cost recomputations.
    cost_uncertainty_sd : float
        Standard deviation of random noise added to perceived travel cost.
    seed : int
        Random seed ensuring reproducibility.
    mode_sequence_search_parallel : bool
        Whether to compute mode sequence utilities in parallel.
    min_activity_time_constant : float
        Minimum feasible activity duration constant.
    saturation_fun_beta : float
        Shape parameter for the destination saturation function.
    saturation_fun_ref_level : float
        Reference level for saturation normalization.
    transition_cost : float
        Fixed cost of transition between the current programme of individuals and 
        the available programs, that will be added the costs of these programmes.
        If an individual is currently doing programme A with utility Ua, and can 
        choose for the next iteration between programme A and B with utility Ub,
        the perceived utllity of plan B will be Ub - transition_cost.
    
    Methods
    -------
    validate():
        Checks the validity and consistency of parameter values.
    """
    
    n_iterations: int = 1
    alpha: float = 0.01
    k_mode_sequences: int = 6
    dest_prob_cutoff: float = 0.99
    activity_utility_coeff: float = 2.0
    stay_home_utility_coeff: float = 1.0
    n_iter_per_cost_update: int = 3
    cost_uncertainty_sd: float = 1.0
    seed: int = 0
    mode_sequence_search_parallel: bool = True
    min_activity_time_constant: float = 1.0
    saturation_fun_beta: float = 1.5
    saturation_fun_ref_level: float = 4.0
    transition_cost: float = 5.0

    def validate(self) -> None:
        assert self.n_iterations >= 1
        assert 0.0 < self.dest_prob_cutoff <= 1.0
        assert self.alpha >= 0.0
        assert self.k_mode_sequences >= 1
        assert self.n_iter_per_cost_update >= 0
        assert self.cost_uncertainty_sd > 0.0
        assert self.seed >= 0
        assert self.min_activity_time_constant >= 0
        assert self.saturation_fun_beta > 0
        assert self.saturation_fun_ref_level > 0
