from dataclasses import dataclass

@dataclass(frozen=True)
class PopulationTripsParameters:
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

    def validate(self) -> None:
        assert self.n_iterations >= 1
        assert 0.0 < self.dest_prob_cutoff <= 1.0
        assert self.alpha >= 0.0
        assert self.k_mode_sequences >= 1
        assert self.n_iter_per_cost_update >= 0
        assert self.cost_uncertainty_sd > 0.0
        assert self.seed >= 0
