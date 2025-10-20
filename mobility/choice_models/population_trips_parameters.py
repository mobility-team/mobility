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
    min_activity_time_constant: float = 1.0
    pre_calibrate_on_survey: bool = False
    utility_of_stay_home_time: float = 1.0
    precalibration_n_iterations: int = 300
    plot_precalibration_fit: bool = False
    precalibration_loss_fun: str = "NLL"

    def validate(self) -> None:
        assert self.n_iterations >= 1
        assert 0.0 < self.dest_prob_cutoff <= 1.0
        assert self.alpha >= 0.0
        assert self.k_mode_sequences >= 1
        assert self.n_iter_per_cost_update >= 0
        assert self.cost_uncertainty_sd > 0.0
        assert self.seed >= 0
        assert self.min_activity_time_constant > 0
        assert self.utility_of_stay_home_time > 0.0
        assert self.precalibration_n_iterations > 1
        assert self.precalibration_loss_fun == "NLL" or self.precalibration_loss_fun == "SSI"
