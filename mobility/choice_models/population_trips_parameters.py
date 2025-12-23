from dataclasses import dataclass, fields
from typing import List, Union

from mobility import Parameter, ParameterSet


Number = Union[int, float]    


@dataclass
class PopulationTripsParameters(ParameterSet):
    """To add a new parameter:
        - add name, type and default value here,
        - add the description in the __post_init__ method
        - if the parameter is linked to another one, maybe add a constraint
          in _validate_param_interdependency()
        """
    n_iterations: int = 4
    alpha: float = 0.01
    k_mode_sequences: int = 6
    dest_prob_cutoff: float = 0.99
    n_iter_per_cost_update: int = 3
    cost_uncertainty_sd: float = 1.0
    seed: int = 0
    seeds: List[int] = None
    mode_sequence_search_parallel: bool = True
    min_activity_time_constant: float = 1.0
    simulate_weekend: bool = False
    name: str = ""

    
    def __post_init__(self):
        self.parameters = {
    "param_n_iterations":         Parameter(name="Number of iterations", name_fr="Nombre d'itérations",
                                     value=self.n_iterations,
                                     description=("""Number of simulation iterations used to compute the population trips.
                                                     Increase this to get more diverse programmes and to allow congestion feedbacks to propagate"""),
                                     parameter_type=int, min_value=1, max_value=42, interval=1,
                                     source_default="experience", parameter_role="simulation_parameter"),
                                     
    "param_alpha" :               Parameter(name="Alpha", name_fr="Alpha", description="Alpha",
                                          value=self.alpha,
                                          parameter_type=float, min_value=0.0, max_value=1.0, interval=0.005,
                                          source_default="experience", parameter_role="simulation_parameter"),
    "param_k_mode_sequences" :    Parameter(name="Number k of sampled mode sequences",
                                          value=self.k_mode_sequences,
                                          name_fr="Nombre k de séquences échantillonnées",
                                          description="Increase this to get more mode sequence options for each destination sequence, decrease to speed up the simulation",
                                          parameter_type=int, min_value=1, interval=1,
                                          source_default="experience", parameter_role="simulation_parameter"),
    "param_dest_prob_cutoff" :    Parameter(name="TBD", name_fr="TBD",
                                          value=self.dest_prob_cutoff,
                                          description="Increase this to get more destinations possibilities, decrease to speed up the simulation",
                                          parameter_type=float, min_value=0.10, max_value=1.00, interval=0.05,
                                          source_default="experience", parameter_role="simulation_parameter"),
    "param_n_iter_per_cost_update" : Parameter(name="Number n of iteration between two cost updates",
                                             value=self.n_iter_per_cost_update,
                                             name_fr="Nombre n d'itérations entre deux mises à jours des coûts",
                                             description="""Every n iterations, the model will update the costs:
                                                         set to zero to ignore congestion in the simulation,
                                                         set to 1 to update congestion at each iteration,
                                                         set to a higher level to speed up the simulation""",
                                             parameter_type=int, min_value=0, max_value=99, interval=1,
                                             source_default="experience", parameter_role="simulation_parameter"),
    "param_cost_uncertainty_sd" : Parameter(name="TBD", name_fr="TBD",
                                          value=self.cost_uncertainty_sd,
                                          description="Increase this to make the location of opportunities more uncertain around the destination points",
                                          parameter_type=float, min_value=0.1, interval=0.1,
                                          source_default="experience", parameter_role="simulation_parameter"),
    "param_seed" :                Parameter(name="Seed", name_fr="Graine (seed)",
                                          value=self.seed,
                                          description="""Unique seed used for these trips.
                                                      Change this value to get new programmes and results for a given set of inputs,
                                                      keep this to a set value to get reproductible results.""",
                                          parameter_type=int, min_value=0,
                                          parameter_role="randomisation"),
    "param_seeds" :               Parameter(name="Set of seeds", name_fr="Ensemble de graines (seeds)",
                                          value=self.seeds,
                                          description="Set of seeds, used to improve resultats by minimising noise",
                                          parameter_type=list,
                                          parameter_role="randomisation"),
    "param_mode_sequence_search_parallel" : Parameter(name="TBD", name_fr="TBD",
                                                    value=self.mode_sequence_search_parallel,
                                                    description="Set to False to debug or for small simulations, otherwise set to True to speed up the simulation",
                                                    parameter_type=bool,
                                                    source_default="tests", parameter_role="simulation_time_parameter"),
    "param_min_activity_time_constant" : Parameter(name="TBD", name_fr="TBD", description="TBD",
                                                 value=self.min_activity_time_constant,
                                                 parameter_type=float, interval=0.1,
                                                 source_default="experience", parameter_role="simulation_parameter"),
    "param_simulate_weekend" :    Parameter(name="Simulate weekend", name_fr="Simulation du week-end",
                                          value=self.simulate_weekend,
                                          description="Simulate or not trips for the week-ends",
                                          parameter_type=bool,
                                          source_default="speed_optimisation", parameter_role="simulation_parameter"),
    "param_name": Parameter(name="Name", name_fr="Nom", description="Name of the run with those parameters",
                            value=self.name, parameter_type=str)}
 
    def to_dict(self):
        # Return a structured description of all parameters
        return {
            "n_iterations": self.n_iterations,
            "alpha": self.alpha,
            "k_mode_sequences": self.k_mode_sequences,
            "dest_prob_cutoff": self.dest_prob_cutoff,
            "n_iter_per_cost_update": self.n_iter_per_cost_update,
            "cost_uncertainty_sd": self.cost_uncertainty_sd,
            "seed": self.seed,
            "seeds": self.seeds,
            "mode_sequence_search_parallel": self.mode_sequence_search_parallel,
            "min_activity_time_constant": self.min_activity_time_constant,
            "simulate_weekend": self.simulate_weekend,
            "name": self.name,
            # Add parameters in structured form
            "parameters": {key: param.to_dict() for key, param in self.parameters.items()}
        }                                        
                                            
                                            
    def _validate_param_interdependency(self):
        if self.seeds is not None:
            for seed in self.seeds:
                if (not isinstance(seed, int) or seed < 0):
                    raise TypeError(f"Seed must be a positive int and not {seed}")
        if self.n_iter_per_cost_update > self.n_iterations:
            raise ValueError(f"n_iter_per_cost_update ({self.n_iter_per_cost_update}) should not be higher than the number of iterations ({self.n_iterations})")
         

    
