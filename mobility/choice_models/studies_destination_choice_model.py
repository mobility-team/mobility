import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import pathlib
import os
import polars as pl
import matplotlib.pyplot as plt


from importlib import resources
import mobility
from mobility.choice_models.utilities import Utilities
from mobility.parsers.students_distribution import StudentsDistribution
from mobility.parsers.schools_capacity_distribution import SchoolsCapacityDistribution


from mobility.r_utils.r_script import RScript

from mobility.radiation_model import radiation_model
from mobility.radiation_model_selection import apply_radiation_model

from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_zones import TransportZones

from dataclasses import dataclass, field
from typing import Dict, Union, List


@dataclass
class StudiesDestinationChoiceModelParameters:
    
    model: Dict[str, Union[str, float]] = field(
        default_factory=lambda: {
            "type": "radiation_selection",
            "lambda": 0.99986,
            #"end_of_contract_rate": 0.00, # à supprimer ?
            #"job_change_utility_constant": -5.0, # à supprimer ?
            #"max_iterations": 10,
            #"tolerance": 0.01,
            #"cost_update": False,
            #"n_iter_cost_update": 3
            }
        )
    
    utility: Dict[str, float] = field(
        default_factory=lambda: {
            "fr": 70.0,
            "ch": 50.0
            }
        )
    
    motive_ids: List[str] = field(
        default_factory=lambda: ["1.11"]
    )
    
    

class StudiesDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self,
            transport_zones: TransportZones,
            modes: List[TransportMode],
            parameters: StudiesDestinationChoiceModelParameters = StudiesDestinationChoiceModelParameters(),
            students_distribution, 
            school_capacities, 
            reference_flows: pd.DataFrame = None,
            ssi_min_flow_volume: float = 200.0
        ):
        """

        """
        
        
        if students_distribution is None:
            self.students_distribution = StudentsDistribution()
            self.school_capacities = SchoolsCapacityDistribution()
            self.reference_flows = SchoolStudentsFlows()
        else:
            self.students_distribution = students_distribution
            self.school_capacities = school_capacities
            self.reference_flows = reference_flows

        
        
        if "type" not in parameters.model.keys():
            raise ValueError("The model_parameters should be a dict that specifies the type of radiation model : radiation_universal or radiation_selection")
        
        if parameters.model["type"] == "radiation_selection":
            if "lambda" not in parameters.model.keys():
                raise ValueError("Lambda parameter missing in model_parameters. It should be a dict with keys fr and ch.")
            
        if parameters.model["type"] == "radiation_universal":
            if "alpha" not in parameters.model.keys():
                raise ValueError("Alpha parameter missing in model_parameters.")
            if "beta" not in parameters.model.keys():
                raise ValueError("Beta parameter missing in model_parameters.")
        
        super().__init__(
            "studies",
            transport_zones,
            modes,
            self.studies_sources_and_sinks,
            parameters,
            ssi_min_flow_volume
        )
        
        
    def prepare_sources_and_sinks(self, transport_zones: TransportZones):
        
        
        transport_zones = transport_zones.get()
        sources = self.prepare_sources(transport_zones)
        sinks = self.prepare_sinks(transport_zones)
        
        return sources, sinks
    
    def prepare_utilities(self, transport_zones, sinks):
        utilities = Utilities(transport_zones, sinks, self.inputs["parameters"].utility)
        return utilities

    
    def prepare_sources(
            self,
            transport_zones: pd.DataFrame
        ) -> pd.DataFrame:

        tz_lau_ids = set(transport_zones["local_admin_unit_id"].unique())        
        
        students_distribution = self.students_distribution.get()
        students_distribution = students_distribution[students_distribution["local_admin_unit_id"].isin(tz_lau_ids)]
        
        return students_distribution
        
    
    def prepare_sinks(
            self,
            transport_zones: pd.DataFrame
        ) -> pd.DataFrame:
        """
        """
        
        # missing swiss school capacities
        school_capacities = self.school_capacities.get()
        school_capacities = school_capacities[school_capacities["local_admin_unit_id"].isin(tz_lau_ids)]

                
        return all_shops
    
