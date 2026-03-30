import os
import pathlib
import logging
import json
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Annotated

from importlib import resources
from pydantic import BaseModel, ConfigDict, Field, model_validator

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.parameter_profiles import ListParameterProfile
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.modes.core.defaults import (
    DEFAULT_LONG_RANGE_MOTORIZED_MAX_BEELINE_DISTANCE_KM,
)
from .gtfs.gtfs_router import GTFSRouter
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.modes.core.transport_mode import TransportMode
from mobility.transport.modes.core.modal_transfer import IntermodalTransfer

class PublicTransportGraph(FileAsset):
    """
    A class for managing public transport travel costs calculations using GTFS files, inheriting from the FileAsset class.

    This class is responsible for creating, caching, and retrieving public transport travel costs 
    based on specified transport zones and travel modes, using a R script managed by the gtfs_graph method.
    
    Uses GTFS files that have been prepared by TransportZones, but a list of additional GTFS files
    (representing a project for instance) can be provided.
    
    Args:
        transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
        parameters: PublicTransportRoutingParameters. Will use standard parameters by default
    """

    def __init__(
            self,
            transport_zones: TransportZones,
            parameters: "PublicTransportRoutingParameters | None" = None
    ):
        if parameters is None:
            parameters = PublicTransportRoutingParameters()
        
        gtfs_router = GTFSRouter(
            transport_zones,
            parameters.additional_gtfs_files,
            parameters.expected_agencies
        )

        inputs = {
            "transport_zones": transport_zones,
            "gtfs_router": gtfs_router,
            "parameters": parameters
        }

        file_name = "public_transport_graph/simplified/public-transport-graph"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Graph already prepared. Reusing the file : " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pd.DataFrame:
        
        self.gtfs_graph(
            self.inputs["transport_zones"],
            self.inputs["gtfs_router"],
            self.inputs["parameters"]
        )

        return self.cache_path

    
    def gtfs_graph(
            self,
            transport_zones: TransportZones,
            gtfs_router: GTFSRouter,
            parameters: "PublicTransportRoutingParameters"
        ) -> pd.DataFrame:
        """
        Calculates travel costs for public transport between transport zones using the R script prepare_public_transport_graph.R

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            gtfs_router : GTFSRouter object containing data about public transport routes and schedules.
            parameters: PublicTransportRoutingParameters

        """

        logging.info("Computing public transport travel costs...")
        
        script = RScriptRunner(resources.files('mobility.transport.modes.public_transport').joinpath('prepare_public_transport_graph.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(gtfs_router.get()),
                json.dumps(parameters.model_dump(mode="json")),
                str(self.cache_path)
            ]
        )

        return None
    

    
    def audit_gtfs(self):
        return self.gtfs_router.audit_gtfs()


class PublicTransportRoutingParameters(BaseModel):
    """
    Routing parameters for public transport.

    These parameters combine:
    - a coarse outer OD envelope through `max_beeline_distance`, in km
    - time-window and generalized-time constraints for the public transport leg

    `max_beeline_distance` is only used to prune obviously too-distant OD pairs
    before detailed multimodal routing. It does not replace the detailed public
    transport time constraints below.
    """

    model_config = ConfigDict(extra="forbid")

    start_time_min: Annotated[float, Field(default=6.5, ge=0.0, le=24.0)]
    start_time_max: Annotated[float, Field(default=8.0, ge=0.0, le=24.0)]
    max_traveltime: Annotated[float, Field(default=2.0, gt=0.0)]
    wait_time_coeff: Annotated[float, Field(default=2.0, gt=0.0)]
    transfer_time_coeff: Annotated[float, Field(default=2.0, gt=0.0)]
    no_show_perceived_prob: Annotated[float, Field(default=0.2, ge=0.0, le=1.0)]
    target_time: Annotated[float, Field(default=8.0, ge=0.0, le=24.0)]
    max_wait_time_at_destination: Annotated[float, Field(default=0.25, ge=0.0)]
    max_perceived_time: Annotated[float, Field(default=2.0, gt=0.0)]
    max_beeline_distance: Annotated[
        float,
        Field(default=DEFAULT_LONG_RANGE_MOTORIZED_MAX_BEELINE_DISTANCE_KM, gt=0.0),
    ]
    additional_gtfs_files: Annotated[list[str] | ListParameterProfile | None, Field(default=None)]
    expected_agencies: Annotated[list[str] | None, Field(default=None)]

    @model_validator(mode="after")
    def validate_time_window(self) -> "PublicTransportRoutingParameters":
        if self.start_time_max < self.start_time_min:
            raise ValueError("start_time_max should be greater than or equal to start_time_min.")
        return self
