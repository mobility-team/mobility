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
from mobility.runtime.parameter_values import ParameterValue, SensitivityValue
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.modes.core.defaults import (
    DEFAULT_LONG_RANGE_MOTORIZED_MAX_BEELINE_DISTANCE_KM,
)
from .gtfs.gtfs_router import GTFSRouter
from .gtfs.gtfs_sources import GTFSSources
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
        parameters: PublicTransportRoutingParameters with an explicit GTFS reference date and sources folder.
    """

    def __init__(
            self,
            transport_zones: TransportZones,
            parameters: "PublicTransportRoutingParameters | None" = None
    ):
        if parameters is None:
            raise ValueError(
                "PublicTransportGraph requires PublicTransportRoutingParameters "
                "with an explicit `gtfs_reference_date` and `gtfs_sources_folder`."
            )
        
        gtfs_sources = GTFSSources(
            gtfs_reference_date=parameters.gtfs_reference_date,
            gtfs_sources_folder=parameters.gtfs_sources_folder,
            countries=self.get_countries(transport_zones),
            use_live_gtfs=parameters.use_live_gtfs,
            max_gtfs_file_age_days=parameters.max_gtfs_file_age_days,
            transport_zones=transport_zones,
        )

        gtfs_router = GTFSRouter(
            transport_zones=transport_zones,
            gtfs_sources=gtfs_sources,
            additional_gtfs_files=parameters.additional_gtfs_files,
            expected_agencies=parameters.expected_agencies,
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
        logging.debug("Graph already prepared. Reusing the file : " + str(self.cache_path))
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

    def get_countries(self, transport_zones: TransportZones) -> list[str]:
        """Return country codes from the actual transport zones."""
        countries = transport_zones.countries
        if not countries:
            raise ValueError("Transport zones should expose a country list.")
        return countries


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
    additional_gtfs_files: Annotated[
        ParameterValue | SensitivityValue | list[str] | None,
        Field(default=None),
    ]
    expected_agencies: Annotated[list[str] | None, Field(default=None)]
    gtfs_reference_date: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Date used to select GTFS source files, in YYYY-MM-DD format."
            ),
        ),
    ]
    max_gtfs_file_age_days: Annotated[
        int,
        Field(
            default=30,
            ge=0,
            description=(
                "Maximum accepted age, in days, of an archived GTFS file "
                "relative to gtfs_reference_date."
            ),
        ),
    ]
    gtfs_sources_folder: Annotated[
        pathlib.Path | None,
        Field(
            default=None,
            description=(
                "Project folder where Mobility stores the GTFS sources SQLite file."
            ),
        ),
    ]
    use_live_gtfs: Annotated[
        bool,
        Field(
            default=False,
            description=(
                "Allow Mobility to use live GTFS URLs. This can make two runs "
                "select different public transport schedules."
            ),
        ),
    ]

    @model_validator(mode="after")
    def validate_time_window(self) -> "PublicTransportRoutingParameters":
        if self.start_time_max < self.start_time_min:
            raise ValueError("start_time_max should be greater than or equal to start_time_min.")
        if self.additional_gtfs_files == []:
            self.additional_gtfs_files = None
        if self.gtfs_reference_date is None:
            raise ValueError(
                "Public transport routing requires `gtfs_reference_date` "
                "(YYYY-MM-DD) to select reproducible GTFS inputs."
            )
        GTFSSources.parse_reference_date(self.gtfs_reference_date)
        if self.gtfs_sources_folder is None:
            raise ValueError(
                "Public transport routing requires `gtfs_sources_folder`, for example "
                "`inputs/gtfs_sources`, so selected GTFS sources can be "
                "shared with other users."
            )
        return self
