import os
import pathlib
import logging
import json
from typing import Any

import pandas as pd

from importlib import resources

from mobility.transport.costs.travel_costs_asset import TravelCostsAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.modes.public_transport.public_transport_graph import PublicTransportRoutingParameters
from mobility.transport.modes.public_transport.intermodal_transport_graph import IntermodalTransportGraph
from mobility.transport.modes.core.modal_transfer import IntermodalTransfer

class PublicTransportTravelCosts(TravelCostsAsset):
    """
    A class for managing public transport travel costs calculations using GTFS files, inheriting from the FileAsset class.

    This class is responsible for creating, caching, and retrieving public transport travel costs 
    based on specified transport zones and travel modes.
    
    Uses GTFS files that have been prepared by TransportZones, but a list of additional GTFS files
    (representing a project for instance) can be provided.
    """

    def __init__(
            self,
            transport_zones: TransportZones,
            parameters: PublicTransportRoutingParameters,
            first_leg_travel_costs,
            last_leg_travel_costs,
            first_leg_mode_name: str,
            last_leg_mode_name: str,
            first_modal_transfer: IntermodalTransfer = None,
            last_modal_transfer: IntermodalTransfer = None
    ):
        """Build the PT travel-cost asset for one pair of access and egress legs."""
        if first_modal_transfer is None or last_modal_transfer is None:
            raise ValueError(
                "PublicTransportTravelCosts requires both `first_modal_transfer` and `last_modal_transfer`."
            )

        # Store the leg dependencies directly so PT variants can rebind them
        # without rebuilding whole leg-mode objects.
        self.first_leg_travel_costs = first_leg_travel_costs
        self.last_leg_travel_costs = last_leg_travel_costs
        self.first_leg_mode_name = first_leg_mode_name
        self.last_leg_mode_name = last_leg_mode_name

        intermodal_graph = IntermodalTransportGraph(
            transport_zones,
            parameters,
            first_leg_travel_costs,
            last_leg_travel_costs,
            first_leg_mode_name,
            last_leg_mode_name,
            first_modal_transfer,
            last_modal_transfer
        )

        inputs = {
            "intermodal_graph": intermodal_graph,
            "transport_zones": transport_zones,
            "first_modal_transfer": first_modal_transfer,
            "last_modal_transfer": last_modal_transfer,
            "parameters": parameters
        }

        file_name = (
            first_leg_mode_name
            + "_public_transport_"
            + last_leg_mode_name
            + "_travel_costs.parquet"
        )
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self, congestion: bool = False) -> pd.DataFrame:
        """Load the persisted PT OD costs."""
        
        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)

        return costs

    def create_and_get_asset(self, congestion: bool = False) -> pd.DataFrame:
        """Compute and persist the PT OD costs."""
        
        costs = self.compute_travel_costs(
            self.inputs["transport_zones"],
            self.inputs["intermodal_graph"],
            self.inputs["first_modal_transfer"],
            self.inputs["last_modal_transfer"],
            self.inputs["parameters"]
        )
        
        costs.to_parquet(self.cache_path)

        return costs

    
    def compute_travel_costs(
            self,
            transport_zones: TransportZones,
            intermodal_graph: IntermodalTransportGraph,
            first_modal_transfer: IntermodalTransfer,
            last_modal_transfer: IntermodalTransfer,
            parameters: PublicTransportRoutingParameters
        ) -> pd.DataFrame:
        """Run the PT routing pipeline and return the resulting OD costs."""

        logging.info("Computing public transport travel costs...")
        
        script = RScriptRunner(resources.files('mobility.transport.modes.public_transport').joinpath('compute_intermodal_public_transport_travel_costs.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(intermodal_graph.get()),
                json.dumps(first_modal_transfer.model_dump(mode="json")),
                json.dumps(last_modal_transfer.model_dump(mode="json")),
                json.dumps(parameters.model_dump(mode="json")),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)

        return costs
    
    
    def update(self, od_flows):
        """Refresh the PT asset after one of its dependencies changed."""
        
        self.inputs["intermodal_graph"].update()
        self.create_and_get_asset()

    def asset_for_congestion_state(self, congestion_state):
        """Return a PT variant rebound to congestion-aware leg travel costs."""
        first_leg_variant = self._travel_costs_for_congestion_state(
            self.first_leg_travel_costs,
            congestion_state,
        )
        last_leg_variant = self._travel_costs_for_congestion_state(
            self.last_leg_travel_costs,
            congestion_state,
        )

        if (
            first_leg_variant is self.first_leg_travel_costs
            and last_leg_variant is self.last_leg_travel_costs
        ):
            return self

        # PT owns its own parquet/intermodal graph cache, but not the caches of
        # the leg travel-cost assets it depends on.
        return PublicTransportTravelCosts(
            transport_zones=self.inputs["transport_zones"],
            parameters=self.inputs["parameters"],
            first_leg_travel_costs=first_leg_variant,
            last_leg_travel_costs=last_leg_variant,
            first_leg_mode_name=self.first_leg_mode_name,
            last_leg_mode_name=self.last_leg_mode_name,
            first_modal_transfer=self.inputs["first_modal_transfer"],
            last_modal_transfer=self.inputs["last_modal_transfer"],
        )

    def remove_congestion_artifacts(self, congestion_state) -> None:
        """Remove PT-owned congestion-specific variants without touching leg-owned caches."""
        variant = self.asset_for_congestion_state(congestion_state)
        if variant is self:
            return

        variant.remove()
        variant.inputs["intermodal_graph"].remove()
        
    def audit_gtfs(self):
        """Expose GTFS audit information from the intermodal graph."""
        return self.inputs["intermodal_graph"].audit_gtfs()

    @staticmethod
    def _travel_costs_for_congestion_state(travel_costs: Any, congestion_state):
        """Resolve one leg travel-cost asset for the requested congestion state."""
        if isinstance(travel_costs, TravelCostsAsset) is False:
            return travel_costs

        variant = travel_costs.asset_for_congestion_state(congestion_state)
        return travel_costs if variant is None else variant
