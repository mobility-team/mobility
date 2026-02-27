from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

def OSMCapacityParameters(mode, **kwargs):
    
    kwargs = {
        k: (
            v if isinstance(v, OSMEdgeCapacity)
            else OSMEdgeCapacity.model_validate(v)
        )
        for k, v in kwargs.items()
    }
    
    if mode == "car":
        return CarOSMCapacityParameters(**kwargs)
    elif mode == "walk":
        return WalkOSMCapacityParameters(**kwargs)
    elif mode == "bicycle":
        return BicycleOSMCapacityParameters(**kwargs)
    else:
        raise ValueError(f"Mode {mode} has no parameter dataclass.")

class OSMEdgeCapacity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capacity: Annotated[float, Field(default=1000.0, gt=0.0)]
    alpha: Annotated[float, Field(default=0.15, ge=0.0)]
    beta: Annotated[float, Field(default=4.0, ge=0.0)]

class BaseOSMCapacityParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    
    def get_highway_tags(self):
        return list(self.__class__.model_fields)


class CarOSMCapacityParameters(BaseOSMCapacityParameters):
    
    # List of OSM highway types to extract from OSM data to feed dodgr weight_streenet function
    # Capacities according to https://svn.vsp.tu-berlin.de/repos/public-svn/publications/vspwp/2011/11-10/2011-06-20_openstreetmap_for_traffic_simulation_sotm-eu.pdf
    # Typical values for the BPR volume decay function parameters, according to https://www.istiee.unict.it/sites/default/files/files/ET_2021_83_7.pdf
    
    # ferry ways are included in the default dodgr weighting profile, but we 
    # exclude them in mobility. We should handle car transport with a ferry in public transport.

    motorway: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=2000.0))]
    trunk: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    primary: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    secondary: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    tertiary: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))]
    unclassified: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))]
    residential: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))]
    living_street: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=300.0))]
    motorway_link: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    trunk_link: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    primary_link: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    secondary_link: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))]
    tertiary_link: Annotated[OSMEdgeCapacity, Field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))]


class WalkOSMCapacityParameters(BaseOSMCapacityParameters):
    
    # List of OSM highway types to extract from OSM data to feed dodgr weight_streenet function
    # Parameters not actually used used for now because we don't compute congestion for this mode :
    # Default capacity of 1000 pers/h .
    # Typical values for the BPR volume decay function parameters (same as car).

    # ferry ways are included in the default dodgr weighting profile, but we 
    # disable them here because they should be represented in the public transport graph
    
    trunk: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    primary: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    secondary: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    tertiary: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    unclassified: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    residential: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    track: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    cycleway: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    path: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    steps: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    living_street: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    bridleway: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    footway: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    pedestrian: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    trunk_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    primary_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    secondary_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    tertiary_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]


class BicycleOSMCapacityParameters(BaseOSMCapacityParameters):
    
    # List of OSM highway types to extract from OSM data to feed dodgr weight_streenet function
    # Parameters not actually used used for now because we don't compute congestion for this mode :
    # Default capacity of 1000 pers/h .
    # Typical values for the BPR volume decay function parameters (same as car).

    # track, path, steps ways are included in the default dodgr weighting profile, 
    # but we disable them here because they lead to large graphs, with ways that 
    # are not very likely to be selected because they match hinking trails and 
    # dirt roads in OSM

    # we also remove ferry ways which should be included in the public transport graph instead
         
    trunk: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    primary: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    secondary: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    tertiary: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    unclassified: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    residential: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    cycleway: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    living_street: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    bridleway: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    footway: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    pedestrian: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    trunk_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    primary_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    secondary_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
    tertiary_link: Annotated[OSMEdgeCapacity, Field(default_factory=OSMEdgeCapacity)]
