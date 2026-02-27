from dataclasses import dataclass, field

def OSMCapacityParameters(mode, **kwargs):
    
    kwargs = {k: OSMEdgeCapacity(**v) for k,v in kwargs.items()}
    
    if mode == "car":
        return CarOSMCapacityParameters(**kwargs)
    elif mode == "walk":
        return WalkOSMCapacityParameters(**kwargs)
    elif mode == "bicycle":
        return BicycleOSMCapacityParameters(**kwargs)
    else:
        raise ValueError(f"Mode {mode} has no parameter dataclass.")

@dataclass
class OSMEdgeCapacity:
    capacity: float = 1000.0
    alpha: float = 0.15
    beta: float = 4.0


@dataclass
class BaseOSMCapacityParameters:
    
    def get_highway_tags(self):
        return list(self.__annotations__)


@dataclass
class CarOSMCapacityParameters(BaseOSMCapacityParameters):
    
    # List of OSM highway types to extract from OSM data to feed dodgr weight_streenet function
    # Capacities according to https://svn.vsp.tu-berlin.de/repos/public-svn/publications/vspwp/2011/11-10/2011-06-20_openstreetmap_for_traffic_simulation_sotm-eu.pdf
    # Typical values for the BPR volume decay function parameters, according to https://www.istiee.unict.it/sites/default/files/files/ET_2021_83_7.pdf
    
    # ferry ways are included in the default dodgr weighting profile, but we 
    # exclude them in mobility. We should handle car transport with a ferry in public transport.

    motorway: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=2000.0))
    trunk: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    primary: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    secondary: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    tertiary: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))
    unclassified: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))
    residential: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))
    living_street: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=300.0))
    motorway_link: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    trunk_link: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    primary_link: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    secondary_link: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=1000.0))
    tertiary_link: OSMEdgeCapacity = field(default_factory=lambda: OSMEdgeCapacity(capacity=600.0))


@dataclass
class WalkOSMCapacityParameters(BaseOSMCapacityParameters):
    
    # List of OSM highway types to extract from OSM data to feed dodgr weight_streenet function
    # Parameters not actually used used for now because we don't compute congestion for this mode :
    # Default capacity of 1000 pers/h .
    # Typical values for the BPR volume decay function parameters (same as car).

    # ferry ways are included in the default dodgr weighting profile, but we 
    # disable them here because they should be represented in the public transport graph
    
    trunk: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    primary: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    secondary: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    tertiary: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    unclassified: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    residential: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    track: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    cycleway: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    path: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    steps: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    living_street: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    bridleway: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    footway: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    pedestrian: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    trunk_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    primary_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    secondary_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    tertiary_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)


@dataclass
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
         
    trunk: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    primary: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    secondary: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    tertiary: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    unclassified: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    residential: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    cycleway: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    living_street: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    bridleway: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    footway: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    pedestrian: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    trunk_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    primary_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    secondary_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)
    tertiary_link: OSMEdgeCapacity = field(default_factory=OSMEdgeCapacity)