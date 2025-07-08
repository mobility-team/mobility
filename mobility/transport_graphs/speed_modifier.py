import pathlib
import json
import geopandas as gpd

from mobility.in_memory_asset import InMemoryAsset
from mobility.transport_zones import TransportZones
from mobility.parsers.osm import GeofabrikRegions, GeofabrikExtract, OSMCountryBorder

class SpeedModifier(InMemoryAsset):
    def __init__(self, inputs):
        super().__init__(inputs)
    

class BorderCrossingSpeedModifier(SpeedModifier):
    
    def __init__(
            self,
            transport_zones: TransportZones,
            max_speed: float = 30.0,
            time_penalty: float = 0.0,
            geofabrik_extract_date: str = "240101"
        ):
        """
            Args:
                - max_speed (float): maximum speed of border crossing (km/h).
                - time_penalty (float): fixed time penalty of border crossing (min).
        """

        self.modifier_type = "border_crossing"
        self.inputs = {
            "transport_zones": transport_zones,
            "max_speed": max_speed,
            "time_penalty": time_penalty,
            "geofabrik_extract_date": geofabrik_extract_date
        }

        super().__init__(self.inputs)
 
    def get(self):

        transport_zones = self.inputs["transport_zones"]
        transport_zones.get()
        boundary = gpd.read_file(transport_zones.inputs["study_area"].cache_path["boundary"]).geometry[0]
        
        if boundary.is_valid is False:
            boundary = boundary.buffer(0.0)
            
        if boundary.is_valid is False:
            raise ValueError(
                """
                The boundary of the study area created by StudyArea is not 
                valid (and it could not be fixed with a 0.0 buffer).
                """
            )

        regions = GeofabrikRegions(extract_date=self.inputs["geofabrik_extract_date"]).get()
        regions = regions[regions.intersects(boundary)]

        borders = []
        has_borders = False

        for index, region in regions.iterrows():
            extract = GeofabrikExtract(region.url)
            border = OSMCountryBorder(extract)
            borders.append(str(border.get()))
            border_geometry = gpd.read_file(border.get())
            has_borders = bool(border_geometry.intersects(boundary).any())
            

        return {
            "modifier_type": self.modifier_type,
            "max_speed": self.inputs["max_speed"],
            "time_penalty": self.inputs["time_penalty"],
            "borders": borders,
            "has_borders": has_borders
        }

    
 
class LimitedSpeedZonesModifier(SpeedModifier):

    def __init__(
            self,
            zones_geometry_file_path: pathlib.Path | str,
            max_speed: float = 30.0   
        ):
        """
            Args:
                - zones_geometry_file_path (pathlib.Path | str): 
                    Path to a GIS file (geojson, gpkg, shp...) that defines the 
                    geometries of the zones in which the max speed should be set.
                - max_speed (float): maximum speed of border crossing (km/h).
        """

        self.modifier_type = "limited_speed_zones"
        self.inputs = {
            "max_speed": max_speed,
            "zones_geometry_file_path": zones_geometry_file_path
        }

        super().__init__(self.inputs)
    
    def get(self):

        return {
            "modifier_type": self.modifier_type,
            "max_speed": self.max_speed,
            "zones_geometry_file_path": self.zones_geometry_file_path
        }
    


class RoadLaneNumberModifier(SpeedModifier):

    def __init__(
            self,
            zones_geometry_file_path: pathlib.Path | str,
            lane_delta: int = 0.0  
        ):
        """
            Args:
                - zones_geometry_file_path (pathlib.Path | str): 
                    Path to a GIS file (geojson, gpkg, shp...) that defines the 
                    geometries of the zones in which the road capacity should be 
                    modified.
                - lane_delta (int): road lane number variation (the minimum 
                    number of lanes is set to one lane, so no road would be 
                    "closed" by a lane number modification).
        """

        self.modifier_type = "lane_number_modification"
        self.inputs = {
            "lane_delta": lane_delta,
            "zones_geometry_file_path": zones_geometry_file_path
        }

        super().__init__(self.inputs)
    
    def get(self):

        return {
            "modifier_type": self.modifier_type,
            "lane_delta": self.lane_delta,
            "zones_geometry_file_path": self.zones_geometry_file_path
        }
    



class NewRoadModifier(SpeedModifier):

    def __init__(
            self,
            zones_geometry_file_path: pathlib.Path | str,
            max_speed: float = 30.0,
            capacity: float = 1800.0,
            alpha: float = 0.15,
            beta: float = 4.0
        ):
        """
            Args:
                - zones_geometry_file_path (pathlib.Path | str): 
                    Path to a GIS file (geojson, gpkg, shp...) that defines the 
                    geometries of the zones in which the max speed should be set.
                - max_speed (float): max speed of the new road.
                - capacity (float): capacity (vehicles/h) of the new road.
                - alpha (float): 
                    Parameter alpha of the volume decay function of the new road.
                - beta (float): 
                    Parameter beta of the volume decay function of the new road.
        """

        self.modifier_type = "new_road"
        self.inputs = {
            "capacity": capacity,
            "alpha": alpha,
            "beta": beta,
            "max_speed": max_speed,
            "zones_geometry_file_path": zones_geometry_file_path
        }

        super().__init__(self.inputs)
    
    def get(self):

        return {
            "modifier_type": self.modifier_type,
            "capacity": self.capacity,
            "alpha": self.alpha,
            "beta": self.beta,
            "max_speed": self.max_speed,
            "zones_geometry_file_path": self.zones_geometry_file_path
        }