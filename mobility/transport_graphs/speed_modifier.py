import pathlib
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

        for index, region in regions.iterrows():
            extract = GeofabrikExtract(region.url)
            border = OSMCountryBorder(extract)
            borders.append(str(border.get()))

        return {
            "modifier_type": self.modifier_type,
            "max_speed": self.inputs["max_speed"],
            "time_penalty": self.inputs["time_penalty"],
            "borders": borders
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
    
    def get(self):

        return {
            "modifier_type": self.modifier_type,
            "max_speed": self.max_speed,
            "zones_geometry_file_path": self.zones_geometry_file_path
        }