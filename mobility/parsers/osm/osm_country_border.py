import pathlib
import os
import subprocess
import logging

from mobility.file_asset import FileAsset
from mobility.parsers.osm.geofabrik_extract import GeofabrikExtract

class OSMCountryBorder(FileAsset):

    def __init__(self, geofabrik_extract: GeofabrikExtract):

        inputs = {"geofabrik_extract": geofabrik_extract}
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "osm" / "osm_border.geojson"
        super().__init__(inputs, cache_path)


    def get_cached_asset(self) -> pathlib.Path:

        logging.info("OSM border already prepared. Reusing the file : " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self):
        
        logging.info("Extracting country borders from Geofabrik extract : " + self.inputs["geofabrik_extract"].inputs["download_url"])

        folder_path = self.cache_path.parent

        command = [
            "osmium", "tags-filter", "-t", "--overwrite",
            "-o", str(folder_path / "tmp.osm.pbf"),
            str(self.geofabrik_extract.get()),
            "w/admin_level=2"
        ]

        subprocess.run(command)

        command = [
            "osmium", "tags-filter", "-t", "--overwrite",
            "-o", str(folder_path / "tmp2.osm.pbf"),
            str(folder_path / "tmp.osm.pbf"),
            "w/boundary=administrative"
        ]

        subprocess.run(command)
        
        command = [
            "osmium", "export",
            str(folder_path / "tmp2.osm.pbf"),
            "-o", str(self.cache_path)
        ]

        subprocess.run(command)

        return self.cache_path
