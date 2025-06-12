import pathlib
import os

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file

class GeofabrikExtract(FileAsset):

    def __init__(self, download_url):

        inputs = {"download_url": download_url}
        self.file = pathlib.Path(download_url).name
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "osm" / self.file
        
        super().__init__(inputs, cache_path)

    def get_cached_asset(self):
        return self.cache_path
    
    def create_and_get_asset(self):
        download_file(self.download_url, self.cache_path)
        return self.cache_path