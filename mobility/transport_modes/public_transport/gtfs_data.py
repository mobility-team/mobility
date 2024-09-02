import os
import pathlib
import logging
import zipfile
import hashlib

from mobility.asset import Asset
from mobility.parsers.download_file import download_file, clean_path

class GTFSData(Asset):
    
    def __init__(self, url: str):
        
        path = clean_path(url)
        name = hashlib.md5(url.encode('utf-8')).hexdigest() + "_" + path.name
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / name
        
        if cache_path.suffix != ".zip":
            cache_path = cache_path.with_suffix('.zip')
            
        inputs = {
            "url": url,
            "download_date": os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"]
        }
        
        print(inputs)

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self):
        
        file_ok = self.is_gtfs_file_ok(self.cache_path)
        
        return [self.cache_path, file_ok]
    
    def create_and_get_asset(self):
        
        download_file(
            self.url,
            self.cache_path
        )
        
        file_ok = self.is_gtfs_file_ok(self.cache_path)
        
        return [self.cache_path, file_ok]

    
    def is_gtfs_file_ok(self, path):
        
        file_ok = False
        
        if os.path.getsize(path) < 1024:
            
            logging.info("Downloaded file size is inferior to 1 ko, it will not be used by mobility.")
            
        else:
        
            try:
                with zipfile.ZipFile(path, 'r') as zip_ref:
                    zip_contents = zip_ref.namelist()              
                if "agency.txt" in zip_contents:
                    file_ok = True
            except:
                logging.info("Downloaded file is not a regular GTFS zip file, it will not be used by mobility.")
                
        return file_ok
    
    
        
        
        
