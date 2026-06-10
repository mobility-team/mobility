import os
import pathlib
import logging
import zipfile
import hashlib

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import clean_path, download_file, download_files

class GTFSData(FileAsset):
    """
    Simple FileAsset to store GTFS files.
    
    Checks that the GTFS zip is >1ko and contains an agency.txt file.
    """
    
    def __init__(
        self,
        provider: str,
        dataset_id: str,
        resource_id: str,
        download_url: str,
        gtfs_file_date: str | None,
        source_status: str,
        sources_created_at_utc: str,
    ):
        
        path = clean_path(download_url)
        live_sources_created_at_utc = None
        if source_status == "live":
            live_sources_created_at_utc = sources_created_at_utc

        source_key = "|".join(
            [
                provider,
                dataset_id,
                resource_id,
                download_url,
                str(gtfs_file_date),
                source_status,
                str(live_sources_created_at_utc),
            ]
        )
        name = hashlib.md5(source_key.encode('utf-8')).hexdigest() + "_" + path.name
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / name
        
        if cache_path.suffix != ".zip":
            cache_path = cache_path.with_suffix('.zip')
            
        self.name = name 
            
        inputs = {
            "provider": provider,
            "dataset_id": dataset_id,
            "resource_id": resource_id,
            "download_url": download_url,
            "gtfs_file_date": gtfs_file_date,
            "source_status": source_status,
            "live_sources_created_at_utc": live_sources_created_at_utc,
        }

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self):
        
        file_ok = self.is_gtfs_file_ok(self.cache_path)
        
        return [self.cache_path, file_ok]
    
    def create_and_get_asset(self):
        
        download_file(
            self.download_url,
            self.cache_path,
            raise_on_error=False
        )
        
        file_ok = self.is_gtfs_file_ok(self.cache_path)
        
        return [self.cache_path, file_ok]

    @classmethod
    def download_gtfs_files(cls, sources: list[dict]) -> list[list]:
        """Download selected GTFS sources in parallel and validate each file."""
        gtfs_files = [
            cls(
                provider=source["provider"],
                dataset_id=source["dataset_id"],
                resource_id=source["resource_id"],
                download_url=source["download_url"],
                gtfs_file_date=source["gtfs_file_date"],
                source_status=source["status"],
                sources_created_at_utc=source["sources_created_at_utc"],
            )
            for source in sources
        ]

        files_to_download = [
            (gtfs_file.download_url, gtfs_file.cache_path)
            for gtfs_file in gtfs_files
            if gtfs_file.is_update_needed()
        ]
        download_files(files_to_download, raise_on_error=False)

        return [
            [gtfs_file.cache_path, gtfs_file.is_gtfs_file_ok(gtfs_file.cache_path)]
            for gtfs_file in gtfs_files
        ]

    
    def is_gtfs_file_ok(self, path):
        
        if os.path.exists(path) is False:
            return False
        
        if os.path.getsize(path) < 1024:
            logging.info("Downloaded file size is inferior to 1 ko, it will not be used by mobility.")
            return False
        
        if "e8f2aceaaaa2493f6041dc7f0251f325-5d7ae44c16ad373ca1afbc4590f53256_gtfs-2015-chamonix-mobilit" in path.name:
            logging.info("Manual exception, GTFS not used from path", path)
            return False
        
        if "f5bcfc06b3dcbecec3f57857349e1036_20-gtfs-urbain-vitre-ete-2025" in path.name:
            logging.info("Manual exception for old Vitré GTFS, GTFS not used from path", path)
            return False
        
        try:
            with zipfile.ZipFile(path, 'r') as zip_ref:
                zip_contents = zip_ref.namelist()
            has_an_agency = "agency.txt" in zip_contents
            if has_an_agency:
                logging.info("Downloaded file is a proper GTFS zip which contains an agency file.")
            else:
                logging.info("Downloaded file is a proper GTFS zip but does not contain an agency file, it will not be used by Mobility.")
            return has_an_agency
        except:
            logging.info("Downloaded file is not a regular GTFS zip file, it will not be used by Mobility.")
            return False
    
    
    @staticmethod
    def get_agencies_names(path):
                
        with zipfile.ZipFile(path, 'r') as gtfs_folder:
            with gtfs_folder.open("agency.txt") as agency:
                agencies = agency.read().decode('utf-8')
                logging.info(agencies)
                return agencies


        
    
    
        
        
        
