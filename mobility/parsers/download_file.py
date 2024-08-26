import pathlib
import requests
import os
import logging

from rich.progress import Progress

def download_file(url, path):
    """
        Downloads a file to a given path. Creates the containing parent folde, if 
        it does not exist. 
        
        Args:
            url (str): the url of the file tow download.
            path (str or pathlib.Path): the path to download the file to.
    
        Returns:
            None
    """
    
    path = pathlib.Path(path)
    temp_path = path.parent / (path.name + ".part")
    
    # Create the folder containing the file if not already existing
    if path.parent.exists() is False:
        os.makedirs(str(path.parent))
    
    # Download the file if not already existing
    if path.exists() is False:
        
        if temp_path.exists() is True:
            os.remove(temp_path)
        
        logging.info("Downloading " + url)
         
        verify = os.environ["MOBILITY_CERT_FILE"] if "MOBILITY_CERT_FILE" in os.environ.keys() else True
        
        proxies = {}
        if "HTTP_PROXY" in os.environ.keys():
            proxies["http_proxy"] = os.environ["HTTP_PROXY"]
        if "HTTPS_PROXY" in os.environ.keys():
            proxies["https_proxy"] = os.environ["HTTPS_PROXY"]
        
        response = requests.get(
            url=url,
            stream=True,
            proxies=proxies,
            verify=verify
        )
        
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))

        with Progress() as progress:
            
            task = progress.add_task("[green]Downloading...", total=total_size)
    
            with open(temp_path, "wb") as file:
                for data in response.iter_content(chunk_size=8192):
                    file.write(data)
                    progress.update(task, advance=len(data))
                    
            os.rename(temp_path, path)
            
        logging.info("Downloaded file to " + str(path))
        
    else:
    
        logging.info("Reusing already downloaded file at : " + str(path) + ".")
