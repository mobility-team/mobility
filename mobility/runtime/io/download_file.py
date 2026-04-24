import pathlib
import requests
import os
import logging
import re
from rich.progress import Progress
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def download_file(url, path, max_retries=3, timeout=30):
    """
    Downloads a file to a given path. Creates the containing parent folder, if 
    it does not exist. Handles retries and timeouts to avoid common download issues.
    
    Args:
        url (str): the URL of the file to download.
        path (str or pathlib.Path): the path to download the file to.
        max_retries (int): the maximum number of retries for failed requests.
        timeout (int or tuple): the timeout setting for requests (in seconds).
    
    Returns:
        pathlib.Path: The path where the file was downloaded.
    """

    path = clean_path(path)
    temp_path = path.parent / (path.name + ".part")

    # Create the folder containing the file if not already existing
    if not path.parent.exists():
        os.makedirs(str(path.parent))
    
    # Download the file if not already existing
    if not path.exists():
        
        if temp_path.exists():
            os.remove(temp_path)
        
        logging.info("Downloading " + url)
         
        verify = os.environ.get("MOBILITY_CERT_FILE", True)
        proxies = {
            "http": os.environ.get("HTTP_PROXY"),
            "https": os.environ.get("HTTPS_PROXY")
        }
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=0.3
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        
        try:
            response = http.get(url, stream=True, proxies=proxies, verify=verify, timeout=timeout)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 404:
                logging.error(f"Error 404: The resource at {url} was not found.")
                return None
            if response.status_code == 401:
                logging.error(f"Error 401: The resource at {url} could not be accessed (authorization error).")
                return None
            logging.error(f"HTTP error occurred: {http_err}")
            raise
        except ConnectionError as conn_err:
            logging.error(f"Connection error occurred: {conn_err}")
            return None
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Error during requests to {url}: {req_err}")
            raise

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
        
    return path

def clean_path(path):
    path = pathlib.Path(path)
    name = re.sub(r"\s", "_", path.name)
    name = re.sub(r"[^\w\-_.]", "", name)
    path = path.parent / name
    
    return path