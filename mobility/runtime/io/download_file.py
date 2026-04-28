import logging
import os
import pathlib
import re

import requests
from rich.progress import Progress
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def download_file(url, path, max_retries=5, timeout=(10, 120)):
    """
    Downloads a file to a given path. Creates the containing parent folder, if
    it does not exist. Handles retries and timeouts to avoid common download issues.

    Args:
        url (str): the URL of the file to download.
        path (str or pathlib.Path): the path to download the file to.
        max_retries (int): the maximum number of retries for failed requests.
        timeout (int or tuple): the timeout setting for requests (in seconds).
            A tuple is interpreted as ``(connect_timeout, read_timeout)``.

    Returns:
        pathlib.Path: The path where the file was downloaded.
    """
    chunk_size = 1024 * 1024

    path = clean_path(path)
    temp_path = path.parent / f"{path.name}.part"
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        logging.info("Reusing already downloaded file at : " + str(path) + ".")
        return path

    if temp_path.exists():
        temp_path.unlink()

    logging.info("Downloading " + url)

    verify = os.environ.get("MOBILITY_CERT_FILE", True)
    proxies = {
        "http": os.environ.get("HTTP_PROXY"),
        "https": os.environ.get("HTTPS_PROXY"),
    }

    retryer = Retrying(
        stop=stop_after_attempt(max_retries + 1),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
        reraise=True,
    )

    def _download_once() -> None:
        try:
            with requests.Session() as session:
                with session.get(
                    url,
                    stream=True,
                    proxies=proxies,
                    verify=verify,
                    timeout=timeout,
                ) as response:
                    if response.status_code == 404:
                        logging.error(f"Error 404: The resource at {url} was not found.")
                        return
                    if response.status_code == 401:
                        logging.error(
                            f"Error 401: The resource at {url} could not be accessed (authorization error)."
                        )
                        return

                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))

                    with Progress() as progress:
                        task = progress.add_task("[green]Downloading...", total=total_size)
                        with open(temp_path, "wb") as file:
                            for data in response.iter_content(chunk_size=chunk_size):
                                file.write(data)
                                progress.update(task, advance=len(data))

            os.replace(temp_path, path)
            logging.info("Downloaded file to " + str(path))

        except requests.exceptions.RequestException:
            if temp_path.exists():
                temp_path.unlink()
            raise

    try:
        retryer(_download_once)
    except requests.exceptions.RequestException as req_err:
        logging.error(
            "Error during requests to %s after %s attempts: %s",
            url,
            max_retries + 1,
            req_err,
        )
        raise

    return path


def clean_path(path):
    path = pathlib.Path(path)
    name = re.sub(r"\s", "_", path.name)
    name = re.sub(r"[^\w\-_.]", "", name)
    path = path.parent / name

    return path
