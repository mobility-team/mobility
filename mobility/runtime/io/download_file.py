import logging
import os
import pathlib
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from rich.progress import Progress
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from mobility.runtime.io.http import request_url


def download_file(url, path, max_retries=3, timeout=(10, 120), raise_on_error=True):
    """
    Downloads a file to a given path. Creates the containing parent folder, if
    it does not exist. Handles retries and timeouts to avoid common download issues.

    Args:
        url (str): the URL of the file to download.
        path (str or pathlib.Path): the path to download the file to.
        max_retries (int): the maximum number of retries for failed requests.
        timeout (int or tuple): the timeout setting for requests (in seconds).
            A tuple is interpreted as ``(connect_timeout, read_timeout)``.
        raise_on_error (bool): whether to raise if the download still fails
            after retries. If False, logs a warning and returns the target path.

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

    retryer = Retrying(
        stop=stop_after_attempt(max_retries + 1),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=before_sleep_log(logging.root, logging.WARNING),
        reraise=True,
    )

    def _download_once() -> None:
        response = None
        try:
            response = request_url(
                url,
                max_retries=0,
                timeout=timeout,
                stream=True,
                allowed_status_codes={401, 404},
            )
            if response.status_code == 404:
                logging.error(f"Error 404: The resource at {url} was not found.")
                return
            if response.status_code == 401:
                logging.error(
                    f"Error 401: The resource at {url} could not be accessed (authorization error)."
                )
                return

            total_size = int(response.headers.get("content-length", 0))

            # Rich progress uses a live console display. Avoid it in CI and in
            # log mode because parallel downloads can already have a live display.
            feedback = os.environ.get("MOBILITY_FEEDBACK")
            feedback = feedback.lower() if feedback is not None else None
            progress_setting = os.environ.get("MOBILITY_PROGRESS")
            progress_setting = progress_setting.lower() if progress_setting is not None else None
            use_rich_progress = (
                feedback == "progress"
                or (feedback is None and progress_setting in {"auto", "rich"})
                or (
                    feedback is None
                    and progress_setting is None
                    and not os.environ.get("CI")
                    and sys.stderr.isatty()
                )
            )

            with open(temp_path, "wb") as file:
                if use_rich_progress:
                    with Progress() as progress:
                        task = progress.add_task("[green]Downloading...", total=total_size)
                        for data in response.iter_content(chunk_size=chunk_size):
                            file.write(data)
                            progress.update(task, advance=len(data))
                else:
                    for data in response.iter_content(chunk_size=chunk_size):
                        file.write(data)

            os.replace(temp_path, path)
            logging.info("Downloaded file to " + str(path))

        except requests.exceptions.RequestException:
            if temp_path.exists():
                temp_path.unlink()
            raise
        finally:
            if response is not None:
                response.close()

    try:
        retryer(_download_once)
    except requests.exceptions.RequestException as req_err:
        log_message = "Error during requests to %s after %s attempts: %s"
        if raise_on_error:
            logging.error(log_message, url, max_retries + 1, req_err)
            raise

        logging.warning(log_message, url, max_retries + 1, req_err)

    return path


def download_files(url_path_pairs, max_workers=4, max_retries=3, timeout=(10, 120), raise_on_error=True):
    """
    Download several files in parallel and return paths in input order.

    Args:
        url_path_pairs (list): list of ``(url, path)`` pairs.
        max_workers (int): maximum number of parallel downloads.
        max_retries (int): maximum retries for each download.
        timeout (int or tuple): the timeout setting for requests.
        raise_on_error (bool): whether to raise on failed downloads.

    Returns:
        list[pathlib.Path]: downloaded file paths, in input order.
    """
    url_path_pairs = list(url_path_pairs)
    paths = [None] * len(url_path_pairs)
    if len(url_path_pairs) == 0:
        return paths

    worker_count = min(max_workers, len(url_path_pairs))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                download_file,
                url,
                path,
                max_retries=max_retries,
                timeout=timeout,
                raise_on_error=raise_on_error,
            ): index
            for index, (url, path) in enumerate(url_path_pairs)
        }
        try:
            for future in as_completed(futures):
                paths[futures[future]] = future.result()
        except Exception:
            for pending_future in futures:
                pending_future.cancel()
            raise

    return paths


def clean_path(path):
    path = pathlib.Path(path)
    name = re.sub(r"\s", "_", path.name)
    name = re.sub(r"[^\w\-_.]", "", name)
    path = path.parent / name

    return path
