import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def request_url(
    url,
    max_retries=3,
    timeout=(10, 120),
    stream=False,
    allowed_status_codes=None,
):
    """
    Call a URL with Mobility proxy, certificate, timeout, and retry settings.

    Args:
        url (str): the URL to call.
        max_retries (int): the maximum number of retries for failed requests.
        timeout (int or tuple): the timeout setting for requests.
        stream (bool): whether to stream the response body.
        allowed_status_codes (set): status codes returned without raising.

    Returns:
        requests.Response: the HTTP response. The caller should close it after
        streamed downloads.
    """
    allowed_status_codes = allowed_status_codes or set()
    verify = os.environ.get("MOBILITY_CERT_FILE", True)
    proxies = {
        "http": os.environ.get("HTTP_PROXY"),
        "https": os.environ.get("HTTPS_PROXY"),
    }

    retryer = Retrying(
        stop=stop_after_attempt(max_retries + 1),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=before_sleep_log(logging.root, logging.WARNING),
        reraise=True,
    )

    def _request_once():
        response = requests.get(
            url,
            stream=stream,
            proxies=proxies,
            verify=verify,
            timeout=timeout,
        )
        if response.status_code in allowed_status_codes:
            return response
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException:
            response.close()
            raise
        return response

    return retryer(_request_once)


def request_urls(
    urls,
    max_workers=8,
    max_retries=3,
    timeout=(10, 120),
    stream=False,
    allowed_status_codes=None,
    progress_description=None,
):
    """
    Call several URLs in parallel and return responses in input order.

    The first request error is raised. Any responses already received are
    closed before raising so failed parallel calls do not leak connections.
    """
    urls = list(urls)
    responses = [None] * len(urls)
    if len(urls) == 0:
        return responses

    request_error = None
    futures = {}
    worker_count = min(max_workers, len(urls))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                request_url,
                url,
                max_retries=max_retries,
                timeout=timeout,
                stream=stream,
                allowed_status_codes=allowed_status_codes,
            ): index
            for index, url in enumerate(urls)
        }
        try:
            if progress_description is None:
                for future in as_completed(futures):
                    responses[futures[future]] = future.result()
            else:
                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                    transient=False,
                ) as progress:
                    task = progress.add_task(progress_description, total=len(urls))
                    for future in as_completed(futures):
                        responses[futures[future]] = future.result()
                        progress.advance(task)
        except Exception as exc:
            request_error = exc
            for pending_future in futures:
                pending_future.cancel()

    if request_error is not None:
        for future, index in futures.items():
            if responses[index] is not None:
                continue
            if future.done() and not future.cancelled():
                try:
                    responses[index] = future.result()
                except Exception:
                    pass
        for response in responses:
            if response is not None:
                response.close()
        raise request_error

    return responses
