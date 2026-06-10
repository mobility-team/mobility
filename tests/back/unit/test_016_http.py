import time
from importlib import import_module

import pytest
import requests

from mobility.runtime.io.http import request_url, request_urls


http_module = import_module("mobility.runtime.io.http")


class _FakeResponse:
    def __init__(self, status_code=200, raise_exc=None):
        self.status_code = status_code
        self._raise_exc = raise_exc
        self.closed = False

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc

    def close(self):
        self.closed = True


def test_request_url_allows_declared_status_codes(monkeypatch):
    response = _FakeResponse(
        status_code=404,
        raise_exc=requests.exceptions.HTTPError("missing"),
    )
    monkeypatch.setattr(http_module.requests, "get", lambda *args, **kwargs: response)

    result = request_url(
        "https://example.com/missing",
        max_retries=0,
        allowed_status_codes={404},
    )

    assert result is response
    assert response.closed is False


def test_request_url_closes_response_when_status_raises(monkeypatch):
    response = _FakeResponse(
        status_code=500,
        raise_exc=requests.exceptions.HTTPError("server error"),
    )
    monkeypatch.setattr(http_module.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(requests.exceptions.HTTPError):
        request_url("https://example.com/error", max_retries=0)

    assert response.closed is True


def test_request_urls_returns_responses_in_input_order(monkeypatch):
    def fake_request_url(url, **kwargs):
        if url.endswith("slow"):
            time.sleep(0.05)
        return _FakeResponse(status_code=200)

    monkeypatch.setattr(http_module, "request_url", fake_request_url)

    responses = request_urls(
        [
            "https://example.com/slow",
            "https://example.com/fast",
        ],
        max_workers=2,
    )

    assert len(responses) == 2
    assert [response.status_code for response in responses] == [200, 200]


def test_request_urls_closes_completed_responses_when_one_request_fails(monkeypatch):
    first_response = _FakeResponse(status_code=200)

    def fake_request_url(url, **kwargs):
        if url.endswith("bad"):
            time.sleep(0.05)
            raise requests.exceptions.ConnectionError("boom")
        return first_response

    monkeypatch.setattr(http_module, "request_url", fake_request_url)

    with pytest.raises(requests.exceptions.ConnectionError):
        request_urls(
            [
                "https://example.com/good",
                "https://example.com/bad",
            ],
            max_workers=2,
        )

    assert first_response.closed is True
