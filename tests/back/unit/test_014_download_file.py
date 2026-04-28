import pathlib
from importlib import import_module

import pytest
import requests

from mobility.runtime.io.download_file import download_file


download_file_module = import_module("mobility.runtime.io.download_file")


class _FakeProgress:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def add_task(self, *args, **kwargs):
        return 1

    def update(self, *args, **kwargs):
        return None


class _FakeResponse:
    def __init__(self, *, status_code=200, chunks=None, headers=None, raise_exc=None):
        self.status_code = status_code
        self._chunks = chunks or []
        self.headers = headers or {}
        self._raise_exc = raise_exc
        self.chunk_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc

    def iter_content(self, chunk_size=8192):
        self.chunk_sizes.append(chunk_size)
        yield from self._chunks


class _FakeSession:
    def __init__(self, response=None, request_exc=None):
        self._response = response
        self._request_exc = request_exc
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self._request_exc is not None:
            raise self._request_exc
        return self._response


def test_download_file_returns_without_writing_on_404(monkeypatch, tmp_path):
    session = _FakeSession(response=_FakeResponse(status_code=404))
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module.requests, "Session", lambda: session)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path)

    assert result == path
    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False


def test_download_file_returns_without_writing_on_401(monkeypatch, tmp_path):
    session = _FakeSession(response=_FakeResponse(status_code=401))
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module.requests, "Session", lambda: session)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path)

    assert result == path
    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False


def test_download_file_removes_partial_file_when_request_fails(monkeypatch, tmp_path):
    session = _FakeSession(
        response=_FakeResponse(
            status_code=200,
            chunks=[b"partial-data"],
            headers={"content-length": "12"},
            raise_exc=requests.exceptions.Timeout("boom"),
        )
    )
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module.requests, "Session", lambda: session)

    path = tmp_path / "file.txt"

    with pytest.raises(requests.exceptions.Timeout):
        download_file("https://example.com/file.txt", path, max_retries=0)

    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False


def test_download_file_deletes_preexisting_partial_file_before_retrying(monkeypatch, tmp_path):
    session = _FakeSession(request_exc=requests.exceptions.ConnectionError("boom"))
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module.requests, "Session", lambda: session)

    path = tmp_path / "file.txt"
    temp_path = pathlib.Path(str(path) + ".part")
    temp_path.write_bytes(b"stale")

    with pytest.raises(requests.exceptions.ConnectionError):
        download_file("https://example.com/file.txt", path, max_retries=0)

    assert temp_path.exists() is False


def test_download_file_can_warn_instead_of_raising_on_request_error(monkeypatch, tmp_path, caplog):
    session = _FakeSession(request_exc=requests.exceptions.ConnectionError("boom"))
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module.requests, "Session", lambda: session)

    path = tmp_path / "file.txt"

    with caplog.at_level("WARNING"):
        result = download_file(
            "https://example.com/file.txt",
            path,
            max_retries=0,
            raise_on_error=False,
        )

    assert result == path
    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False
    assert "Error during requests to https://example.com/file.txt after 1 attempts" in caplog.text


def test_download_file_uses_large_default_chunk_size(monkeypatch, tmp_path):
    response = _FakeResponse(
        status_code=200,
        chunks=[b"data"],
        headers={"content-length": "4"},
    )
    session = _FakeSession(response=response)
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module.requests, "Session", lambda: session)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path, max_retries=0)

    assert result == path
    assert path.read_bytes() == b"data"
    assert response.chunk_sizes == [1024 * 1024]
