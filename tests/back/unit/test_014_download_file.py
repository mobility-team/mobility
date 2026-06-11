import pathlib
import time
from importlib import import_module

import pytest
import requests

from mobility.runtime.io.download_file import download_file, download_files


download_file_module = import_module("mobility.runtime.io.download_file")


class _FakeProgress:
    started_count = 0

    def __enter__(self):
        type(self).started_count += 1
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
        self.closed = False

    def close(self):
        self.closed = True

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc

    def iter_content(self, chunk_size=8192):
        self.chunk_sizes.append(chunk_size)
        yield from self._chunks
        if self._raise_exc is not None:
            raise self._raise_exc


def test_download_file_returns_without_writing_on_404(monkeypatch, tmp_path):
    response = _FakeResponse(status_code=404)
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module, "request_url", lambda *args, **kwargs: response)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path)

    assert result == path
    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False
    assert response.closed is True


def test_download_file_returns_without_writing_on_401(monkeypatch, tmp_path):
    response = _FakeResponse(status_code=401)
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module, "request_url", lambda *args, **kwargs: response)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path)

    assert result == path
    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False
    assert response.closed is True


def test_download_file_removes_partial_file_when_request_fails(monkeypatch, tmp_path):
    response = _FakeResponse(
        status_code=200,
        chunks=[b"partial-data"],
        headers={"content-length": "12"},
        raise_exc=requests.exceptions.Timeout("boom"),
    )
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module, "request_url", lambda *args, **kwargs: response)

    path = tmp_path / "file.txt"

    with pytest.raises(requests.exceptions.Timeout):
        download_file("https://example.com/file.txt", path, max_retries=0)

    assert path.exists() is False
    assert (tmp_path / "file.txt.part").exists() is False
    assert response.closed is True


def test_download_file_deletes_preexisting_partial_file_before_retrying(monkeypatch, tmp_path):
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(
        download_file_module,
        "request_url",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.exceptions.ConnectionError("boom")),
    )

    path = tmp_path / "file.txt"
    temp_path = pathlib.Path(str(path) + ".part")
    temp_path.write_bytes(b"stale")

    with pytest.raises(requests.exceptions.ConnectionError):
        download_file("https://example.com/file.txt", path, max_retries=0)

    assert temp_path.exists() is False


def test_download_file_can_warn_instead_of_raising_on_request_error(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(
        download_file_module,
        "request_url",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.exceptions.ConnectionError("boom")),
    )

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
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module, "request_url", lambda *args, **kwargs: response)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path, max_retries=0)

    assert result == path
    assert path.read_bytes() == b"data"
    assert response.chunk_sizes == [1024 * 1024]
    assert response.closed is True


def test_download_file_does_not_start_rich_progress_in_ci(monkeypatch, tmp_path):
    response = _FakeResponse(
        status_code=200,
        chunks=[b"data"],
        headers={"content-length": "4"},
    )
    _FakeProgress.started_count = 0
    monkeypatch.setenv("CI", "true")
    monkeypatch.delenv("MOBILITY_FEEDBACK", raising=False)
    monkeypatch.delenv("MOBILITY_PROGRESS", raising=False)
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module, "request_url", lambda *args, **kwargs: response)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path, max_retries=0)

    assert result == path
    assert path.read_bytes() == b"data"
    assert _FakeProgress.started_count == 0


def test_download_file_starts_rich_progress_when_feedback_asks_for_it(monkeypatch, tmp_path):
    response = _FakeResponse(
        status_code=200,
        chunks=[b"data"],
        headers={"content-length": "4"},
    )
    _FakeProgress.started_count = 0
    monkeypatch.setenv("MOBILITY_FEEDBACK", "progress")
    monkeypatch.setattr(download_file_module, "Progress", _FakeProgress)
    monkeypatch.setattr(download_file_module, "request_url", lambda *args, **kwargs: response)

    path = tmp_path / "file.txt"
    result = download_file("https://example.com/file.txt", path, max_retries=0)

    assert result == path
    assert path.read_bytes() == b"data"
    assert _FakeProgress.started_count == 1


def test_download_files_returns_paths_in_input_order(monkeypatch, tmp_path):
    paths = [tmp_path / "slow.txt", tmp_path / "fast.txt"]

    def fake_download_file(url, path, **kwargs):
        if url.endswith("slow"):
            time.sleep(0.05)
        return pathlib.Path(path)

    monkeypatch.setattr(download_file_module, "download_file", fake_download_file)

    result = download_files(
        [
            ("https://example.com/slow", paths[0]),
            ("https://example.com/fast", paths[1]),
        ],
        max_workers=2,
    )

    assert result == paths
