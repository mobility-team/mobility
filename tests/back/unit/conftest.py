import os
from pathlib import Path
from uuid import uuid4

import pytest


@pytest.fixture
def project_dir(monkeypatch) -> Path:
    root = Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
    path = root / f"pytest-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(path))
    return path
