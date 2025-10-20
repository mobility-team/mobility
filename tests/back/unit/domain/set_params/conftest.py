import builtins
import os
import sys
from pathlib import Path
import types
import pytest

# This suite tests mobility.set_params. We isolate environment and external processes.

@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """
    Keep environment clean between tests. Unset the env vars the module may touch.
    """
    for key in [
        "MOBILITY_ENV_PATH",
        "MOBILITY_CERT_FILE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "MOBILITY_DEBUG",
        "MOBILITY_PACKAGE_DATA_FOLDER",
        "MOBILITY_PROJECT_DATA_FOLDER",
    ]:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """
    Redirect Path.home() to a temp location so any default directories land under tmp_path.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pathlib.Path.home", lambda: fake_home, raising=True)
    return fake_home


@pytest.fixture
def resources_dir(tmp_path):
    """
    Provide a temp directory to stand in for importlib.resources.files(...) roots.
    """
    root = tmp_path / "resources"
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture(autouse=True)
def patch_importlib_resources_files(monkeypatch, resources_dir):
    """
    Patch importlib.resources.files to always return our temp resources directory
    for any package name (we don't need real distribution data for these tests).
    """
    from importlib import resources as _resources

    def _fake_files(_package_name):
        # Behaves adequately for .joinpath(...) calls.
        return resources_dir

    monkeypatch.setattr(_resources, "files", _fake_files, raising=True)


@pytest.fixture(autouse=True)
def patch_rscript(monkeypatch):
    """
    Provide a fake RScript class at mobility.r_utils.r_script.RScript
    that records the script path and args instead of running R.
    """
    # Ensure the module tree exists
    if "mobility" not in sys.modules:
        sys.modules["mobility"] = types.ModuleType("mobility")
    if "mobility.r_utils" not in sys.modules:
        sys.modules["mobility.r_utils"] = types.ModuleType("mobility.r_utils")
    if "mobility.r_utils.r_script" not in sys.modules:
        sys.modules["mobility.r_utils.r_script"] = types.ModuleType("mobility.r_utils.r_script")

    r_script_mod = sys.modules["mobility.r_utils.r_script"]

    class _FakeRScript:
        last_script_path = None
        last_args = None
        call_count = 0

        def __init__(self, script_path):
            _FakeRScript.last_script_path = Path(script_path)

        def run(self, args):
            _FakeRScript.last_args = list(args)
            _FakeRScript.call_count += 1
            return 0  # pretend success

    monkeypatch.setattr(r_script_mod, "RScript", _FakeRScript, raising=True)
    return sys.modules["mobility.r_utils.r_script"].RScript  # so tests can inspect .last_*


@pytest.fixture
def fake_input_yes(monkeypatch):
    """
    Patch builtins.input to return 'Yes' (case-insensitive check in code).
    """
    monkeypatch.setattr(builtins, "input", lambda *_: "Yes", raising=True)


@pytest.fixture
def fake_input_no(monkeypatch):
    """
    Patch builtins.input to return 'No' to trigger the negative branch.
    """
    monkeypatch.setattr(builtins, "input", lambda *_: "No", raising=True)
