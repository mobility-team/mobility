from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def test_901_ensure_quickstart_works():
    repo_root = Path(__file__).resolve().parents[3]
    module_path = repo_root / "examples" / "quickstart-fr-ci.py"

    spec = spec_from_file_location("quickstart_fr_ci", module_path)
    assert spec is not None
    assert spec.loader is not None

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    output = module.run_quickstart_ci()

    weekday_flows = output["weekday_flows"]
    global_metrics = output["global_metrics"]

    assert weekday_flows.height > 0
    assert global_metrics is not None
