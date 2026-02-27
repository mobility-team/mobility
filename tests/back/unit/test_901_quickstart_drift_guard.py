from pathlib import Path


REQUIRED_SNIPPETS = [
    'mobility.TransportZones("fr-09122", radius=10.0)',
    "mobility.EMPMobilitySurvey()",
    "mobility.Population(",
    "mobility.PopulationTrips(",
    'population_trips.get()["weekday_flows"].collect()',
    'population_trips.evaluate("global_metrics")',
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_quickstart_user_and_ci_follow_same_critical_flow():
    repo_root = Path(__file__).resolve().parents[3]
    user_path = repo_root / "examples" / "quickstart-fr.py.py"
    ci_path = repo_root / "examples" / "quickstart-fr-ci.py"

    user_text = _read(user_path)
    ci_text = _read(ci_path)

    for snippet in REQUIRED_SNIPPETS:
        assert snippet in user_text
        assert snippet in ci_text
