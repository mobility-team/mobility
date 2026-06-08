from pathlib import Path


DOCS_EXAMPLE_MARKER = "## Complete Example"

REQUIRED_SNIPPETS = [
    'mobility.TransportZones("fr-87085", radius=10.0)',
    "mobility.EMPMobilitySurvey()",
    "mobility.Population(",
    "mobility.PopulationGroupDayTrips(",
    'weekday_run = population_trips.run("weekday")',
    'weekday_run.get()["plan_steps"].collect()',
    'weekday_results = population_trips.results("weekday")',
    "weekday_results.metrics.trip_count(",
    'by_variable="mode"',
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_first_python_block_after_marker(text: str, marker: str) -> str:
    marker_index = text.index(marker)
    code_start = text.index("```python", marker_index) + len("```python")
    code_end = text.index("```", code_start)
    return text[code_start:code_end].strip()


def test_quickstart_user_and_ci_follow_same_critical_flow():
    repo_root = Path(__file__).resolve().parents[3]
    user_path = repo_root / "examples" / "quickstart-fr.py"
    ci_path = repo_root / "examples" / "quickstart-fr-ci.py"
    docs_path = repo_root / "docs" / "source" / "quickstart.md"

    user_text = _read(user_path)
    ci_text = _read(ci_path)
    docs_text = _read(docs_path)

    for snippet in REQUIRED_SNIPPETS:
        assert snippet in user_text
        assert snippet in ci_text
        assert snippet in docs_text


def test_user_quickstart_and_docs_complete_example_are_aligned():
    repo_root = Path(__file__).resolve().parents[3]
    user_path = repo_root / "examples" / "quickstart-fr.py"
    docs_path = repo_root / "docs" / "source" / "quickstart.md"

    user_text = _read(user_path).strip()
    docs_text = _read(docs_path)
    docs_example = _extract_first_python_block_after_marker(docs_text, DOCS_EXAMPLE_MARKER)

    assert docs_example == user_text
