import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import mobility
import pytest


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed"
    ],
    scope="session",
)
def test_project_cache_cleanup_after_real_group_day_trips_run(tmp_path):
    """Cleanup removes stale files after a real model script changed and reran."""
    package_folder = Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
    project_folder = Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
    script = tmp_path / "scripts" / "run_model.py"
    result_file = tmp_path / "run-result.txt"
    script.parent.mkdir()
    existing_files = {
        path.resolve()
        for path in project_folder.rglob("*")
        if path.is_file()
    }

    def write_model_script(sample_size):
        scenario_name = f"project_cache_cleanup_{sample_size}"
        script.write_text(
            f"""
import mobility

SAMPLE_SIZE = {sample_size}
SCENARIO_NAME = {scenario_name!r}
PACKAGE_FOLDER = {str(package_folder)!r}
PROJECT_FOLDER = {str(project_folder)!r}
RESULT_FILE = {str(result_file)!r}

mobility.set_params(
    package_data_folder_path=PACKAGE_FOLDER,
    project_data_folder_path=PROJECT_FOLDER,
    r_packages=False,
)

transport_zones = mobility.TransportZones("fr-87085", radius=10.0)
survey = mobility.EMPMobilitySurvey()
population = mobility.Population(transport_zones, sample_size=SAMPLE_SIZE)

population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=[mobility.CarMode(transport_zones)],
    activities=[
        mobility.HomeActivity(),
        mobility.WorkActivity(),
        mobility.OtherActivity(population=population),
    ],
    surveys=[survey],
    scenarios=mobility.Scenarios(
        [
            mobility.Scenario(name=SCENARIO_NAME),
        ]
    ),
    parameters=mobility.GroupDayTripsParameters(
        run=mobility.GroupDayTripsRunParameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            seed=0,
        ),
        outputs=mobility.GroupDayTripsOutputParameters(
            cache_iteration_events=True,
        ),
        periods=mobility.GroupDayTripsPeriodParameters(simulate_weekend=False),
        destination_sequences=mobility.GroupDayTripsDestinationSequenceParameters(
            dest_prob_cutoff=0.9,
            cost_uncertainty_sd=1.0,
        ),
        mode_sequences=mobility.GroupDayTripsModeSequenceParameters(
            k_mode_sequences=3,
            mode_sequence_search_parallel=False,
        ),
    ),
)

plan_steps = population_trips.run("weekday", scenario=SCENARIO_NAME).get()["plan_steps"].collect()
with open(RESULT_FILE, "w", encoding="utf-8") as file:
    file.write(str(plan_steps.height))
""",
            encoding="utf-8",
        )

    def run_model_script():
        subprocess.run(
            [sys.executable, str(script)],
            check=True,
        )
        assert int(result_file.read_text(encoding="utf-8")) > 0

    # Run a real model script, then change its source and rerun it. The first
    # run's registered cache files are now stale if the second run no longer
    # uses them.
    write_model_script(sample_size=5)
    run_model_script()
    write_model_script(sample_size=6)
    run_model_script()

    mobility.set_params(
        package_data_folder_path=package_folder,
        project_data_folder_path=project_folder,
        r_packages=False,
        track_project_cache=False,
    )
    registry_path = project_folder / ".mobility" / "cache.sqlite"
    with sqlite3.connect(registry_path) as con:
        latest_transition_event_paths = {
            Path(row[0])
            for row in con.execute(
                """
                SELECT a.path
                FROM ref_assets a
                JOIN project_ref_versions v ON a.version_id = v.version_id
                JOIN project_refs r ON v.ref_id = r.ref_id
                WHERE r.source_path = ? AND v.is_latest = 1
                    AND a.path LIKE ?
                """,
                (
                    str(script.resolve()),
                    f"%{os.sep}transition_events_%.parquet",
                ),
            )
        }
    assert latest_transition_event_paths

    # Older projects can contain Mobility-looking files that were never
    # registered, plus user files. These must stay out of unused-cache removal.
    legacy_cache_file = (
        project_folder / "group_day_trips" / "project-cache-cleanup-legacy.parquet"
    )
    legacy_cache_file.parent.mkdir(exist_ok=True)
    legacy_cache_file.write_text("old mobility cache", encoding="utf-8")
    user_report = project_folder / "reports" / "project-cache-cleanup-note.txt"
    user_report.parent.mkdir(exist_ok=True)
    user_report.write_text("hand-written output", encoding="utf-8")
    protected_input = project_folder / "manual_inputs" / "project-cache-survey.csv"
    protected_input.parent.mkdir(exist_ok=True)
    protected_input.write_text("do not remove", encoding="utf-8")

    cache = mobility.ProjectCache()
    protected_by_test = []
    archived_by_test = []
    for source in cache.cache_sources():
        if source["status"] in {"missing", "modified"}:
            source_name = source["source_path"] or source["name"]
            cache.archive_cache_source(source_name)
            archived_by_test.append(source_name)

    cache.protect("manual_inputs")
    protected_by_test.append(project_folder / "manual_inputs")

    unused_report = cache.unused_files_preview()
    for path in unused_report.files_to_delete:
        if path.resolve() in existing_files:
            cache.protect(path)
            protected_by_test.append(path)
    unused_report = cache.unused_files_preview()

    unregistered_report = cache.untracked_files_preview()
    expected_unregistered = {legacy_cache_file.resolve(), user_report.resolve()}
    for path in unregistered_report.files_to_delete:
        if path.resolve() not in expected_unregistered:
            cache.protect(path)
            protected_by_test.append(path)
    unregistered_report = cache.untracked_files_preview()

    assert unused_report.files_to_delete
    assert latest_transition_event_paths.isdisjoint(unused_report.files_to_delete)
    assert legacy_cache_file not in unused_report.files_to_delete
    assert user_report not in unused_report.files_to_delete
    assert "unused registered cache files" in str(unused_report)

    assert latest_transition_event_paths.isdisjoint(
        unregistered_report.files_to_delete
    )
    assert legacy_cache_file in unregistered_report.files_to_delete
    assert user_report in unregistered_report.files_to_delete
    assert protected_input not in unregistered_report.files_to_delete
    assert "files not registered by Mobility" in str(unregistered_report)
    assert "old Mobility cache files and user files" in str(unregistered_report)

    removed_unused_report = cache.remove_unused_files()

    assert removed_unused_report.deleted_files
    assert set(removed_unused_report.deleted_files).issubset(
        set(unused_report.files_to_delete)
    )
    assert not any(path.exists() for path in removed_unused_report.deleted_files)
    assert legacy_cache_file.exists()
    assert user_report.exists()
    assert protected_input.exists()

    removed_unregistered_report = cache.remove_untracked_files()

    assert legacy_cache_file in removed_unregistered_report.deleted_files
    assert user_report in removed_unregistered_report.deleted_files
    assert protected_input not in removed_unregistered_report.deleted_files
    assert not legacy_cache_file.exists()
    assert not user_report.exists()
    assert protected_input.exists()

    for path in protected_by_test:
        cache.unprotect(path)
    for source_name in archived_by_test:
        cache.restore_cache_source(source_name)
