import os
import sys
import types

import pytest

from mobility.config import set_params


def test_set_params_injects_truststore(monkeypatch, tmp_path):
    truststore_module = types.ModuleType("truststore")
    calls = {"count": 0}

    def _inject_into_ssl():
        calls["count"] += 1

    truststore_module.inject_into_ssl = _inject_into_ssl
    monkeypatch.setitem(sys.modules, "truststore", truststore_module)

    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        inject_into_ssl=True,
        r_packages=False,
    )

    assert calls["count"] == 1


def test_set_params_sets_r_runner_idle_monitor_settings(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        r_idle_timeout_seconds=120,
        r_idle_cpu_percent=0.25,
        r_idle_memory_change_mb=4.5,
        r_cpu_check_interval_seconds=7,
    )

    assert os.environ["MOBILITY_R_IDLE_TIMEOUT_SECONDS"] == "120"
    assert os.environ["MOBILITY_R_IDLE_CPU_PERCENT"] == "0.25"
    assert os.environ["MOBILITY_R_IDLE_MEMORY_CHANGE_MB"] == "4.5"
    assert os.environ["MOBILITY_R_CPU_CHECK_INTERVAL_SECONDS"] == "7"


def test_set_params_sets_feedback_logs_by_default_in_non_interactive_shell(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "logs"
    assert os.environ["MOBILITY_PROGRESS"] == "log"
    assert os.environ["MOBILITY_DEBUG"] == "0"


def test_set_params_sets_feedback_progress_when_requested(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        feedback="progress",
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "progress"
    assert os.environ["MOBILITY_PROGRESS"] == "rich"


def test_set_params_sets_feedback_logs_mode(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        feedback="logs",
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "logs"
    assert os.environ["MOBILITY_PROGRESS"] == "log"


def test_set_params_maps_old_progress_mode_to_feedback(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        progress="log",
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "logs"
    assert os.environ["MOBILITY_PROGRESS"] == "log"


def test_set_params_maps_old_debug_args_to_feedback_debug(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        debug=True,
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "debug"
    assert os.environ["MOBILITY_DEBUG"] == "1"


def test_set_params_maps_old_debug_logging_level_to_feedback_debug(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        logging_level="DEBUG",
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "debug"
    assert os.environ["MOBILITY_DEBUG"] == "1"


def test_set_params_maps_trace_logging_level_to_feedback_debug(tmp_path):
    set_params(
        package_data_folder_path=str(tmp_path / "pkg"),
        project_data_folder_path=str(tmp_path / "project"),
        r_packages=False,
        logging_level="TRACE",
    )

    assert os.environ["MOBILITY_FEEDBACK"] == "debug"
    assert os.environ["MOBILITY_DEBUG"] == "1"


def test_set_params_rejects_unknown_feedback_mode(tmp_path):
    with pytest.raises(ValueError, match="Unknown feedback setting"):
        set_params(
            package_data_folder_path=str(tmp_path / "pkg"),
            project_data_folder_path=str(tmp_path / "project"),
            r_packages=False,
            feedback="verbose",
        )


def test_set_params_rejects_unknown_progress_mode(tmp_path):
    with pytest.raises(ValueError, match="Unknown progress setting"):
        set_params(
            package_data_folder_path=str(tmp_path / "pkg"),
            project_data_folder_path=str(tmp_path / "project"),
            r_packages=False,
            progress="verbose",
        )
