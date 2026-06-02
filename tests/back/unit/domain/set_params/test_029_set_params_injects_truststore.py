import os
import sys
import types

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
