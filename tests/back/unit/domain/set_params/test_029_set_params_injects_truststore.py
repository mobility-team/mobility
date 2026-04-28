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
