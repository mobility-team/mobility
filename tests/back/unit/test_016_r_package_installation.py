import json

import mobility.config as config


def test_cpp_routing_cch_declares_minimum_r_universe_version(monkeypatch):
    captured = {}

    class FakeRScriptRunner:
        def __init__(self, script_path):
            captured["script_path"] = script_path

        def run(self, args):
            captured["args"] = args

    monkeypatch.setattr(config, "RScriptRunner", FakeRScriptRunner)
    monkeypatch.setattr(config.platform, "system", lambda: "Linux")

    config.install_r_packages(True, False, "auto")

    packages = json.loads(captured["args"][0])
    cpp_routing = next(
        package for package in packages if package.get("name") == "cppRoutingCCH"
    )
    assert cpp_routing == {
        "source": "r-universe",
        "universe": "mobility-team",
        "name": "cppRoutingCCH",
        "minimum_version": "3.3.0",
    }
