import os
from mobility.set_params import set_env_variable

def test_set_env_variable_sets_when_value_present(monkeypatch):
    set_env_variable("MOBILITY_CERT_FILE", "/tmp/cert.pem")
    assert os.environ["MOBILITY_CERT_FILE"] == "/tmp/cert.pem"

def test_set_env_variable_skips_when_value_none(monkeypatch):
    # ensure absent
    monkeypatch.delenv("MOBILITY_CERT_FILE", raising=False)
    set_env_variable("MOBILITY_CERT_FILE", None)
    assert "MOBILITY_CERT_FILE" not in os.environ
