import pytest
from mobility.parsers import ademe_base_carbone as mod


def test_uses_proxies_and_returns_float(monkeypatch: pytest.MonkeyPatch):
    captured = {}

    class _Resp:
        def json(self):
            return {"results": [{"Total_poste_non_décomposé": 7.0}]}

    def fake_get(url, proxies=None):
        captured["url"] = url
        captured["proxies"] = proxies
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", fake_get, raising=True)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None, raising=True)

    proxies = {"http": "http://proxy:8080", "https": "http://proxy:8443"}
    value = mod.get_emissions_factor("ABC-42", proxies=proxies)

    assert isinstance(value, (float, int))
    assert value == pytest.approx(7.0)
    assert captured["proxies"] == proxies
    assert "ABC-42" in captured["url"]

