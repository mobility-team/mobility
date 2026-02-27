import pytest
from mobility.parsers import ademe_base_carbone as mod


def test_missing_result_key_raises_key_error(monkeypatch: pytest.MonkeyPatch):
    class _Resp:
        def json(self):
            return {"results": [{"wrong_key": 1.23}]}

    monkeypatch.setattr(mod.requests, "get", lambda url, proxies=None: _Resp(), raising=True)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None, raising=True)

    with pytest.raises(KeyError):
        mod.get_emissions_factor("BADKEY")

