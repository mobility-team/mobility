import pytest
from mobility.parsers import ademe_base_carbone as mod


def test_empty_results_raises_index_error(monkeypatch: pytest.MonkeyPatch):
    class _Resp:
        def json(self):
            return {"results": []}

    monkeypatch.setattr(mod.requests, "get", lambda url, proxies=None: _Resp(), raising=True)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None, raising=True)

    with pytest.raises(IndexError):
        mod.get_emissions_factor("MISSING")

