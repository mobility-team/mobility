from pathlib import Path
import pytest
from mobility.parsers import ademe_base_carbone as mod


def test_builds_expected_query_url_and_throttles(monkeypatch: pytest.MonkeyPatch):
    observed = {}

    class _Resp:
        def json(self):
            return {"results": [{"Total_poste_non_décomposé": 12.34}]}

    def fake_get(url, proxies=None):
        observed["url"] = url
        observed["proxies"] = proxies
        return _Resp()

    def fake_sleep(seconds):
        observed["sleep"] = seconds

    monkeypatch.setattr(mod.requests, "get", fake_get, raising=True)
    monkeypatch.setattr(mod.time, "sleep", fake_sleep, raising=True)

    element_id = "ID123"
    result = mod.get_emissions_factor(element_id)

    expected_prefix = (
        "https://data.ademe.fr/data-fair/api/v1/datasets/base-carboner/"
        "lines?page=1&after=1&size=12&sort=&select=&highlight=&format=json&"
        "html=false&q_mode=simple&qs="
    )
    expected_query = "Identifiant_de_l'élément:ID123 AND Type_Ligne:Elément"
    assert observed["url"] == expected_prefix + expected_query
    assert observed["proxies"] is None or observed["proxies"] == {}
    assert observed["sleep"] == pytest.approx(0.1)
    assert result == pytest.approx(12.34)

