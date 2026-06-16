import pandas as pd
import pytest

from mobility.activities.leisure.leisure import LeisureActivity
from mobility.activities.shopping.shopping_opportunities import ShoppingOpportunities
from mobility.activities.studies.study_flows import StudyFlows
from mobility.activities.studies.study_opportunities import StudyOpportunities
from mobility.activities.work.countries.switzerland import SwissWorkFlows
from mobility.activities.work.work_flows import WorkFlows
from mobility.activities.work.work_opportunities import WorkOpportunities
from mobility.activities.work.work import WorkActivity


class FakeTransportZones:
    countries = ["zz"]
    study_area = "study-area"

    def get(self):
        return pd.DataFrame(
            {
                "transport_zone_id": [1],
                "local_admin_unit_id": ["zz-1"],
                "country": ["zz"],
                "weight": [1.0],
                "geometry": [None],
            }
        )


def test_001_work_opportunities_fail_for_unsupported_country(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    with pytest.raises(ValueError, match="Work opportunities are not available"):
        WorkOpportunities(countries=["zz"]).create_and_get_asset()


def test_001_shopping_opportunities_fail_for_unsupported_country(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    with pytest.raises(ValueError, match="Shopping opportunities are not available"):
        ShoppingOpportunities(countries=["zz"]).create_and_get_asset()


def test_001_study_opportunities_fail_for_unsupported_country(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    with pytest.raises(ValueError, match="Study opportunities are not available"):
        StudyOpportunities(countries=["zz"]).create_and_get_asset()


def test_001_work_activity_uses_transport_zone_countries_and_admin_units(monkeypatch):
    calls = []

    class FakeWorkOpportunities:
        def __init__(self, countries=None, local_admin_unit_ids=None):
            calls.append((countries, local_admin_unit_ids))

        def get(self):
            jobs = pd.DataFrame(
                {"n_jobs_total": [10.0]},
                index=pd.Index(["zz-1"], name="local_admin_unit_id"),
            )
            return jobs, pd.DataFrame()

    monkeypatch.setattr(
        "mobility.activities.work.work.WorkOpportunities",
        FakeWorkOpportunities,
    )

    opportunities = WorkActivity().get_opportunities(FakeTransportZones())

    assert calls == [(["zz"], ["zz-1"])]
    assert opportunities.to_pandas().to_dict("records") == [{"to": 1, "n_opp": 10.0}]


def test_001_leisure_activity_uses_the_transport_zone_study_area(monkeypatch):
    calls = []

    class FakeLeisureFacilitiesDistribution:
        def __init__(self, study_area):
            calls.append(study_area)

        def get(self):
            return pd.DataFrame(
                {
                    "freq_score": [2.0],
                    "geometry": [None],
                }
            )

    monkeypatch.setattr(
        "mobility.activities.leisure.leisure.LeisureFacilitiesDistribution",
        FakeLeisureFacilitiesDistribution,
    )
    monkeypatch.setattr(
        "mobility.activities.leisure.leisure.gpd.sjoin",
        lambda left, right, how, predicate: pd.DataFrame(
            {
                "freq_score": [2.0],
                "transport_zone_id": [1],
                "local_admin_unit_id": ["zz-1"],
                "country": ["zz"],
                "weight": [1.0],
                "index_right": [0],
            }
        ),
    )

    opportunities = LeisureActivity().get_opportunities(FakeTransportZones())

    assert calls == ["study-area"]
    assert opportunities.to_pandas().to_dict("records") == [{"to": 1, "n_opp": 2.0}]


class FakeCountryFlows:
    def __init__(self, flows):
        self.flows = flows
        self.selected_local_admin_unit_ids = None

    def filter_by_local_admin_unit_id(self, local_admin_unit_ids):
        self.selected_local_admin_unit_ids = local_admin_unit_ids
        return self.flows


class FakeCountryData:
    def __init__(self, flows):
        self.flows = FakeCountryFlows(flows)


def test_001_swiss_work_flows_keep_origin_or_destination(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    flows = pd.DataFrame(
        {
            "local_admin_unit_id_from": ["ch-1", "ch-9", "ch-2"],
            "local_admin_unit_id_to": ["ch-9", "ch-1", "ch-2"],
            "mode": ["car", "car", "car"],
            "ref_flow_volume": [10.0, 20.0, 30.0],
        }
    )
    monkeypatch.setattr(SwissWorkFlows, "get", lambda self: flows)

    selected_flows = SwissWorkFlows().filter_by_local_admin_unit_id(["ch-1"])

    assert selected_flows["ref_flow_volume"].tolist() == [10.0, 20.0]


def test_001_work_flows_use_country_priority_order_for_duplicates(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    french_data = FakeCountryData(
        pd.DataFrame(
            {
                "local_admin_unit_id_from": ["fr-1", "fr-1"],
                "local_admin_unit_id_to": ["ch-1", "fr-2"],
                "mode": ["car", "walk"],
                "ref_flow_volume": [10.0, 5.0],
            }
        )
    )
    swiss_data = FakeCountryData(
        pd.DataFrame(
            {
                "local_admin_unit_id_from": ["fr-1", "ch-1"],
                "local_admin_unit_id_to": ["ch-1", "ch-2"],
                "mode": ["car", "car"],
                "ref_flow_volume": [20.0, 7.0],
            }
        )
    )
    monkeypatch.setattr(
        "mobility.activities.work.work_flows.available_work_data",
        lambda: {"fr": french_data, "ch": swiss_data},
    )

    flows = WorkFlows(
        countries=["fr", "ch"],
        local_admin_unit_ids=["fr-1"],
        country_priority_order=["ch", "fr"],
    ).create_and_get_asset()

    assert french_data.flows.selected_local_admin_unit_ids == ["fr-1"]
    assert swiss_data.flows.selected_local_admin_unit_ids == ["fr-1"]
    assert flows.to_dict("records") == [
        {
            "local_admin_unit_id_from": "fr-1",
            "local_admin_unit_id_to": "ch-1",
            "mode": "car",
            "ref_flow_volume": 20.0,
        },
        {
            "local_admin_unit_id_from": "fr-1",
            "local_admin_unit_id_to": "fr-2",
            "mode": "walk",
            "ref_flow_volume": 5.0,
        },
    ]


def test_001_study_flows_use_country_priority_order_for_duplicates(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))

    french_data = FakeCountryData(
        pd.DataFrame(
            {
                "local_admin_unit_id_from": ["fr-1"],
                "local_admin_unit_id_to": ["ch-1"],
                "school_type": ["3"],
                "n_students": [10.0],
            }
        )
    )
    swiss_data = FakeCountryData(
        pd.DataFrame(
            {
                "local_admin_unit_id_from": ["fr-1"],
                "local_admin_unit_id_to": ["ch-1"],
                "school_type": ["3"],
                "n_students": [20.0],
            }
        )
    )
    monkeypatch.setattr(
        "mobility.activities.studies.study_flows.available_study_data",
        lambda: {"fr": french_data, "ch": swiss_data},
    )

    flows = StudyFlows(
        countries=["fr", "ch"],
        local_admin_unit_ids=["fr-1"],
        country_priority_order=["ch", "fr"],
    ).create_and_get_asset()

    assert flows.to_dict("records") == [
        {
            "local_admin_unit_id_from": "fr-1",
            "local_admin_unit_id_to": "ch-1",
            "school_type": "3",
            "n_students": 20.0,
        }
    ]
