from unittest.mock import MagicMock

from mobility.spatial.osm.geofabrik_regions import GeofabrikRegions


def test_geofabrik_boundaries_use_shared_http_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    response = MagicMock()
    response.__enter__.return_value.text = (
        "region\n1\n   1.0   2.0\n   2.0   2.0\n   2.0   3.0\nEND\nEND\n"
    )
    request = MagicMock(return_value=response)
    monkeypatch.setattr("mobility.spatial.osm.geofabrik_regions.request_url", request)

    regions = GeofabrikRegions("260101").create_and_get_asset()

    assert len(regions) == 23
    assert regions.geometry.iloc[0].area == 0.5
    assert regions.url.iloc[0].endswith("/alsace-260101.osm.pbf")
    assert request.call_count == 23
    assert response.__exit__.call_count == 23
