import pandas as pd

from mobility.spatial.osm.geofabrik_regions import GeofabrikRegions


def test_region_boundaries_use_the_current_geofabrik_path(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    urls = []

    def read_boundary(url, **kwargs):
        urls.append(url)
        return pd.DataFrame({"coordinates": ["   1.0   1.0", "   2.0   1.0", "   1.0   2.0"]})

    monkeypatch.setattr(pd, "read_table", read_boundary)
    regions = GeofabrikRegions().get()
    assert len(regions) == 23
    assert urls[0] == "https://download.geofabrik.de/europe/france/alsace/alsace.poly"
    assert urls[-1] == "https://download.geofabrik.de/europe/switzerland/switzerland.poly"
    assert regions.iloc[0]["url"] == "https://download.geofabrik.de/europe/france/alsace-260101.osm.pbf"
