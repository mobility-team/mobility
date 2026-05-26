import geopandas as gpd
from shapely.geometry import LineString, Polygon

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.transport.graphs.modified.modifiers import speed_modifier


class FakeStudyArea:
    def __init__(self, boundary_path):
        self.cache_path = {"boundary": boundary_path}


class FakeTransportZones(InMemoryAsset):
    def __init__(self, boundary_path):
        super().__init__({})
        self.inputs = {"study_area": FakeStudyArea(boundary_path)}
        self.was_called = False

    def get(self):
        self.was_called = True


class FakeGeofabrikRegions:
    def __init__(self, extract_date):
        self.extract_date = extract_date

    def get(self):
        return gpd.GeoDataFrame(
            {
                "url": ["empty", "outside", "inside"],
                "geometry": [
                    Polygon([(-1, -1), (3, -1), (3, 3), (-1, 3)]),
                    Polygon([(-1, -1), (3, -1), (3, 3), (-1, 3)]),
                    Polygon([(-1, -1), (3, -1), (3, 3), (-1, 3)]),
                ],
            },
            crs=4326,
        )


class FakeGeofabrikExtract:
    def __init__(self, url):
        self.url = url


class FakeOSMCountryBorder:
    def __init__(self, extract):
        self.path = f"{extract.url}.geojson"

    def get(self):
        return self.path


def test_border_crossing_modifier_keeps_only_borders_crossing_the_study_area(monkeypatch):
    """Ignore empty and unrelated border files before calling the R graph modifier."""

    boundary_path = "boundary.geojson"
    transport_zones = FakeTransportZones(boundary_path)
    boundary = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])

    empty_borders = gpd.GeoDataFrame(geometry=[], crs=4326)
    outside_borders = gpd.GeoDataFrame(
        geometry=[LineString([(10, 10), (11, 11)])],
        crs=4326,
    )
    inside_borders = gpd.GeoDataFrame(
        geometry=[LineString([(-1, 1), (3, 1)])],
        crs=4326,
    )
    files = {
        boundary_path: gpd.GeoDataFrame(geometry=[boundary], crs=4326),
        "empty.geojson": empty_borders,
        "outside.geojson": outside_borders,
        "inside.geojson": inside_borders,
    }

    monkeypatch.setattr(speed_modifier, "GeofabrikRegions", FakeGeofabrikRegions)
    monkeypatch.setattr(speed_modifier, "GeofabrikExtract", FakeGeofabrikExtract)
    monkeypatch.setattr(speed_modifier, "OSMCountryBorder", FakeOSMCountryBorder)
    monkeypatch.setattr(speed_modifier.gpd, "read_file", lambda path: files[path])

    modifier = speed_modifier.BorderCrossingSpeedModifier(
        transport_zones,
        max_speed=30.0,
        time_penalty=5.0,
        geofabrik_extract_date="240101",
    )

    result = modifier.get()

    assert transport_zones.was_called
    assert result["has_borders"] is True
    assert result["borders"] == ["inside.geojson"]
