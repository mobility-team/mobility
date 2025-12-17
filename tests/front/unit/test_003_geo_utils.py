import geopandas as gpd
from shapely.geometry import Polygon
from app.components.features.map.geo_utils import safe_center, ensure_wgs84

def test_safe_center_simple_square():
    gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0,0),(1,0),(1,1),(0,1),(0,0)])],
        crs="EPSG:4326",
    )
    center = safe_center(gdf)
    assert isinstance(center, tuple) and len(center) == 2
    lon, lat = center
    assert 0.4 < lon < 0.6
    assert 0.4 < lat < 0.6

def test_ensure_wgs84_no_crs():
    gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0,0),(1,0),(1,1),(0,1),(0,0)])],
        crs=None,
    )
    out = ensure_wgs84(gdf)
    assert out.crs is not None