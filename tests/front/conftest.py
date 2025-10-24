# tests/front/conftest.py
import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]   # -> repository root
FRONT_DIR = REPO_ROOT / "front"
if str(FRONT_DIR) not in sys.path:
    sys.path.insert(0, str(FRONT_DIR))
    
@pytest.fixture
def sample_scn():
    poly = Polygon([
        (1.43, 43.60), (1.45, 43.60),
        (1.45, 43.62), (1.43, 43.62),
        (1.43, 43.60)
    ])

    zones_gdf = gpd.GeoDataFrame(
        {
            "transport_zone_id": ["Z1"],
            "local_admin_unit_id": ["31555"],
            "average_travel_time": [32.4],
            "total_dist_km": [7.8],
            "total_time_min": [45.0],
            "share_car": [0.52],
            "share_bicycle": [0.18],
            "share_walk": [0.30],
            "geometry": [poly],
        },
        crs="EPSG:4326",
    )

    flows_df = pd.DataFrame({"from": [], "to": [], "flow_volume": []})
    zones_lookup = zones_gdf[["transport_zone_id", "geometry"]].copy()

    return {"zones_gdf": zones_gdf, "flows_df": flows_df, "zones_lookup": zones_lookup}

@pytest.fixture(autouse=True)
def patch_services(monkeypatch, sample_scn):
    # Patch le service scénario pour les tests d’intégration
    import front.app.services.scenario_service as scn_mod
    def fake_get_scenario(radius=40, local_admin_unit_id="31555"):
        return sample_scn
    monkeypatch.setattr(scn_mod, "get_scenario", fake_get_scenario, raising=True)

    # Patch map_service option B (si présent)
    try:
        import front.app.services.map_service as map_service
        from app.components.features.map.config import DeckOptions
        from app.components.features.map.deck_factory import make_deck_json
        def fake_get_map_deck_json_from_scn(scn, opts=None):
            opts = opts or DeckOptions()
            return make_deck_json(scn, opts)
        monkeypatch.setattr(map_service, "get_map_deck_json_from_scn",
                            fake_get_map_deck_json_from_scn, raising=False)
    except Exception:
        pass

    yield
