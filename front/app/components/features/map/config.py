from dataclasses import dataclass

# ---------- CONSTANTES ----------
CARTO_POSITRON_GL = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
FALLBACK_CENTER = (1.4442, 43.6045)  # Toulouse

HEADER_OFFSET_PX = 80
SIDEBAR_WIDTH = 340

# ---------- OPTIONS ----------
@dataclass(frozen=True)
class DeckOptions:
    zoom: float = 10
    pitch: float = 35
    bearing: float = -15
    map_style: str = CARTO_POSITRON_GL
