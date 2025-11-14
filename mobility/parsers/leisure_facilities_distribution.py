import os
import json
import pathlib
import logging
import subprocess

import geopandas as gpd
from shapely.geometry import shape, Polygon, MultiPolygon

from mobility.file_asset import FileAsset
from mobility.parsers.local_admin_units import LocalAdminUnits
from mobility.study_area import StudyArea
from mobility.parsers.osm import OSMData
from mobility.parsers.leisures_frequentation import LEISURE_MAPPING, LEISURE_FREQUENCY


class LeisureFacilitiesDistribution(FileAsset):
    """
    Build a point layer of leisure facilities:
    - OSM key=leisure, from Geofabrik extracts
    - polygons converted to representative points
    - private access removed
    - some noisy values cleaned / remapped
    - each facility assigned a frequency score
    - stored as a Parquet GeoDataFrame in EPSG:3035
    """

    def __init__(self) -> None:
        inputs = {}

        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "osm"
            / "leisures_points.parquet"
        )

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> gpd.GeoDataFrame:
        logging.info(
            "Leisure facilities already prepared. Reusing the file: %s",
            self.cache_path,
        )
        gdf = gpd.read_parquet(self.cache_path)
        gdf = gdf.set_crs(3035)
        return gdf

    def create_and_get_asset(self) -> gpd.GeoDataFrame:
        gdf = self._prepare_leisure_facilities()
        gdf.to_parquet(self.cache_path, index=False)
        return gdf

    def _prepare_leisure_facilities(self) -> gpd.GeoDataFrame:
        admin_units = LocalAdminUnits().get()
        admin_units_ids = admin_units["local_admin_unit_id"].tolist()

        study_area = StudyArea(admin_units_ids, radius=0)

        pbf_path = OSMData(
            study_area,
            object_type="nwr",
            key="leisure",
            geofabrik_extract_date="240101",
            split_local_admin_units=False,
        ).get()

        out_seq_leisure = pbf_path.with_name("leisures.geojsonseq")

        subprocess.run(
            [
                "osmium",
                "export",
                str(pbf_path),
                "--overwrite",
                "--geometry-types=polygon,multipolygon,point",
                "-f",
                "geojsonseq",
                "-o",
                str(out_seq_leisure),
            ],
            check=True,
        )

        rows = []

        with open(out_seq_leisure, "r", encoding="utf-8") as f:
            for line in f:
                s = line.lstrip("\x1e").strip()
                if not s.startswith("{"):
                    continue

                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    continue

                props = obj.get("properties", obj)
                leisure = props.get("leisure")
                if leisure is None:
                    continue

                geom = obj.get("geometry")
                if geom is None:
                    continue

                g = shape(geom)
                if isinstance(g, (Polygon, MultiPolygon)):
                    g = g.representative_point()

                rows.append(
                    {
                        "leisure": leisure,
                        "access": props.get("access"),
                        "geometry": g,
                    }
                )

        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

        # remove private facilities
        gdf = gdf[gdf["access"] != "private"]

        # remove gardens (often private or ornamental)
        gdf = gdf[gdf["leisure"] != "garden"]

        # normalize leisure values (lowercase, split composite values, apply mapping)
        vals = gdf["leisure"].astype(str).str.strip().str.lower()
        split_vals = vals.str.replace("+", ";", regex=False).str.split(";")

        cleaned = []
        for lst in split_vals:
            cleaned_value = None
            fallback = None

            for p in lst:
                p = p.strip()
                if not p:
                    continue

                if p in LEISURE_MAPPING:
                    cleaned_value = LEISURE_MAPPING[p]
                    break

                if fallback is None:
                    fallback = p

            if cleaned_value is None:
                cleaned_value = fallback

            cleaned.append(cleaned_value)

        gdf["leisure_clean"] = cleaned

        # drop items mapped to None or explicitly unwanted categories
        gdf = gdf[~gdf["leisure_clean"].isna()].copy()
        gdf = gdf[gdf["leisure_clean"] != "nature_reserve"]

        # assign frequency score
        gdf["freq_score"] = gdf["leisure_clean"].map(LEISURE_FREQUENCY).fillna(2)

        # reproject to 3035 and return
        gdf = gdf.to_crs(3035)

        return gdf
