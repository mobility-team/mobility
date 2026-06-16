import json
import logging
import os
import pathlib
import subprocess

import geopandas as gpd
import pandas as pd
import requests

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file
from mobility.spatial.local_admin_units import LocalAdminUnits
from mobility.spatial.osm import OSMData
from mobility.spatial.study_area import StudyArea


class SwissStudyOpportunities(FileAsset):
    """Swiss study opportunities by local admin unit."""

    def __init__(self, local_admin_unit_ids: list[str] | None = None):
        inputs = {"local_admin_unit_ids": local_admin_unit_ids or []}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bfs"
            / "study_opportunities_ch.parquet"
        )
        super().__init__(inputs, cache_path)

    def filter_by_local_admin_unit_id(self, local_admin_unit_ids):
        """Keep rows in the selected local admin units."""
        if not local_admin_unit_ids:
            schools = self.get()
            return schools
        schools = SwissStudyOpportunities(local_admin_unit_ids).get()
        return schools[schools["local_admin_unit_id"].isin(local_admin_unit_ids)].copy()

    def get_cached_asset(self):
        logging.info("Swiss school capacity already prepared. Reusing %s.", self.cache_path)
        schools = gpd.read_parquet(self.cache_path)
        schools = schools.set_crs(3035)
        return schools

    def create_and_get_asset(self):
        admin_unit_ids = [
            local_admin_unit_id
            for local_admin_unit_id in self.inputs["local_admin_unit_ids"]
            if local_admin_unit_id.startswith("ch-")
        ]
        if not admin_unit_ids:
            admin_units = LocalAdminUnits(countries=["ch"]).get()
            admin_unit_ids = admin_units["local_admin_unit_id"].tolist()

        study_area = StudyArea(admin_unit_ids, radius=0)
        pbf_path = OSMData(
            study_area,
            object_type="nwr",
            key="amenity",
            tags=["school", "kindergarten", "college", "university"],
            geofabrik_extract_date="260101",
            split_local_admin_units=False,
        ).get()

        schools = self.get_swiss_osm_schools(pbf_path, study_area.get())
        totals = self.get_swiss_student_totals(year_oblig="2023/24", year_sup="2024/25")

        surface_by_group = (
            schools.groupby("amenity_group", as_index=False)["area_m2"]
            .sum()
            .rename(columns={"area_m2": "area_m2_sum"})
        )
        ratio = pd.DataFrame({"amenity_group": ["oblig", "superieur"]}).merge(
            surface_by_group,
            on="amenity_group",
            how="left",
        )
        ratio["effectifs"] = ratio["amenity_group"].map(totals)
        ratio["ratio_elev_m2"] = ratio["effectifs"] / ratio["area_m2_sum"]

        ratio_by_group = ratio.set_index("amenity_group")["ratio_elev_m2"].to_dict()
        schools["n_students"] = schools["area_m2"] * schools["amenity_group"].map(ratio_by_group)
        schools["geometry"] = schools.geometry.representative_point()
        schools = schools.to_crs(3035)
        schools = schools.rename(columns={"amenity": "school_type"})
        schools = schools[["school_type", "local_admin_unit_id", "geometry", "n_students"]]
        schools.to_parquet(self.cache_path)
        return schools

    def get_swiss_student_totals(self, year_oblig="2023/24", year_sup="2024/25"):
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)

        urls = {
            "oblig": "https://www.data.gouv.fr/api/1/datasets/r/fd96016f-eb0b-4a51-9158-9d063c2a566f",
            "heu": "https://www.data.gouv.fr/api/1/datasets/r/6dd5621e-581c-4c4e-838d-f4796ac69afc",
            "hes": "https://www.data.gouv.fr/api/1/datasets/r/a0321bb2-cb2a-498d-8d13-976fff4b8e14",
            "hep": "https://www.data.gouv.fr/api/1/datasets/r/714e5796-5722-4d56-b4fe-2778deb8b324",
        }
        for key, url in urls.items():
            path = data_folder / f"ch-{key}.csv"
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            path.write_bytes(response.content)

        df_obl = pd.read_csv(data_folder / "ch-oblig.csv", encoding="utf-8")
        df_heu = pd.read_csv(data_folder / "ch-heu.csv", encoding="utf-8")
        df_hes = pd.read_csv(data_folder / "ch-hes.csv", encoding="utf-8")
        df_hep = pd.read_csv(data_folder / "ch-hep.csv", encoding="utf-8")

        s_oblig = df_obl.loc[df_obl["Année"].astype(str) == year_oblig, "VALUE"].sum()
        s_sup = (
            df_heu.loc[df_heu["Année"].astype(str) == year_sup, "VALUE"].sum()
            + df_hes.loc[df_hes["Année"].astype(str) == year_sup, "VALUE"].sum()
            + df_hep.loc[df_hep["Année"].astype(str) == year_sup, "VALUE"].sum()
        )
        return {"oblig": float(s_oblig), "superieur": float(s_sup)}

    def get_swiss_osm_schools(self, pbf_path: pathlib.Path, study_area_gdf: gpd.GeoDataFrame):
        pbf_path = pathlib.Path(pbf_path)
        study_area_4326 = study_area_gdf.to_crs(4326)

        out_seq = pbf_path.with_name("amenity_multipolygons.geojsonseq")
        subprocess.run(
            [
                "osmium",
                "export",
                str(pbf_path),
                "--overwrite",
                "--geometry-types=polygon,multipolygon",
                "-f",
                "geojsonseq",
                "-o",
                str(out_seq),
            ],
            check=True,
        )

        features = []
        with open(out_seq, "r", encoding="utf-8") as file:
            for line in file:
                text = line.strip()
                if not text.startswith("{"):
                    continue
                try:
                    item = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if item.get("type") == "Feature":
                    features.append(item)

        schools = (
            gpd.GeoDataFrame.from_features(features)
            if features
            else gpd.GeoDataFrame(geometry=gpd.GeoSeries(dtype="geometry"))
        )
        if not schools.empty and schools.crs is None:
            schools.set_crs(4326, inplace=True)

        schools = schools[schools.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
        schools = gpd.overlay(schools, study_area_4326, how="intersection")
        columns = ["amenity", "local_admin_unit_id", "local_admin_unit_name", "country", "urban_unit_category", "geometry"]
        schools = schools.loc[:, [column for column in columns if column in schools.columns]].copy()
        schools = schools[schools["amenity"].isin(["college", "kindergarten", "school", "university"])].copy()
        schools["amenity_group"] = schools["amenity"].replace(
            {
                "college": "oblig",
                "kindergarten": "oblig",
                "school": "oblig",
                "university": "superieur",
            }
        )
        schools = schools[~schools.geometry.is_empty & schools.geometry.notna()].copy()
        schools_m = schools.to_crs(2056)
        schools["area_m2"] = schools_m.geometry.area.values
        return schools


class SwissStudyFlows:
    """Swiss home-study flows."""

    def filter_by_local_admin_unit_id(self, local_admin_unit_ids) -> pd.DataFrame:
        """Keep flows where origin or destination is in the selected local admin units."""
        raise ValueError("Swiss school flow data is not available yet.")


class SwissStudy:
    """Swiss study inputs."""

    country = "ch"

    @property
    def opportunities(self):
        return SwissStudyOpportunities()

    @property
    def flows(self):
        return SwissStudyFlows()
