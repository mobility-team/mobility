import os
import json
import pathlib
import logging
import subprocess
import pandas as pd
import geopandas as gpd
import requests

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file
from mobility.parsers.local_admin_units import LocalAdminUnits
from mobility.study_area import StudyArea
from mobility.parsers.osm import OSMData

class SchoolsCapacityDistribution(FileAsset):

    def __init__(self):

        inputs = {}

        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools_capacity.parquet"
        
        
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("School capacity spatial distribution already prepared. Reusing the file: " + str(self.cache_path))

        schools = gpd.read_parquet(self.cache_path)
        schools = schools.set_crs(3035)
        
        return schools

    def create_and_get_asset(self) -> pd.DataFrame:

        schools_fr = self.prepare_french_schools_capacity_distribution()
        schools_ch = self.prepare_swiss_schools_capacity_distribution()
        
        cols = ['school_type', 'local_admin_unit_id', 'geometry', 'n_students']
        schools_fr = schools_fr[[c for c in cols if c in schools_fr.columns]]
        schools_ch = schools_ch[[c for c in cols if c in schools_ch.columns]]
        
        school_students = pd.concat([schools_fr, schools_ch])
        school_students["school_type"] = school_students["school_type"].astype(str) 
        school_students["local_admin_unit_id"] = school_students["local_admin_unit_id"].astype(str)
        school_students["n_students"] = pd.to_numeric(school_students["n_students"], errors="coerce")

        school_students = gpd.GeoDataFrame(school_students, geometry="geometry", crs="EPSG:4326")
        school_students.to_crs(3035, inplace= True)
        
        school_students.to_parquet(self.cache_path)
        
        return school_students

    def prepare_french_schools_capacity_distribution(self):

        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)

        # ---------------------------------------------------------------------
        # Primary and secondary schools (Ecole, Collège, Lycée)
        # ---------------------------------------------------------------------

        url = (
            "https://www.data.gouv.fr/api/1/datasets/r/6ebc938c-af7a-4faa-b10b-7b2757b50404"
            )
        csv_path = data_folder / "fr-en-annuaire-education.csv"
        download_file(url, csv_path)

        schools = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "Code_commune",
                "Type_etablissement",
                "Nombre_d_eleves",
                "latitude",
                "longitude",
            ],
            dtype={
                "Code_commune": str,
                "Type_etablissement": str,
            },
        )

        schools.rename(
            columns={
                "Code_commune": "local_admin_unit_id",
                "Type_etablissement": "school_type",
                "Nombre_d_eleves": "n_students",
                "latitude": "lat",
                "longitude": "lon",
            },
            inplace=True,
        )

        schools = schools[schools["school_type"].isin(["Ecole", "Collège", "Lycée"])]
        schools["school_type"] = schools["school_type"].replace({"Ecole": 1, "Collège": 2, "Lycée": 3})
        schools = schools.dropna(subset=["n_students"])

        schools = schools.groupby(
            ["local_admin_unit_id", "school_type", "lon", "lat"], as_index=False
        )["n_students"].sum()

        # ---------------------------------------------------------------------
        # Higher education institutions
        # ---------------------------------------------------------------------
        url = "https://www.data.gouv.fr/fr/datasets/r/b10fd6c8-6bc9-41fc-bdfd-e0ac898c674a"
        csv_path = data_folder / "fr-esr-atlas_regional-effectifs-d-etudiants-inscrits-detail_etablissements.csv"
        download_file(url, csv_path)

        higher = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "Rentrée universitaire",
                "code commune",
                "gps",
                "nombre total d’étudiants inscrits hors doubles inscriptions université/CPGE",
            ],
            dtype={
                "Rentrée universitaire": int,
                "code commune": str,
                "gps": str,
                "nombre total d’étudiants inscrits hors doubles inscriptions université/CPGE": int,
            },
        )

        higher.columns = ["year", "local_admin_unit_id", "coords", "n_students"]
        higher[["lat", "lon"]] = higher["coords"].str.strip().str.split(",", expand=True).astype(float)
        higher = higher[higher["year"] == 2023]

        higher = higher.groupby(
            ["local_admin_unit_id", "lon", "lat"], as_index=False
        )["n_students"].sum()
        higher["school_type"] = 4

        # ---------------------------------------------------------------------
        # Concatenate and return
        # ---------------------------------------------------------------------

        schools = pd.concat([schools, higher], ignore_index=True)

        schools["local_admin_unit_id"] = "fr-" + schools["local_admin_unit_id"]
        
        schools_fr = gpd.GeoDataFrame(
            schools,
            geometry=gpd.points_from_xy(schools["lon"], schools["lat"]),
            crs="EPSG:4326"
        )

        return schools_fr
    
    
        
        
    def get_swiss_student_totals(self, year_oblig="2023/24", year_sup="2024/25"):
    
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)
    
        urls = {
            "oblig": "https://www.data.gouv.fr/api/1/datasets/r/fd96016f-eb0b-4a51-9158-9d063c2a566f",
            "heu":   "https://www.data.gouv.fr/api/1/datasets/r/6dd5621e-581c-4c4e-838d-f4796ac69afc",
            "hes":   "https://www.data.gouv.fr/api/1/datasets/r/a0321bb2-cb2a-498d-8d13-976fff4b8e14",
            "hep":   "https://www.data.gouv.fr/api/1/datasets/r/714e5796-5722-4d56-b4fe-2778deb8b324",
        }
    
        for key, url in urls.items():
            p = data_folder / f"ch-{key}.csv"
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            p.write_bytes(r.content)
    
        df_obl = pd.read_csv(data_folder / "ch-oblig.csv", encoding="utf-8")
        df_heu = pd.read_csv(data_folder / "ch-heu.csv",   encoding="utf-8")
        df_hes = pd.read_csv(data_folder / "ch-hes.csv",   encoding="utf-8")
        df_hep = pd.read_csv(data_folder / "ch-hep.csv",   encoding="utf-8")
    
        s_oblig = df_obl.loc[df_obl["Année"].astype(str) == year_oblig, "VALUE"].sum()
    
        s_sup = (
            df_heu.loc[df_heu["Année"].astype(str) == year_sup, "VALUE"].sum() +
            df_hes.loc[df_hes["Année"].astype(str) == year_sup, "VALUE"].sum() +
            df_hep.loc[df_hep["Année"].astype(str) == year_sup, "VALUE"].sum()
        )
    
        return {"oblig": float(s_oblig), "superieur": float(s_sup)}

    
    
    def get_swiss_osm_schools(self, pbf_path: pathlib.Path, study_area_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Extrait les (Multi)Polygons OSM 'amenity' (college/kindergarten/school/university),
        clippe au périmètre, regroupe en 'oblig' vs 'superieur' et calcule area_m2.
        """
        pbf_path = pathlib.Path(pbf_path)
        study_area_4326 = study_area_gdf.to_crs(4326)
    
        # Export PBF -> GeoJSONSeq (polygones)
        out_seq = pbf_path.with_name("amenity_multipolygons.geojsonseq")
        subprocess.run([
            "osmium","export",str(pbf_path),
            "--overwrite","--geometry-types=polygon,multipolygon",
            "-f","geojsonseq","-o",str(out_seq)
        ], check=True)
    
        # Lecture GeoJSONSeq robuste
        feats = []
        with open(out_seq, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s.startswith("{"):
                    continue
                try:
                    obj = json.loads(s)
                    if obj.get("type") == "Feature":
                        feats.append(obj)
                except json.JSONDecodeError:
                    continue
    
        gdf = gpd.GeoDataFrame.from_features(feats) if feats else gpd.GeoDataFrame(geometry=gpd.GeoSeries(dtype="geometry"))
        if not gdf.empty and gdf.crs is None:
            gdf.set_crs(4326, inplace=True)
    
        # Polygones uniquement + clip
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon","MultiPolygon"])].copy()
        gdf = gpd.overlay(gdf, study_area_4326, how="intersection")
    
        # Colonnes utiles + filtre amenities
        cols = ["amenity","local_admin_unit_id","local_admin_unit_name","country","urban_unit_category","geometry"]
        gdf = gdf.loc[:, [c for c in cols if c in gdf.columns]].copy()
        gdf = gdf[gdf["amenity"].isin(["college","kindergarten","school","university"])].copy()
    
        # Regroupement
        gdf["amenity_group"] = gdf["amenity"].replace({
            "college":"oblig", "kindergarten":"oblig", "school":"oblig", "university":"superieur"
        })
    
        # Surface (m²) en 2056
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
        gdf_m = gdf.to_crs(2056)
        gdf["area_m2"] = gdf_m.geometry.area.values
    
        return gdf
        
    def prepare_swiss_schools_capacity_distribution(self):
        

        admin_units = LocalAdminUnits().get()
        admin_units_ch = admin_units["local_admin_unit_id"].astype(str).str.lower().str.startswith("ch")
        admin_units_ch_ids = admin_units.loc[admin_units_ch, "local_admin_unit_id"].tolist()

        study_area = StudyArea(admin_units_ch_ids, radius = 0)
        
        education_tags = ["school","kindergarten","college","university"]
        pbf_path = OSMData(study_area, object_type="nwr", key="amenity",
                           tags=education_tags, geofabrik_extract_date="240101",
                           split_local_admin_units=False).get()

        out_seq = pbf_path.with_name("amenity_multipolygons.geojsonseq")

        # Export (Multi)Polygons uniquement
        subprocess.run([
            "osmium","export",str(pbf_path),
            "--overwrite","--geometry-types=polygon,multipolygon",
            "-f","geojsonseq","-o",str(out_seq)
        ], check=True)
        
        study_area_gdf = study_area.get()
        schools_ch = self.get_swiss_osm_schools(pbf_path, study_area_gdf)
        totals = self.get_swiss_student_totals(year_oblig="2023/24", year_sup="2024/25")

        # Surfaces par groupe
        surf = (schools_ch.groupby("amenity_group", as_index=False)["area_m2"]
                         .sum().rename(columns={"area_m2":"area_m2_sum"}))

        # Ratios élèves/m²
        ratio_df = pd.DataFrame({"amenity_group":["oblig","superieur"]}).merge(surf, on="amenity_group", how="left")
        ratio_df["effectifs"] = ratio_df["amenity_group"].map(totals)
        ratio_df["ratio_elev_m2"] = ratio_df["effectifs"] / ratio_df["area_m2_sum"]

        # Application aux polygones
        rmap = ratio_df.set_index("amenity_group")["ratio_elev_m2"].to_dict()
        schools_ch["n_students"] = schools_ch["area_m2"] * schools_ch["amenity_group"].map(rmap)
        
        schools_ch["geometry"] = schools_ch.geometry.representative_point()
        schools_ch = schools_ch.to_crs(4326)
        
        schools_ch = schools_ch.rename(columns={"amenity": "school_type"})

        
        return schools_ch

        
        

                
        


        