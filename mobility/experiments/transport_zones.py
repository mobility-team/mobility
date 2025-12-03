import quackosm
import duckdb
import polars as pl
import numpy as np
import pathlib
from sklearn.cluster import MiniBatchKMeans
import logging


def pbf_to_gpq(pbf_path):
    
    pbf_path = pathlib.Path(pbf_path)
    gpq_path = pbf_path.parent / (pbf_path.name + "-buildings.parquet")
    
    if gpq_path.exists() is False:
        quackosm.convert_pbf_to_parquet(
            pbf_path,
            result_file_path=gpq_path,
            tags_filter={"building": True}
        )
        
    return str(gpq_path)


def build_building_area_grid(con, study_area_path, gpq_paths):
    
    con.execute("""
                
        CREATE OR REPLACE TABLE study_area AS
        SELECT
          *
        FROM ST_Read(?);
        
    """, [study_area_path])
    
    con.execute("""
                            
        CREATE OR REPLACE TABLE buildings AS
                     
        WITH src AS (
            SELECT * 
            FROM read_parquet(?)
        ),
        
        p3035 AS (
          SELECT
            ST_Transform(geometry,'EPSG:4326','EPSG:3035', TRUE) AS geometry
          FROM src
        ),
        
        filtered AS (
          SELECT
            geometry,
            ST_Area(geometry) AS area,
            ST_Centroid(geometry) AS centroid
          FROM p3035
        )
        
        SELECT
          filtered.area,
          ST_X(filtered.centroid) AS x,
          ST_Y(filtered.centroid) AS y,
          study_area.local_admin_unit_id
        FROM filtered 
        JOIN study_area
            ON ST_Intersects(filtered.geometry, study_area.geom)
        WHERE area > 20
    
    """, [gpq_paths])
    
    
    con.execute("""
                                 
        CREATE OR REPLACE TABLE buildings_grid AS
                                 
        SELECT 
            local_admin_unit_id,
            ROUND(x/50.0)*50.0 as x,
            ROUND(y/50.0)*50.0 as y,
            sum(area) as area
        FROM buildings 
        GROUP BY local_admin_unit_id, ROUND(x/50.0)*50.0, ROUND(y/50.0)*50.0 
        
    """)

def get_buildings_clusters_centers(local_admin_unit_id: str) -> pl.DataFrame:
    
    log.info(f"Finding buildings clusters centers for LAU {local_admin_unit_id}")
    
    
    buildings = con.execute("""
        SELECT 
            x, y 
        FROM buildings 
        WHERE local_admin_unit_id = ? 
    """, [local_admin_unit_id]).pl()
    
    buildings_coords = np.c_[
        buildings["x"].to_numpy(),
        buildings["y"].to_numpy()
    ]
    
    buildings_grid = con.execute("""
        SELECT 
            *
        FROM buildings_grid 
        WHERE local_admin_unit_id = ? 
    """, [local_admin_unit_id]).pl()

    buildings_grid_coords = np.c_[
        buildings_grid["x"].to_numpy(),
        buildings_grid["y"].to_numpy()
    ]
    buildings_grid_weight = buildings_grid["area"].to_numpy()
    
    buildings_clusters_centers = []
    
    for n_clusters in range(1, 6):
    
        km = MiniBatchKMeans(
            n_clusters=n_clusters,
            random_state=0,
            n_init=1,
            max_iter=50,
            tol=1e-2
        )
        
        km.fit(
            buildings_grid_coords,
            sample_weight=buildings_grid_weight
        )
        
        centers = km.cluster_centers_
    
        idx = np.argmin(((buildings_coords[:,None,:]-centers[None,:,:])**2).sum(2), axis=0)
        centers = buildings_coords[idx]
        
        from shapely.geometry import MultiPoint, box
        from shapely.ops import voronoi_diagram
        import geopandas as gpd
        
        voronoi = voronoi_diagram(
            geom=MultiPoint(centers),
            envelope=box(
                centers[:,0].min()-10e3,
                centers[:,0].min()-10e3,
                centers[:,1].max()+10e3,
                centers[:,1].max()+10e3
            )
        )
        
        gpd.GeoDataFrame(geometry=list(voronoi.geoms), crs="EPSG:3035").plot(facecolor="none")

        buildings_clusters_centers.append(
            pl.DataFrame({
                "local_admin_unit_id": local_admin_unit_id,
                "n_clusters": n_clusters,
                "building_id": np.arange(1, n_clusters+1, dtype=int),
                "x": centers[:,0],
                "y": centers[:,1]
            })
        )
        
    buildings_clusters_centers = pl.concat(buildings_clusters_centers)
        
    return buildings_clusters_centers


if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    log = logging.getLogger(__name__)
    
    study_area_path = "d:/data/mobility/projects/grand-geneve/fa13b52a0ef729b2f68561ea0d101d62-study_area.gpkg"
    
    pbf_paths = [
        "d:/data/mobility/package/osm/78626e49cb02c2566219f7bf96f9f12f-franche-comte-240101.osm.pbf",
        "d:/data/mobility/package/osm/63ed165658899e2cf09c47b91a691db1-rhone-alpes-240101.osm.pbf",
        "d:/data/mobility/package/osm/7b7d2cab2f18375be9dca3432caa85f7-switzerland-240101.osm.pbf",
    ]
    
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    
    gpq_paths = [pbf_to_gpq(p) for p in pbf_paths]
    
    build_building_area_grid(con, study_area_path, gpq_paths)
    
    lau_ids = con.execute(
        """
            SELECT local_admin_unit_id as value
            FROM study_area
        """
    ).pl()["value"].to_list()

   
    for lau_id in lau_ids:
        buildings_clusters_centers = get_buildings_clusters_centers(lau_id)
    
        
        
    
    
