import quackosm
import duckdb

duckdb.install_extension("spatial")
duckdb.load_extension("spatial")

pbf_path = "D:/data/mobility/projects/study_area/f24dc3bfc3575879d875a07a60e2dd5b-building-osm_data.pbf"
gpq_path = quackosm.convert_pbf_to_parquet(pbf_path)
