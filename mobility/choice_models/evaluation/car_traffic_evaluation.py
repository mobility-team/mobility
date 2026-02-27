import logging

import polars as pl
import geopandas as gpd

from shapely import linestrings

class CarTrafficEvaluation:
    
    def __init__(self, results):
        self.results = results
    
    
    def get(
            self,
            weekday: bool = True
        ):

        car_mode = [m for m in self.results.modes if m.inputs["parameters"].name == "car"]
        
        if len(car_mode) == 0:
            raise ValueError("No car mode in the model.")
            
        car_mode = car_mode[0]

        freeflow_graph = self.build_graph_lines_dataframe(car_mode.inputs["travel_costs"].modified_path_graph)
        congested_graph = self.build_graph_lines_dataframe(car_mode.inputs["travel_costs"].congested_path_graph)
        
        comparison = (
            freeflow_graph
            .join(
                congested_graph.select(["edge_id", "point_id", "time"]),
                on=["edge_id", "point_id"],
                suffix="_congested"
            )
            .with_columns(
                time_ratio=pl.col("time_congested")/pl.col("time")
            )
        )
        
        geoms = linestrings(
            comparison.select(["x", "y"]),
            indices=comparison["edge_id"]
        )
        
        gdf = gpd.GeoDataFrame(
            comparison.filter(pl.col("point_id") == 0).select(["edge_id", "time", "time_congested", "time_ratio"]).to_pandas(),
            geometry=geoms,
            crs="EPSG:3035"
        )
        
        fp = car_mode.inputs["travel_costs"].modified_path_graph.cache_path.parent / "congestion.gpkg"
        
        gdf.to_file(
            fp,
            layer="graph",
            driver="GPKG"
        )
        
        logging.info(f"Congestion gpkg created in {fp}")

        return None
    
    
    def build_graph_lines_dataframe(self, path_graph):
        
        graph_file_token = path_graph.cache_path
        graph_file_folder = graph_file_token.parent
        graph_file_hash = graph_file_token.stem.split("-")[0]
        
        graph_data_path = graph_file_folder / (graph_file_hash + "data.parquet")
        graph_dict_path = graph_file_folder / (graph_file_hash + "dict.parquet")
        
        graph_data = ( 
            pl.read_parquet(graph_data_path)
            .with_row_index("edge_id")
        )
            
        graph_dict = pl.read_parquet(graph_dict_path)
        
        vertices_path = graph_file_folder.parent / (graph_file_hash + "-vertices.parquet")
        vertices = pl.read_parquet(vertices_path)
        
        graph_lines = pl.concat([
            
            graph_data
            .join(graph_dict, left_on="from", right_on="id")
            .join(vertices, left_on="ref", right_on="vertex_id")
            .select(["edge_id", "x", "y"])
            .with_columns(
                point_id=pl.lit(0, pl.UInt8)
            )
            
            ,
            
            graph_data
            .join(graph_dict, left_on="to", right_on="id")
            .join(vertices, left_on="ref", right_on="vertex_id")
            .select(["edge_id", "x", "y"])
            .with_columns(
                point_id=pl.lit(1, pl.UInt8)
            )
            
        ])
        
        graph_lines = ( 
            
            graph_lines
            .join(graph_data.select(["edge_id", "dist"]).rename({"dist": "time"}), on="edge_id")
            .sort("edge_id")
            
        )
        
        return graph_lines
