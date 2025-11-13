import logging

import polars as pl
import geopandas as gpd

from shapely import linestrings

class PublicTransportNetworkEvaluation:
    
    def __init__(self, results):
        self.results = results
    
    
    def get(
            self
        ):

        pt_mode = [m for m in self.results.modes if m.name == "walk/public_transport/walk"]
        
        if len(pt_mode) == 0:
            raise ValueError("No public transport mode in the model.")
            
        pt_mode = pt_mode[0]
        
        public_transport_graph = pt_mode.travel_costs.intermodal_graph.public_transport_graph
        graph = self.build_graph_lines_dataframe(public_transport_graph)
        
        geoms = linestrings(
            graph.select(["x", "y"]),
            indices=graph["edge_id"]
        )
        
        gdf = gpd.GeoDataFrame(
            graph.filter(pl.col("point_id") == 0).select(["edge_id", "time"]).to_pandas(),
            geometry=geoms,
            crs="EPSG:3035"
        )
        
        fp = public_transport_graph.cache_path.parent / "public_transport_network.gpkg"
        
        gdf.to_file(
            fp,
            layer="graph",
            driver="GPKG"
        )
        
        logging.info(f"Public transport network gpkg created in {fp}")

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
        vertices = ( 
            pl.read_parquet(vertices_path)
            .with_columns(
                vertex_id=pl.col("vertex_id").cast(pl.Int32()).cast(pl.String())
            )
        )
        
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
            
            # Filter edges with less than 2 points to avoid a shapely error later on
            # (this should never be the case ?)
            .with_columns(
                n_points=pl.col("point_id").count().over("edge_id")
            )
            .filter(
                pl.col("n_points") == 2
            )
            .with_columns(
                edge_id=pl.col("edge_id").rank("dense").cast(pl.Int64()) - 1
            )
            
            .sort("edge_id")
            
        )
        
        return graph_lines