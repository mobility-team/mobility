import json
import logging

import polars as pl
import geopandas as gpd

from typing import List
from shapely import linestrings
from importlib import resources
from sklearn.neighbors import NearestNeighbors
from mobility.r_utils.r_script import RScript

class RoutingEvaluation:
    
    def __init__(self, results):
        self.results = results
    
    
    def get(
            self,
            routes: pl.DataFrame,
            weekday: bool = True
        ):
        
        graph = self.get_mode_graph(self.results.modes, "car")
        transport_zones = self.results.transport_zones
        
        graph_folder, graph_hash = self.get_graph_folder_and_hash(graph)
        
        vertices = self.get_vertices(graph_folder, graph_hash)
    
        routes = self.convert_to_dataframe(routes)
        routes = self.join_transport_zone_info(routes, transport_zones)
        routes = self.snap_routes_to_graph(
            routes,
            graph,
            vertices,
            transport_zones
        )
        
        cpprouting_paths = self.run_cpprouting_get_path_pair(routes, graph)
        graph_data, graph_dict = self.get_graph_data(graph_folder, graph_hash)
        gpd_paths = self.create_geopandas_paths(cpprouting_paths, graph_data, graph_dict, vertices, routes)

        self.save_gpkg(
            gpd_paths,
            graph.cache_path.parent / "routes.gpkg"
            )

        return gpd_paths
    
    
    def get_mode_graph(self, modes, mode_name):
        
        mode = [m for m in modes if m.name == mode_name]
        
        if len(mode) == 0:
            raise ValueError(f"No {mode_name} mode in the model.")
        
        graph = mode[0].travel_costs.congested_path_graph 
        
        return graph
    
    
    def get_vertices(self, graph_folder, graph_hash):
        
        vertices_path = graph_folder.parent / (graph_hash + "-vertices.parquet")
        vertices = ( 
            pl.read_parquet(vertices_path)
            .with_row_index("vertex_index")
        )
        
        return vertices
        
    
    
    def convert_to_dataframe(self, routes: List):

        routes = [
            {
                "origin": r["origin"]["name"],
                "destination": r["destination"]["name"],
                "from_lon": r["origin"]["lon"],
                "from_lat": r["origin"]["lat"],
                "to_lon":  r["destination"]["lon"],
                "to_lat": r["destination"]["lat"]
            } 
            for r in routes
        ]
        
        routes = ( 
            pl.from_dicts(routes)
            .with_row_index("ref_index")
        )
        
        
        return routes
    
    
    
    def join_transport_zone_info(self, routes, transport_zones):
        """
        Assign origin and destination transport zone IDs to each route.
        
        Parameters
        ----------
        ref_costs : pl.DataFrame
            Reference travel cost DataFrame with coordinates.
        transport_zones : gpd.GeoDataFrame
            Transport zone polygons with an ID column.
        
        Returns
        -------
        pl.DataFrame
            Input DataFrame with added 'from' and 'to' zone identifiers.
        """
        
        transport_zones = transport_zones.get()
        
        origins = gpd.GeoDataFrame(
            routes.to_pandas()[["ref_index"]],
            geometry=gpd.points_from_xy(
                routes["from_lon"],
                routes["from_lat"]
            ),
            crs="EPSG:4326"
        )
        
        origins = origins.to_crs("EPSG:3035")
        
        destinations = gpd.GeoDataFrame(
            routes.to_pandas()[["ref_index"]],
            geometry=gpd.points_from_xy(
                routes["to_lon"],
                routes["to_lat"]
            ),
            crs="EPSG:4326"
        )
        
        destinations = destinations.to_crs("EPSG:3035")
        
        origins = gpd.sjoin(origins, transport_zones[["transport_zone_id", "x", "y", "geometry"]])
        destinations = gpd.sjoin(destinations, transport_zones[["transport_zone_id", "x", "y", "geometry"]])
        
        origins = ( 
            pl.DataFrame(origins.drop(["geometry", "index_right"], axis=1))
            .rename({"transport_zone_id": "from"})
        )
        
        destinations = ( 
            pl.DataFrame(destinations.drop(["geometry", "index_right"], axis=1))
            .rename({"transport_zone_id": "to"})
        )
        
        routes = (
            routes
            .join(origins, on="ref_index")
            .join(destinations, on="ref_index")
            .with_columns(
                distance=((pl.col("x") - pl.col("x_right")).pow(2) + (pl.col("y") - pl.col("y_right")).pow(2)).pow(0.5)
            )
            .with_columns(
                n_clusters=(1.0 + 4.0*pl.col("distance").truediv(1000.0*2.0).neg().exp()).cast(pl.Int32())
            )
            .select(["origin", "from", "destination", "to", "n_clusters"])
        )
        
        return routes
    
    
    def snap_routes_to_graph(self, routes, graph, vertices, transport_zones):
        
        buildings = self.snap_buildings_to_graph(transport_zones, graph, vertices)
        
        routes = (
            
            routes
            .join(buildings, left_on=["from", "n_clusters"], right_on=["transport_zone_id", "n_clusters"])
            .join(buildings, left_on=["to", "n_clusters"], right_on=["transport_zone_id", "n_clusters"], suffix="_to")
            .filter(
                pl.col("building_id") != pl.col("building_id_to")
            )
            
        )
        
        return routes
        
        
    def snap_buildings_to_graph(self, transport_zones, graph, vertices):

        buildings = self.get_buildings(transport_zones)
        buildings = self.find_nearest_vertex(buildings, vertices)
        
        return buildings
    
    
    def get_graph_folder_and_hash(self, graph):
        
        graph_file_token = graph.cache_path
        graph_file_folder = graph_file_token.parent
        graph_file_hash = graph_file_token.stem.split("-")[0]
        
        return graph_file_folder, graph_file_hash
    
    
    def get_buildings(self, transport_zones):
        
        transport_zones_path = transport_zones.cache_path
        transport_zones_hash = transport_zones_path.stem.split("-")[0]
        
        buildings_path = ( 
            transport_zones_path.parent / 
            (transport_zones_hash + "-transport_zones_buildings.parquet")
        )
        
        buildings = (
            pl.read_parquet(buildings_path)
            .with_row_index("building_id")
            .drop("weight")
        )
        
        return buildings
    
    
    def find_nearest_vertex(self, buildings, vertices):
        
        nn_model = ( 
            NearestNeighbors(
                n_neighbors=1
            )
            .fit(
                vertices.select(["x", "y"]).to_numpy()
            )
        )
        
        distances, indices = nn_model.kneighbors(buildings.select(["x", "y"]).to_numpy())
        
        buildings = (
            buildings
            .with_columns(
                vertex_index=pl.Series(indices.reshape(-1))
            )
            .join(vertices.select(["vertex_index", "vertex_id"]), on="vertex_index")
            .drop("vertex_index")
        )
        
        return buildings
        

    def run_cpprouting_get_path_pair(self, routes, graph):
        
        unique_ods = routes.select(["vertex_id", "vertex_id_to"]).unique()
        
        script = RScript(resources.files('mobility.transport_graphs').joinpath('get_path_pair.R'))
        output_path = graph.cache_path.parent / "path_pairs.parquet"

        script.run(
            args=[
                str(graph.cache_path),
                json.dumps(unique_ods["vertex_id"].to_list()),
                json.dumps(unique_ods["vertex_id_to"].to_list()),
                str(output_path)
            ]
        )
        
        paths = pl.read_parquet(output_path)
        
        return paths
    
    
    def get_graph_data(self, graph_folder, graph_hash):
        
        graph_data_path = graph_folder / (graph_hash + "data.parquet")
        graph_dict_path = graph_folder / (graph_hash + "dict.parquet")
        graph_attr_path = graph_folder / (graph_hash + "attrib.parquet")
        
        graph_dict = pl.read_parquet(graph_dict_path)
        graph_attr = pl.read_parquet(graph_attr_path)
        
        graph_data = ( 
            pl.read_parquet(graph_data_path)
            .with_row_index("edge_id")
            .rename({"dist": "time"})
            .with_columns(
                distance=graph_attr["aux"]
            )
            .with_columns(
                speed_kph=pl.col("distance")/pl.col("time")*3.6
            )
        )
        
        
        return graph_data, graph_dict
    
    
    def create_geopandas_paths(self, cpprouting_paths, graph_data, graph_dict, vertices, routes):
        
        paths = ( 
            
            # Format cppRouting result
            cpprouting_paths
            .rename({
                "from": "origin_vertex_id",
                "to": "destination_vertex_id",
                "node": "vertex_id"
            })
            .with_columns(
                prev_vertex_id=pl.col("vertex_id").shift(1).over(["origin_vertex_id", "destination_vertex_id"])
            )
            
            # Add travel time / distance information
            .join(graph_dict, left_on="prev_vertex_id", right_on="ref", how="left")
            .join(graph_dict, left_on="vertex_id", right_on="ref", how="left", suffix="_to")
            .join(graph_data.select(["from", "to", "time", "distance", "speed_kph"]), left_on=["id", "id_to"], right_on=["from", "to"])
            
            .select(["origin_vertex_id", "destination_vertex_id", "prev_vertex_id", "vertex_id", "time", "distance", "speed_kph"])
            .with_columns(
                path_section_id=pl.col("vertex_id").cum_count().over(["origin_vertex_id", "destination_vertex_id"])
            )
            
            .unpivot(index=["origin_vertex_id", "destination_vertex_id", "path_section_id", "time", "distance", "speed_kph"], value_name="vertex_id")
            
            # Get the coordinates of the points
            .join(vertices.select(["vertex_id", "x", "y"]), on="vertex_id")
            
            # Add route informations
            .join(
                ( 
                    routes.select([
                        "origin", "local_admin_unit_id", "from",
                        "destination", "local_admin_unit_id_to", "to",
                        "vertex_id", "vertex_id_to"
                    ])
                    .rename({
                        "vertex_id": "origin_vertex_id",
                        "vertex_id_to": "destination_vertex_id",
                    })
                ),
                on=["origin_vertex_id", "destination_vertex_id"]
            )
            

            .with_columns(
                path_id=pl.struct("origin_vertex_id", "destination_vertex_id", "path_section_id").rank("dense")-1
            )
            .sort(["origin_vertex_id", "destination_vertex_id", "path_section_id"])
            
        )
         
        geoms = linestrings(
            paths.select(["x", "y"]),
            indices=paths["path_id"]
        )
        
        gdf = gpd.GeoDataFrame(
            paths.filter(pl.col("variable") == "vertex_id").to_pandas(),
            geometry=geoms,
            crs="EPSG:3035"
        )
        
        return gdf
    
    
    def save_gpkg(self, gdf, path):
        
        gdf.to_file(
            path,
            layer="graph",
            driver="GPKG"
        )
        
        logging.info(f"Routes gpkg created in {path}")
        
        return None