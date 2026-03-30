import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

class GraphGPKGExporter:

    def export(self, graph):

        gpkg_fp = dict_fp = graph.cache_path.parent / (graph.inputs_hash + "-graph.gpkg")
        
        data_fp = graph.cache_path.parent / (graph.inputs_hash + "data.parquet")
        dict_fp = graph.cache_path.parent / (graph.inputs_hash + "dict.parquet")
        vertices_fp = graph.cache_path.parents[1] / (graph.inputs_hash + "-vertices.parquet")

        if data_fp.exists() and dict_fp.exists() and vertices_fp.exists():

            graph_data = pd.read_parquet(data_fp)
            graph_dict = pd.read_parquet(dict_fp)
            graph_vertices = pd.read_parquet(vertices_fp)

            graph_data = pd.merge(graph_data, graph_dict, left_on="from", right_on="id")
            graph_data = pd.merge(graph_data, graph_dict, left_on="to", right_on="id", suffixes=["_from", "_to"])

            graph_data = pd.merge(graph_data, graph_vertices, left_on="ref_from", right_on="vertex_id")
            graph_data = pd.merge(graph_data, graph_vertices, left_on="ref_to", right_on="vertex_id", suffixes=["_from", "_to"])

            gdf = gpd.GeoDataFrame(
                graph_data, 
                geometry=[
                    LineString([(x1, y1), (x2, y2)]) 
                    for x1, y1, x2, y2 in zip(
                        graph_data['x_from'],
                        graph_data['y_from'],
                        graph_data['x_to'],
                        graph_data['y_to']
                    )
                ],
                crs="EPSG:3035"
            )

            gdf.to_file(gpkg_fp, driver="GPKG")

        else:

            raise ValueError(
                """
                Cannot convert the graph, at least one of the data / dict / 
                vertices parquet files is missing"
                """
            )
        
        return gpkg_fp