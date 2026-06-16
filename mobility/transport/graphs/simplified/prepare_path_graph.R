library(dodgr)
library(osmdata)
library(log4r)
library(sf)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)
library(arrow)
library(FNN)
library(dplyr)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility\\mobility',
#   'd:\\data\\mobility\\projects\\grand-geneve\\c342828dcd0f0af0e9f15e00009ad911-transport_zones.gpkg',
#   'd:\\data\\mobility\\projects\\grand-geneve\\03748c70bbd44d8414efd32839aae124-highway-osm_data.osm',
#   'car',
#   '{"motorway": {"capacity": 2000.0, "alpha": 0.15, "beta": 4.0}, "trunk": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "primary": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "secondary": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "tertiary": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "unclassified": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "residential": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "living_street": {"capacity": 300.0, "alpha": 0.15, "beta": 4.0}, "motorway_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "trunk_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "primary_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "secondary_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "tertiary_link": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}}',
#   'd:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\simplified\\70a9e44cdf0262a6dda7c578b604b298-car-simplified-path-graph'
# )

package_path <- args[1]
tz_fp <- args[2]
osm_file_path <- args[3]
mode <- args[4]
osm_capacity_parameters <- args[5]
output_file_path <- args[6]

buildings_sample_fp <- file.path(
  dirname(tz_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_fp)),
    "-transport_zones_buildings.parquet"
  )
)

osm_capacity_parameters <- fromJSON(osm_capacity_parameters)
osm_capacity_parameters <- cbind(
  data.table(highway = names(osm_capacity_parameters)),
  rbindlist(osm_capacity_parameters)
)

source(file.path(package_path, "transport", "graphs", "core", "cpprouting_io.R"))
source(file.path(package_path, "transport", "graphs", "core", "project_od_nodes.R"))
source(file.path(package_path, "transport", "graphs", "simplified", "osm_streetnet.R"))
source(file.path(package_path, "transport", "graphs", "simplified", "car_edge_attributes.R"))
source(file.path(package_path, "transport", "graphs", "simplified", "path_graph_tables.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_fp, quiet = TRUE)
transport_zones <- st_transform(transport_zones, 4326)
bbox <- st_bbox(transport_zones)

info(logger, "Parsing OSM data...")

osm_data <- osmdata_sc(q = opq(bbox), doc = osm_file_path)
osm_data <- normalize_oneway_tags(osm_data)

info(logger, "Weighting network with dodgr...")

graph <- weight_path_streetnet(
  osm_data,
  mode,
  osm_capacity_parameters,
  output_file_path
)

if (mode == "car") {
  graph <- filter_car_streetnet_graph(graph)
}

info(logger, "Extracting edges and nodes...")

edges <- build_path_edges(
  graph,
  mode,
  osm_data,
  osm_capacity_parameters
)

vertices <- build_graph_vertices(graph)
vertices_3035 <- project_graph_vertices(vertices)

info(logger, "Creating cppRouting graph...")

cppr_graph <- build_cppr_path_graph(edges, mode)

info(logger, "Simplifying and pruning graph...")

cppr_graph_simple <- simplify_and_prune_cppr_graph(cppr_graph, mode)

vertices_3035 <- vertices_3035[vertex_id %in% cppr_graph_simple[["dict"]][["ref"]]]

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))

info(logger, "Projecting OD points onto the final graph...")

projection_result <- insert_projected_od_nodes(
  cppr_graph_simple,
  vertices_3035,
  buildings_sample,
  mode
)

cppr_graph_simple <- projection_result$graph
vertices_3035 <- projection_result$vertices
od_vertex_map <- projection_result$od_vertex_map

info(logger, "Saving cppRouting graph and vertices coordinates...")

hash <- strsplit(basename(output_file_path), "-")[[1]][1]
folder_path <- dirname(output_file_path)

save_cppr_graph(cppr_graph_simple, folder_path, hash)

write_parquet(
  vertices_3035,
  file.path(
    dirname(folder_path),
    paste0(hash, "-vertices.parquet")
  )
)

write_parquet(
  od_vertex_map,
  file.path(
    dirname(folder_path),
    paste0(hash, "-od-vertex-map.parquet")
  )
)

file.create(output_file_path)
