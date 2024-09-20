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

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_fp <- args[2]
osm_file_path <- args[3]
mode <- args[4]
output_file_path <- args[5]

# package_path <- "D:/dev/mobility_oss/mobility"
# tz_fp <- "D:/data/mobility/projects/study_area/d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg"
# osm_file_path <- "D:/data/mobility/projects/study_area/f95cfc68f5969d51995c677439de7053-highway-osm_data.osm"
# mode <- "walk"
# output_file_path <- "D:/data/mobility/projects/study_area/path_graph_walk/simplified/0fc1301e47e4f0bc1c161ab1d1097fb5-done"

buildings_sample_fp <- file.path(
  dirname(tz_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_fp)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_fp, quiet = TRUE)
transport_zones <- st_transform(transport_zones, 4326)
bbox <- st_bbox(transport_zones)

info(logger, "Parsing OSM data...")

osm_data <- osmdata_sc(q = opq(bbox), doc = osm_file_path)

info(logger, "Weighting network with dodgr...")

dodgr_cache_off()

# Map mobility modes to dodgr modes
modes <- list(
  walk = "foot",
  car = "motorcar",
  bicycle = "bicycle"
)

graph <- weight_streetnet(
  osm_data,
  wt_profile = modes[[mode]],
  turn_penalty = FALSE
)

graph <- graph[graph$component == 1, ]


info(logger, "Converting to cppRouting graph...")

edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted")])

vertices <- as.data.table(graph[, c(".vx0", ".vx0_x", ".vx0_y", ".vx1", ".vx1_x", ".vx1_y")])
vertices <- rbind(
  vertices[, list(vertex_id = .vx0, x = .vx0_x, y = .vx0_y)][!duplicated(vertex_id)],
  vertices[, list(vertex_id = .vx1, x = .vx1_x, y = .vx1_y)][!duplicated(vertex_id)]
)
vertices <- vertices[!duplicated(vertex_id)]


# Locate transport zones buildings on the graph
vertices_3035 <- sfheaders::sf_point(vertices, x = "x", y = "y", keep = TRUE)
st_crs(vertices_3035) <- 4326
vertices_3035 <- st_transform(vertices_3035, 3035)
vertices_3035 <- as.data.table(cbind(st_drop_geometry(vertices_3035), st_coordinates(vertices_3035)))
setnames(vertices_3035, c("vertex_id", "x", "y"))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

knn <- get.knnx(
  vertices_3035[, list(x, y)],
  buildings_sample[, list(x, y)],
  k = 1
)

vertices_to_keep <- vertices_3035$vertex_id[knn$nn.index]


# Create the cppRouting graph
cppr_graph <- makegraph(
  df = edges,
  directed = TRUE,
  aux = graph$d
)

# Simplify the graph and accumulate travel times along edges that can be collapsed
# (avoid dropping nodes that are close to the buildings that were selected in the transport zones cration process)
cppr_graph_simple <- cpp_simplify(
  cppr_graph,
  rm_loop = TRUE,
  iterate = TRUE,
  keep = vertices_to_keep
)

# Simplify the graph and accumulate distance along edges that can be collapsed
cppr_graph$data$dist <- cppr_graph$attrib$aux

cppr_graph_s_dist <- cpp_simplify(
  cppr_graph,
  rm_loop = TRUE,
  iterate = TRUE,
  keep = vertices_to_keep
)

cppr_graph_simple$attrib$aux <- cppr_graph_s_dist$data$dist

info(logger, "Saving cppRouting graph and vertices coordinates...")

save_cppr_graph(cppr_graph_simple, dirname(output_file_path))
write_parquet(vertices_3035, file.path(dirname(dirname(output_file_path)), "vertices.parquet"))

file.create(output_file_path)

