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
# tz_fp <- "D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg"
# osm_file_path <- "D:/data/mobility/projects/haut-doubs/75af783de17a27e29622afc2eeb1fc9a-highway-osm_data.osm"
# mode <- "car"
# output_file_path <- "D:/data/mobility/projects/haut-doubs/path_graph_car/simplified/9a6f4500ffbf148bfe6aa215a322e045-done"

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



# Compute road capacity
if (mode == "car") {
  
  edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted", "highway", "lanes")])
  
  edges[, lanes := as.numeric(lanes)]
  edges[is.na(lanes) | lanes == 0, lanes := 1]
  
  capacity_coeffs <- data.table(
    highway = c("primary", "secondary", "residential", "tertiary", 
                "unclassified", "service", "primary_link", "living_street", 
                "tertiary_link", "secondary_link"),
    alpha = c(0.15, 0.20, 0.30, 0.25, 0.25, 0.40, 0.15, 0.50, 0.25, 0.20),
    beta = c(4.0, 3.5, 2.5, 3.0, 3.0, 2.0, 4.0, 1.5, 3.0, 3.5)
  )
  
  edges <- merge(edges, capacity_coeffs, by = "highway", sort = FALSE)
  edges[, capacity := 1800*lanes]
  
} else {
  
  edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted")])
  
}


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
  df = edges[, list(.vx0, .vx1, time_weighted)],
  directed = TRUE,
  aux = graph$d,
  capacity = edges$capacity,
  alpha = edges$alpha,
  beta = edges$beta
)

cppr_graph$attrib$n_edges <- 1.0

# Simplify the graph and accumulate travel times along edges that can be collapsed
# (avoid dropping nodes that are close to the buildings that were selected in the transport zones cration process)
cppr_graph_simple <- cpp_simplify(
  cppr_graph,
  rm_loop = TRUE,
  iterate = TRUE,
  keep = vertices_to_keep
)

# Replace edges times costs by distance, capacity, alpha, beta to be able to 
# aggregate these when simplifying the network
aggregate_aux_data <- function(cppr_graph, dist_var) {
  
  cppr_graph$data$dist <- cppr_graph$attrib[[dist_var]]
  
  cppr_graph_s_dist <- cpp_simplify(
    cppr_graph,
    rm_loop = TRUE,
    iterate = TRUE,
    keep = vertices_to_keep
  )
  
  return(cppr_graph_s_dist$data$dist)
  
}


dist <- aggregate_aux_data(cppr_graph, "aux")
cppr_graph_simple$attrib$aux <- dist

if (mode == "car") {
  
  n_edges <- aggregate_aux_data(cppr_graph, "n_edges")
  cap <- aggregate_aux_data(cppr_graph, "cap")
  alpha <- aggregate_aux_data(cppr_graph, "alpha")
  beta <- aggregate_aux_data(cppr_graph, "beta")
  
  cppr_graph_simple$attrib$cap <- cap/n_edges
  cppr_graph_simple$attrib$alpha <- alpha/n_edges
  cppr_graph_simple$attrib$beta <- beta/n_edges
  
}

vertices_3035 <- vertices_3035[vertex_id %in% cppr_graph_simple$dict$ref]

info(logger, "Saving cppRouting graph and vertices coordinates...")

hash <- strsplit(basename(output_file_path), "-")[[1]][1]

save_cppr_graph(cppr_graph_simple, dirname(output_file_path), hash)
write_parquet(vertices_3035, file.path(dirname(dirname(output_file_path)), paste0(hash, "-vertices.parquet")))

file.create(output_file_path)

