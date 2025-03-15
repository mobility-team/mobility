library(dodgr)
library(osmdata)
library(log4r)
library(sf)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\simplified\\e51422ec7c0fc716b4d4407b96362275-car-simplified-path-graph',
#   '[{"modifier_type": "border_crossing", "max_speed": 30.0, "time_penalty": 5.0, "borders": ["D:\\\\data\\\\mobility\\\\data\\\\osm\\\\5a2241d7b589441aaa0dd526ff9f6b76-osm_border.geojson", "D:\\\\data\\\\mobility\\\\data\\\\osm\\\\ea821fc280b095b2c724694daa7d66e6-osm_border.geojson", "D:\\\\data\\\\mobility\\\\data\\\\osm\\\\367d0c697a383c1bf3a7f47178a986e4-osm_border.geojson"]}]',
#   'D:/data/mobility/projects/haut-doubs/path_graph_car/modified/ad692099a65a929f0dc8a09251624a98-car-modified-path-graph'
# )

package_fp <- args[1]
cppr_graph_fp <- args[2]
speed_modifiers <- args[3]
output_fp <- args[4]

source(file.path(package_fp, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

speed_modifiers <- fromJSON(
  speed_modifiers,
  simplifyDataFrame = FALSE,
  simplifyVector = FALSE
)


# Load the cpprouting graph
hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))


apply_border_crossing_speed_modifier <- function(
    cppr_graph,
    modifier_type,
    max_speed,
    time_penalty,
    borders
) {
  
  info(logger, "Applying border crossing modifier...")
  
  # Create sf line segments for all graph edges
  data <- as.data.table(cppr_graph$data)
  dict <- as.data.table(cppr_graph$dict)
  edges <- merge(data, dict, by.x = "from", by.y = "id")
  edges <- merge(edges, dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
  edges <- merge(edges, vertices, by.x = "ref_from", by.y = "vertex_id")
  edges <- merge(edges, vertices, by.x = "ref_to", by.y = "vertex_id", suffixes = c("_from", "_to"))
  edges[, edge_id := 1:.N]
  
  edges <- rbindlist(
    list(
      edges[, list(from, to, edge_id, x = x_from, y = y_from)],
      edges[, list(from, to, edge_id, x = x_to, y = y_to)]
    )
  )
  
  edges_sf <- sfheaders::sf_linestring(
    edges[order(edge_id)],
    x = "x",
    y = "y",
    linestring_id = "edge_id",
    keep = TRUE
  )
  
  st_crs(edges_sf) <- 3035
  
  # Find the ones intersecting a border
  borders <- lapply(borders, st_read, quiet = TRUE)
  borders <- lapply(borders, "[", "admin_level")
  borders <- st_as_sf(rbindlist(borders))
  borders <- st_transform(borders, 3035)
  
  index <- st_intersects(edges_sf, borders)
  index <- lengths(index) > 0
  
  from_ids <- edges_sf$from[index]
  to_ids <- edges_sf$to[index]
  
  # Convert the max speed to m/s and the time penalty to s to match the graph units
  max_speed <- max_speed/3.6
  time_penalty <- time_penalty*60.0
  
  # Compute the speed on each edge
  # It can be confusing because cppRouting calls the edge cost "dist", so in our 
  # case dist is the travel time and aux is the travel distance.
  data[, speed := cppr_graph$attrib$aux/dist]
  
  # Cap the speed of the edges that go through a border
  data[, speed := ifelse(
    data$from %in% from_ids & data$to %in% to_ids,
    pmin(max_speed, speed),
    speed
  )]
  
  # Recompute the travel time and add the time penalty for those edges
  data[, dist := ifelse(
    data$from %in% from_ids & data$to %in% to_ids,
    cppr_graph$attrib$aux/speed + time_penalty,
    dist
  )]
  
  cppr_graph$data <- data[, list(from, to, dist)]
  
  return(cppr_graph)
  
}

apply_limited_speed_zones_modifier <- function(
    cppr_graph,
    modifier_type,
    zones_geometry_file_path,
    max_speed
) {
  
  if (is.na(zones_geometry_file_path)) {
    stop(paste0("No geometry file was provided, please provide a value for the zones_geometry_file_path argument."))
  }
  
  if (file.exists(zones_geometry_file_path) == FALSE) {
    stop(paste0("Speed modifier file '", zones_geometry_file_path, "' does not exist."))
  }
  
  info(logger, "Applying limited speed zones modifier...")
    
  speed_modifiers <- st_read(zones_geometry_file_path, quiet = TRUE)
  speed_modifiers <- st_transform(speed_modifiers, 3035)
  
  verts <- merge(
    vertices,
    as.data.table(cppr_graph$dict),
    by.x = "vertex_id",
    by.y = "ref"
  )
  
  verts_sf <- sfheaders::sf_point(verts, x = "x", y = "y", keep = TRUE)
  st_crs(verts_sf) <- 3035
  
  # Find which graph nodes are in the speed modifier 
  index <- unlist(st_intersects(speed_modifiers, verts_sf))
  mod_vertex_ids <- verts_sf$id[index]
  
  data <- as.data.table(cppr_graph$data)
  data[, speed := cppr_graph$attrib$aux/dist]
  
  max_speed <- max_speed/3.6
  
  data[, speed := pmin(max_speed, speed)]
  data[, dist := cppr_graph$attrib$aux/speed]
  
  cppr_graph$data <- data[, list(from, to, dist)]
  
  return(cppr_graph)
  
}


# If speed modifiers are provided, update the speed of the links in the graph
modifiers <- list(
  border_crossing = apply_border_crossing_speed_modifier,
  limited_speed_zones = apply_limited_speed_zones_modifier
)

for (sm in speed_modifiers) {
  cppr_graph <- do.call(
    what = modifiers[[sm[["modifier_type"]]]],
    args = c(list(cppr_graph), sm)
  )
}


# Save the graph
hash <- strsplit(basename(output_fp), "-")[[1]][1]
folder_path <- dirname(output_fp)
save_cppr_graph(cppr_graph, folder_path, hash)
write_parquet(vertices, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))
file.create(output_fp)

