library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(readxl)
library(future.apply)
library(lubridate)
library(FNN)
library(cppRouting)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_file_path <- args[2]
pt_graph_fp <- args[3]
first_leg_graph_fp <- args[4]
last_leg_graph_fp <- args[5]
first_modal_shift <- args[6]
last_modal_shift <- args[7]
output_file_path <- args[8]

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg'
# pt_graph_fp <- "D:/data/mobility/projects/haut-doubs/public_transport_graph/simplified/3d807c3ccc9ee020cbabb3e281f5635a-done"
# first_leg_graph_fp <- "D:/data/mobility/projects/haut-doubs/path_graph_car/contracted/60c7d5b22e428d3fbea41ac0ffadd067-done"
# last_leg_graph_fp <- "D:/data/mobility/projects/haut-doubs/path_graph_walk/contracted/3320481ff138926f18a6f45ced9d511e-done"
# first_modal_shift <- '{"max_travel_time": 0.3333333333333333, "average_speed": 50.0, "shift_time": 5.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
# last_modal_shift <- '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "shift_time": 1.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
# output_file_path <- 'D:/data/mobility/projects/experiments/f65f378d4dc11f0929cf7f2aeaa2aaf5-public_transport_travel_costs.parquet'


first_modal_shift <- fromJSON(first_modal_shift)
last_modal_shift <- fromJSON(last_modal_shift)

buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))
source(file.path(package_path, "r_utils", "duplicate_cpprouting_graph.R"))
source(file.path(package_path, "r_utils", "initialize_travel_costs.R"))
source(file.path(package_path, "r_utils", "concatenate_graphs.R"))
source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))
source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Create a three layer routing graph
# Layer 1 : original graph
# Layer 2 : destination graph
# Layer 3 : public transport "shortcuts" from the layer 1 to the layer 2

# This setup prevents the router from jumping back and forth between the first and last
# graphs and the public transport shortcuts (because the router can only go from
# layer 1 to 2 through layer 3, but cannot go back)

# Load cpprouting graphs and vertices
hash <- strsplit(basename(first_leg_graph_fp), "-")[[1]][1]
start_graph <- read_cppr_contracted_graph(dirname(first_leg_graph_fp), hash)
start_verts <- as.data.table(read_parquet(file.path(dirname(dirname(first_leg_graph_fp)), paste0(hash, "-vertices.parquet"))))

hash <- strsplit(basename(last_leg_graph_fp), "-")[[1]][1]
last_graph <- read_cppr_contracted_graph(dirname(last_leg_graph_fp), hash)
last_verts <- as.data.table(read_parquet(file.path(dirname(dirname(last_leg_graph_fp)), paste0(hash, "-vertices.parquet"))))

hash <- strsplit(basename(pt_graph_fp), "-")[[1]][1]
mid_graph <- read_cppr_graph(dirname(pt_graph_fp), hash)
mid_verts <- as.data.table(read_parquet(file.path(dirname(dirname(pt_graph_fp)), paste0(hash, "-vertices.parquet"))))

# Compute the travel time between clusters
info(logger, "Combining graphs...")

# Prepare OD pairs between all transport zones / representative buildings

# Snap the origins and destinations only to well connected nodes in the graph
# to avoid getting stuck in one way dead ends
get_well_connected_vertices_ids <- function(graph) {
  
  data <- as.data.table(graph$data)
  
  bidirectional_edges <- rbindlist(
    list(
      data[, list(from, to)],
      data[, list(from = to, to = from)]
    )
  )
  bidirectional_edges <- bidirectional_edges[, .N, by = list(from, to)][N > 1]
  
  n_incoming_edges <- data[, .N, by = to][N > 3]
  n_outgoing_edges <- data[, .N, by = from][N > 3]
  
  well_connected_edges <- data[from %in% n_outgoing_edges$from & to %in% n_incoming_edges$to]
  well_connected_edges <- merge(well_connected_edges, bidirectional_edges, by = c("from", "to"))
  
  dict <- as.data.table(graph$dict)
  
  wcids <- dict[id %in% well_connected_edges$from & id %in% well_connected_edges$to, ref]
  
  return(wcids)
  
}


start_well_con_ids <- get_well_connected_vertices_ids(start_graph)
last_well_con_ids <- get_well_connected_vertices_ids(last_graph)

start_verts <- start_verts[vertex_id %in% start_well_con_ids]
last_verts <- last_verts[vertex_id %in% last_well_con_ids]

travel_costs <- initialize_travel_costs(
  transport_zones,
  buildings_sample,
  start_verts,
  mid_verts,
  last_verts,
  first_modal_shift,
  last_modal_shift
)

# Filter out trips that are very long
travel_costs <- travel_costs[distance < 80e3]

# Map start and last graph vertices (= vertices of the road network) to the mid 
# graph vertices (= public transport stops)
mid_to_start_knn <- get.knnx(
  start_verts[, list(x, y)],
  mid_verts[vertex_type == "access", list(x, y)],
  k = 1
)

mid_verts[vertex_type == "access", nn_start_vertex_id := start_verts$vertex_id[mid_to_start_knn$nn.index]]

mid_to_last_knn <- get.knnx(
  last_verts[, list(x, y)],
  mid_verts[vertex_type == "exit", list(x, y)],
  k = 1
)

mid_verts[vertex_type == "exit", nn_last_vertex_id := last_verts$vertex_id[mid_to_last_knn$nn.index]]


# Map all start points to nearby public transport stops
# (within a radius based on crowfly distance, average speed and max travel time)
info(logger, "Computing the first leg travel times and distances...")

pt_start_verts <- start_verts[vertex_id %in% unique(travel_costs$vertex_id_from)]
pt_start_verts_sf <- sfheaders::sf_point(pt_start_verts, x = "x", y = "y", keep = TRUE)
pt_start_verts_sf <- st_buffer(pt_start_verts_sf, dist = 1.1*1000.0*first_modal_shift$average_speed*first_modal_shift$max_travel_time)

access_mid_verts_sf <- sfheaders::sf_point(mid_verts[vertex_type == "access"], x = "x", y = "y", keep = TRUE)

first_leg <- st_intersects(pt_start_verts_sf, access_mid_verts_sf)

first_leg <- data.table(
  i = rep(seq_along(first_leg), lengths(first_leg)),
  j = unlist(first_leg)
)

first_leg <- merge(first_leg, pt_start_verts[, list(i = 1:.N, from = vertex_id)], by = "i")
first_leg <- merge(first_leg, mid_verts[vertex_type == "access", list(j = 1:.N, to = nn_start_vertex_id)], by = "j")

first_leg <- first_leg[from != to]

# Limit the number of reachable stops from each origin by taking the N first 
# encountered stops in each angular sector around the origin point
delta_angle_sector <- 20
n_stops_per_sector <- 5

first_leg <- merge(first_leg, start_verts, by.x = "from", by.y = "vertex_id")
first_leg <- merge(first_leg, start_verts, by.x = "to", by.y = "vertex_id", suffixes = c("_from", "_to"))

first_leg[, angle := 180/pi*atan2(y_to - y_from, x_to - x_from)]
first_leg[, angle_bin := round(angle/delta_angle_sector)*delta_angle_sector]
first_leg[, distance := sqrt((x_to - x_from)^2 + (y_to - y_from)^2)]

first_leg <- first_leg[order(i, distance)]
first_leg <- first_leg[first_leg[, .I[1:min(.N, n_stops_per_sector)], by = .(i, angle_bin)]$V1]

# Remove duplicates (origin to stops that are located at the same spot)
first_leg <- first_leg[first_leg[, .I[1], by = list(from, to)]$V1, list(from, to)]

# Compute the time and distance 
first_leg$time <- get_distance_pair(
  start_graph,
  from = first_leg$from,
  to = first_leg$to
)

first_leg <- first_leg[time < first_modal_shift$max_travel_time*3600.0]

first_leg$distance <- get_distance_pair(
  start_graph,
  from = first_leg$from,
  to = first_leg$to,
  aggregate_aux = TRUE
)

first_leg[, from := paste0("s1-", from)]
first_leg[, to := paste0("s2-", to)]


# Map all last points to nearby public transport stops
info(logger, "Computing the last leg travel times and distances...")

pt_last_verts <- last_verts[vertex_id %in% unique(travel_costs$vertex_id_to)]

pt_last_verts_sf <- sfheaders::sf_point(pt_last_verts, x = "x", y = "y", keep = TRUE)
pt_last_verts_sf <- st_buffer(pt_last_verts_sf, dist = 1.1*1000.0*last_modal_shift$average_speed*last_modal_shift$max_travel_time)

exit_mid_verts_sf <- sfheaders::sf_point(mid_verts[vertex_type == "exit"], x = "x", y = "y", keep = TRUE)

last_leg <- st_intersects(exit_mid_verts_sf, pt_last_verts_sf)

last_leg <- data.table(
  i = rep(seq_along(last_leg), lengths(last_leg)),
  j = unlist(last_leg)
)

last_leg <- merge(last_leg, mid_verts[vertex_type == "exit", list(i = 1:.N, from = nn_last_vertex_id)], by = "i")
last_leg <- merge(last_leg, pt_last_verts[, list(j = 1:.N, to = vertex_id)], by = "j")


last_leg <- last_leg[from != to]

last_leg$time <- get_distance_pair(
  last_graph,
  from = last_leg$from,
  to = last_leg$to
)

first_leg <- first_leg[time < last_modal_shift$max_travel_time*3600.0]

last_leg$distance <- get_distance_pair(
  last_graph,
  from = last_leg$from,
  to = last_leg$to,
  aggregate_aux = TRUE
)

last_leg[, from := paste0("l1-", from)]
last_leg[, to := paste0("l2-", to)]

# Combine the 3 graphs into one
info(logger, "Combining the three routing graphs...")

mid_leg <- merge(
  as.data.table(mid_graph$data),
  as.data.table(mid_graph$dict)[, list(id, vertex_id_from = ref)],
  by.x = "from",
  by.y = "id",
  sort = FALSE
)

mid_leg <- merge(
  mid_leg,
  as.data.table(mid_graph$dict)[, list(id, vertex_id_to = ref)],
  by.x = "to",
  by.y = "id",
  sort = FALSE
)

mid_leg <- merge(
  mid_leg[, list(vertex_id_from, vertex_id_to, time = dist)],
  mid_verts[, list(vertex_id = as.character(vertex_id), from = nn_start_vertex_id)],
  by.x = "vertex_id_from",
  by.y = "vertex_id",
  all.x = TRUE,
  sort = FALSE
)

mid_leg <- merge(
  mid_leg,
  mid_verts[, list(vertex_id = as.character(vertex_id), to = nn_last_vertex_id)],
  by.x = "vertex_id_to",
  by.y = "vertex_id",
  all.x = TRUE,
  sort = FALSE
)

mid_leg[!is.na(from), from := paste0("s2-", from)]
mid_leg[!is.na(to), to := paste0("l1-", to)]
mid_leg[is.na(from), from := paste0("m", vertex_id_from)]
mid_leg[is.na(to), to := paste0("m", vertex_id_to)]

mid_leg[, distance := mid_graph$attrib$aux]


all_legs <- rbindlist(
  list(
    first_leg[, list(leg = 1, from, to, time, distance)],
    mid_leg[, list(leg = 2, from, to, time, distance)],
    last_leg[, list(leg = 3, from, to, time, distance)]
  )
)

all_verts <- rbindlist(
  list(
    start_verts[, list(vertex_id = paste0("s1-", vertex_id), x, y)],
    start_verts[, list(vertex_id = paste0("s2-", vertex_id), x, y)],
    mid_verts[, list(vertex_id = paste0("m", vertex_id), x, y)],
    last_verts[, list(vertex_id = paste0("l1-", vertex_id), x, y)],
    last_verts[, list(vertex_id = paste0("l2-", vertex_id), x, y)]
  )
)

all_verts <- all_verts[vertex_id %in% all_legs$from | vertex_id %in% all_legs$to]

cppr_graph <- makegraph(
  all_legs[, list(from, to, dist = time)],
  coords = all_verts
)

cppr_graph$attrib <- list(
  aux = NULL,
  alpha = NULL,
  beta = NULL,
  cap = NULL,
  start_distance = all_legs[, ifelse(leg == 1, distance, 0.0)],
  mid_distance = all_legs[, ifelse(leg == 2, distance, 0.0)],
  last_distance = all_legs[, ifelse(leg == 3, distance, 0.0)],
  start_time = all_legs[, ifelse(leg == 1, time, 0.0)],
  last_time = all_legs[, ifelse(leg == 3, time, 0.0)]
)

travel_costs[, vertex_id_from := paste0("s1-", vertex_id_from)]
travel_costs[, vertex_id_to := paste0("l2-", vertex_id_to)]


# Save the graph
hash <- strsplit(basename(output_fp), "-")[[1]][1]
save_cppr_contracted_graph(cppr_graph, dirname(output_fp), hash)
write_parquet(all_verts, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))

file.create(output_fp)

