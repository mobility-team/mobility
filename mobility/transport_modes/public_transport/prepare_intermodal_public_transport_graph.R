library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
# library(readxl)
library(future.apply)
library(lubridate)
library(FNN)
library(cppRouting)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\grand-geneve\\9f060eb2ec610d2a3bdb3bd731e739c6-transport_zones.gpkg',
#   'D:\\data\\mobility\\projects\\grand-geneve\\public_transport_graph\\simplified\\4d58f32de6ef9c586aedacf9a5af0096-public-transport-graph',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_bicycle\\contracted\\a3001196ce680c9e218d05093418c1cd-bicycle-contracted-path-graph',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_walk\\contracted\\04c2f420aa4f610af7491b56aa785402-walk-contracted-path-graph',
#   '{"max_travel_time": 0.3333333333333333, "average_speed": 15.0, "transfer_time": 2.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}',
#   '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "transfer_time": 1.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}',
#   '',
#   '{"start_time_min": 6.5, "start_time_max": 8.0, "max_traveltime": 1.0, "wait_time_coeff": 2.0, "transfer_time_coeff": 2.0, "no_show_perceived_prob": 0.2, "target_time": 8.0, "max_wait_time_at_destination": 0.25, "max_perceived_time": 2.0, "additional_gtfs_files": [], "expected_agencies": null}',
#   'D:\\data\\mobility\\projects\\grand-geneve\\bicycle_public_transport_walk_intermodal_transport_graph\\simplified\\d313450666183d193cda1298f3b9f310-done'
# )

package_path <- args[1]
tz_file_path <- args[2]
pt_graph_fp <- args[3]
first_leg_graph_fp <- args[4]
last_leg_graph_fp <- args[5]
first_modal_shift <- args[6]
last_modal_shift <- args[7]
osm_parkings_fp <- args[8]
parameters <- args[9]
output_fp <- args[10]

first_modal_shift <- fromJSON(first_modal_shift)
last_modal_shift <- fromJSON(last_modal_shift)
parameters <- fromJSON(parameters)

buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "duplicate_cpprouting_graph.R"))
source(file.path(package_path, "r_utils", "initialize_travel_costs.R"))
source(file.path(package_path, "r_utils", "concatenate_graphs.R"))
source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))
source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Prepare parkings suitable for park + ride (if needed that is when the first leg mode is car)
if (osm_parkings_fp == "") {
  
  parkings <- NULL
  
} else {
  
  parkings <- st_read(osm_parkings_fp, layer = "multipolygons")
  
  # Keep only parkigns with more than 30 spots
  parkings <- st_transform(parkings, 3035)
  parkings <- parkings[as.numeric(st_area(parkings)) > 25.0*30.0, ]
  
  # Exclude parkings based on OSM tags
  # (keeping only park+ride parkings filters too much parkings)
  excluded_tags <- list(
    c("access", "no"),
    c("access", "employees"),
    c("access", "private"),
    c("access", "customers"),
    c("access", "destination"),
    c("access", "military"),
    c("access", "designated"),
    c("access", "delivery"),
    c("access", "hgv"),
    c("hgv", "designated"),
    c("park_ride", "no")
  )
  excluded_tags <- paste0(
    '"', sapply(excluded_tags, `[`, 1),
    '"=>"',
    sapply(excluded_tags, `[`, 2),
    '"',
    collapse = "|"
  )
  
  parkings <- parkings[!grepl(excluded_tags, parkings$other_tags), ]
  
  parkings <- st_geometry(st_centroid(parkings))
  
}


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
  
  n_incoming_edges <- data[, .N, by = to][N > 2]
  n_outgoing_edges <- data[, .N, by = from][N > 2]
  
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
st_crs(pt_start_verts_sf) <- 3035

access_mid_verts_sf <- sfheaders::sf_point(mid_verts[vertex_type == "access"], x = "x", y = "y", keep = TRUE)
st_crs(access_mid_verts_sf) <- 3035

# Keep only public transport stops that have at least a nearby parking available for park+ride
# Threshold is 800 m - 10 min walk
# (if the first leg mode is car)
if (!is.null(parkings)) {
  
  is_parking_nearby <- st_intersects(
    st_buffer(access_mid_verts_sf, 400),
    parkings
  )
  is_parking_nearby <- lengths(is_parking_nearby) > 0
  access_mid_verts_sf <- access_mid_verts_sf[is_parking_nearby, ]
  
}

first_leg <- st_intersects(pt_start_verts_sf, access_mid_verts_sf)

first_leg <- data.table(
  i = rep(seq_along(first_leg), lengths(first_leg)),
  j = unlist(first_leg)
)

first_leg <- merge(first_leg, pt_start_verts[, list(i = 1:.N, from = vertex_id)], by = "i")
first_leg <- merge(first_leg, mid_verts[vertex_type == "access", list(j = 1:.N, to = nn_start_vertex_id)], by = "j")

first_leg <- first_leg[from != to]
first_leg <- unique(first_leg[, list(from, to)])


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
last_leg <- unique(last_leg[, list(from, to)])

last_leg$time <- get_distance_pair(
  last_graph,
  from = last_leg$from,
  to = last_leg$to
)

last_leg <- last_leg[time < last_modal_shift$max_travel_time*3600.0]

last_leg$distance <- get_distance_pair(
  last_graph,
  from = last_leg$from,
  to = last_leg$to,
  aggregate_aux = TRUE
)

last_leg[, from := paste0("l1-", from)]
last_leg[, to := paste0("l2-", to)]

# Compute the max departure time from the last public transport stop by going 
# backward in time from the target time
# (ie if the target time is 8:00 and it takes 10 min to walk from the stop, 
# the user has to be at the station at maximum 7:50)
last_leg[, max_time_at_station := parameters[["target_time"]]*3600.0 - time]
last_leg[, max_time_at_station := round(max_time_at_station/60.0)*60.0]
last_leg[, from_with_time := paste0(from, "-", max_time_at_station)]


# Compute the arrival times to the last public transport stops of the journeys
hash <- strsplit(basename(pt_graph_fp), "-")[[1]][1]
arrival_times <- read_parquet(file.path(dirname(dirname(pt_graph_fp)), paste0(hash, "-delta-target-arrival.parquet")))
arrival_times[, arrival_stop_index := as.character(arrival_stop_index)]
arrival_times[, arrival_time := parameters[["target_time"]]*3600.0 - delta_target_arrival]
arrival_times[, arrival_time := round(arrival_time/60.0)*60.0]
arrival_times <- arrival_times[, list(vertex_id_from = arrival_stop_index, arrival_time)]


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
mid_leg[, real_time := mid_graph$attrib$real_time]

# Create one arrival -> exit edges per arrival time
exit_edges <- mid_leg[grepl("l1", to)]
other_edges <- mid_leg[!grepl("l1", to)]

exit_edges <- merge(exit_edges, arrival_times, by = "vertex_id_from")
exit_edges[, to_with_time := paste0(to, "-", arrival_time)]

# Combine arrival -> exit edges and exit -> final destination edges and create 
# edges between time expanded exit nodes to make it possible for the user to 
# wait once at destination
# (ie if a train arrives at 7:40, the target arrival time at destination is 8:00,
# and it takes 10 min to get to the destination, we simulate the wait at destination
# by making the user wait 10 min at the arrival stop)
wait_edges <- rbindlist(
  list(
    exit_edges[, list(edge_type = "inp", v_id = to, v_id_with_time = to_with_time, time = arrival_time)],
    last_leg[, list(edge_type = "out", v_id = from, v_id_with_time = from_with_time, time = max_time_at_station)]
  )
)

# Don't create edges for exits that do not have any last leg edge
# (this can happen if the destinations are too far way from the stops)
n_in_out <- wait_edges[, .N, by = list(v_id, edge_type)]
n_in_out <- dcast(n_in_out, v_id ~ edge_type, value.var = "N")
valid_v_ids <- n_in_out[!is.na(out), v_id]
wait_edges <- wait_edges[v_id %in% valid_v_ids]

# Remove arrivals that are so early that the user is guaranteed to wait at least 
# a certain amount of time to reach any of the destinations
wait_edges[, min_out_time := min(time[edge_type == "out"]), by = v_id]
wait_edges <- wait_edges[time > min_out_time - parameters[["max_wait_time_at_destination"]]*60.0]

# Remove duplicate exit nodes
wait_edges <- unique(wait_edges[, list(v_id, v_id_with_time, time)])

# Replace the exit nodes by the time expanded ones in the vertices table
time_exp_exit_verts <- wait_edges[, list(v_id, vertex_id = v_id_with_time)]
time_exp_exit_verts <- merge(
  time_exp_exit_verts,
  last_verts[, list(v_id = paste0("l1-", vertex_id), x, y)],
  by = "v_id"
)

mid_verts <- mid_verts[vertex_type != "exit"]


# Create the edges between the unique time expanded exit nodes
wait_edges <- wait_edges[order(v_id, time)]
wait_edges[, delta_time := time - shift(time, n = 1, type = "lag"), by = v_id]
wait_edges[, prev_v_id_with_time := shift(v_id_with_time, n = 1, type = "lag"), by = v_id]
wait_edges <- wait_edges[!is.na(delta_time), list(from = prev_v_id_with_time, to = v_id_with_time, time = delta_time)]

wait_edges[, real_time := time]
wait_edges[, perceived_time := time*parameters[["wait_time_coeff"]]]

# Redirect the arrival -> exit and the exit -> destination edges to the time 
# expanded exit nodes
exit_edges[, to := to_with_time]

# Rebuild the mid leg graph and add the wait edges
mid_leg <- rbindlist(
  list(
    other_edges[, list(from, to, perceived_time = time, real_time, distance)],
    exit_edges[, list(from, to, perceived_time = 0, real_time = 0, distance)],
    wait_edges[, list(from, to, perceived_time, real_time, distance = 0.0)]
  )
)

# Concatenate all graphs
all_legs <- rbindlist(
  list(
    first_leg[, list(leg = 1, from, to, perceived_time = time, real_time = time, distance)],
    mid_leg[, list(leg = 2, from, to, perceived_time, real_time, distance)],
    last_leg[, list(leg = 3, from = from_with_time, to, perceived_time = time, real_time = time, distance)]
  )
)

all_verts <- rbindlist(
  list(
    start_verts[, list(vertex_id = paste0("s1-", vertex_id), x, y)],
    start_verts[, list(vertex_id = paste0("s2-", vertex_id), x, y)],
    mid_verts[, list(vertex_id = paste0("m", vertex_id), x, y)],
    time_exp_exit_verts[, list(vertex_id, x, y)],
    last_verts[, list(vertex_id = paste0("l2-", vertex_id), x, y)]
  )
)

all_verts <- all_verts[vertex_id %in% all_legs$from | vertex_id %in% all_legs$to]
all_legs <- all_legs[from %in% all_verts$vertex_id & to %in% all_verts$vertex_id]

# Create the cpprouting graph
cppr_graph <- makegraph(
  all_legs[, list(from, to, dist = perceived_time)],
  coords = all_verts
)

cppr_graph$attrib <- list(
  aux = NULL,
  alpha = NULL,
  beta = NULL,
  cap = NULL,
  real_time = all_legs$real_time,
  start_distance = all_legs[, ifelse(leg == 1, distance, 0.0)],
  mid_distance = all_legs[, ifelse(leg == 2, distance, 0.0)],
  last_distance = all_legs[, ifelse(leg == 3, distance, 0.0)],
  start_time = all_legs[, ifelse(leg == 1, real_time, 0.0)],
  last_time = all_legs[, ifelse(leg == 3, real_time, 0.0)]
)

# Save the graph
hash <- strsplit(basename(output_fp), "-")[[1]][1]
save_cppr_graph(cppr_graph, dirname(output_fp), hash)
write_parquet(all_verts, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))

file.create(output_fp)


