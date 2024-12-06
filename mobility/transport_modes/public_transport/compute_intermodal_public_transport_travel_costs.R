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
# tz_file_path <- 'D:/data/mobility/projects/experiments/6250b72770c44a2e0776d242a7551226-transport_zones.gpkg'
# pt_graph_fp <- "D:/data/mobility/projects/experiments/public_transport_graph/simplified/d3f6b7a4cf173ef1ba0443d4d484495a-done"
# first_leg_graph_fp <- "D:/data/mobility/projects/experiments/path_graph_walk/simplified/b1f14aa283c795a32964a59ac040c3e7-done"
# last_leg_graph_fp <- "D:/data/mobility/projects/experiments/path_graph_walk/simplified/b1f14aa283c795a32964a59ac040c3e7-done"
# first_modal_shift <- '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "shift_time": 1.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
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

start_verts[, i := 1:.N]
mid_verts[, j := 1:.N]
last_verts[, k := 1:.N]


# Compute the travel time between clusters
info(logger, "Combining graphs...")

# Prepare OD pairs between all transport zones / representative buildings
travel_costs <- initialize_travel_costs(
  transport_zones,
  buildings_sample,
  start_verts,
  mid_verts,
  last_verts,
  first_modal_shift,
  last_modal_shift
)

# Map start and last graph vertices (= vertices of the road network) to the mid 
# graph vertices (= public transport stops)
tc_start_verts <- start_verts[vertex_id %in% unique(travel_costs$vertex_id_from)]
tc_last_verts <- last_verts[vertex_id %in% unique(travel_costs$vertex_id_to)]

mid_to_start_knn <- get.knnx(
  start_verts[, list(x, y)],
  mid_verts[, list(x, y)],
  k = 1
)

mid_verts[, nn_start_vertex_id := start_verts$vertex_id[mid_to_start_knn$nn.index]]

mid_to_last_knn <- get.knnx(
  last_verts[, list(x, y)],
  mid_verts[, list(x, y)],
  k = 1
)

mid_verts[, nn_last_vertex_id := last_verts$vertex_id[mid_to_last_knn$nn.index]]

mid_verts_sf <- sfheaders::sf_point(mid_verts, x = "x", y = "y", keep = TRUE)


# Map all start points to nearby public transport stops
# (within a radius based on crowfly distance, average speed and max travel time)
tc_start_verts_sf <- sfheaders::sf_point(tc_start_verts, x = "x", y = "y", keep = TRUE)
tc_start_verts_sf <- st_buffer(tc_start_verts_sf, dist = 1.5*1000.0*first_modal_shift$average_speed*first_modal_shift$max_travel_time)

first_leg <- st_intersects(tc_start_verts_sf, mid_verts_sf)

first_leg <- lapply(1:length(first_leg), function(u) {
  data.table(
    i = tc_start_verts_sf$i[u],
    j = mid_verts_sf$j[first_leg[[u]]]
  )
})

first_leg <- rbindlist(first_leg)
first_leg <- merge(first_leg, start_verts[, list(i, from = vertex_id)], by = "i")
first_leg <- merge(first_leg, mid_verts[, list(j, to = nn_start_vertex_id)], by = "j")

# Compute the time and distance 
first_leg$time <- get_distance_pair(
  start_graph,
  from = first_leg$from,
  to = first_leg$to
)

first_leg$distance <- get_distance_pair(
  start_graph,
  from = first_leg$from,
  to = first_leg$to,
  aggregate_aux = TRUE
)


# Map all last points to nearby public transport stops
tc_last_verts_sf <- sfheaders::sf_point(tc_last_verts, x = "x", y = "y", keep = TRUE)
tc_last_verts_sf <- st_buffer(tc_last_verts_sf, dist = 1.5*1000.0*last_modal_shift$average_speed*last_modal_shift$max_travel_time)

last_leg <- st_intersects(tc_last_verts_sf, mid_verts_sf)

last_leg <- lapply(1:length(last_leg), function(u) {
  data.table(
    j = mid_verts_sf$j[last_leg[[u]]],
    k = tc_last_verts_sf$k[u]
  )
})

last_leg <- rbindlist(last_leg)
last_leg <- merge(last_leg, last_verts[, list(k, to = vertex_id)], by = "k")
last_leg <- merge(last_leg, mid_verts[, list(j, from = nn_start_vertex_id)], by = "j")


last_leg$time <- get_distance_pair(
  last_graph,
  from = last_leg$from,
  to = last_leg$to
)

last_leg$distance <- get_distance_pair(
  last_graph,
  from = last_leg$from,
  to = last_leg$to,
  aggregate_aux = TRUE
)



# Combine the 3 graphs into one
first_leg[, from := paste0("s", from)]
first_leg[, to := paste0("s", to)]

last_leg[, from := paste0("l", from)]
last_leg[, to := paste0("l", to)]

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
  mid_verts[, list(vertex_id, from = nn_start_vertex_id)],
  by.x = "vertex_id_from",
  by.y = "vertex_id",
  sort = FALSE
)

mid_leg <- merge(
  mid_leg,
  mid_verts[, list(vertex_id, to = nn_last_vertex_id)],
  by.x = "vertex_id_from",
  by.y = "vertex_id",
  sort = FALSE
)

mid_leg[, from := paste0("s", from)]
mid_leg[, to := paste0("l", to)]
mid_leg[, distance := mid_graph$attrib$aux]


all_legs <- rbindlist(
  list(
    first_leg[, list(leg = 1, from, to, time, distance)],
    mid_leg[, list(leg = 2, from, to, time, distance)],
    last_leg[, list(leg = 3, from, to, time, distance)]
  )
)

graph <- makegraph(all_legs[, list(from, to, dist = time)])

graph$attrib <- list(
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


# Compute the travel costs
info(logger, "Computing travel costs...")

travel_costs[, vertex_id_from := paste0("s", vertex_id_from)]
travel_costs[, vertex_id_to := paste0("l", vertex_id_to)]
travel_costs <- travel_costs[vertex_id_from %in% graph$dict$ref & vertex_id_to %in% graph$dict$ref]

travel_costs$total_time <- get_distance_pair(
  graph,
  travel_costs$vertex_id_from,
  travel_costs$vertex_id_to
)

travel_costs <- travel_costs[!is.na(total_time)]

get_distance_pair_aux <- function(graph, from, to, aux_name) {
  
  graph$attrib$aux <- graph$attrib[[aux_name]]
  
  value <- get_distance_pair(
    graph,
    from = travel_costs$vertex_id_from,
    to = travel_costs$vertex_id_to,
    aggregate_aux = TRUE
  )
  
  return(value)
}


for (aux_name in c("start_time", "last_time", "start_distance", "mid_distance", "last_distance")) {
  
  info(logger, paste0("Computing auxiliary variable : ", aux_name))
  
  travel_costs[[aux_name]] <- get_distance_pair_aux(
    graph,
    from = travel_costs$vertex_id_from,
    to = travel_costs$vertex_id_to,
    aux_name = aux_name
  )
  
}

travel_costs$mid_time <- travel_costs$total_time - travel_costs$start_time - travel_costs$last_time



# Aggregate the result by transport zone
travel_costs[, prob := weight_from*weight_to]
travel_costs[, prob := prob/sum(prob), list(from, to)]

travel_costs <- travel_costs[,
  list(
    start_distance = weighted.mean(start_distance, prob)/1000,
    start_time = weighted.mean(start_time, prob)/3600,
    mid_distance = weighted.mean(mid_distance, prob)/1000,
    mid_time = weighted.mean(mid_time, prob)/3600,
    last_distance = weighted.mean(last_distance, prob)/1000,
    last_time = weighted.mean(last_time, prob)/3600
  ),
  by = list(from, to)
]


travel_costs[, mid_time := first_modal_shift$shift_time/60 + last_modal_shift$shift_time/60]


write_parquet(travel_costs, output_file_path)