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
intermodal_graph_fp <- args[3]
first_modal_transfer <- args[4]
last_modal_transfer <- args[5]
parameters <- args[6]
output_file_path <- args[7]

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg'
# intermodal_graph_fp <- "D:\\data\\mobility\\projects\\haut-doubs\\walk_public_transport_walk_intermodal_transport_graph\\simplified\\7c82ea92215d91bfd4683479c70f9986-done"
# first_modal_transfer <- '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "transfer_time": 1.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}'
# last_modal_transfer <- '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "transfer_time": 1.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}'
# parameters <- '{"max_perceived_time": 2.0}'
# output_file_path <- 'D:/data/mobility/projects/experiments/f65f378d4dc11f0929cf7f2aeaa2aaf5-public_transport_travel_costs.parquet'


first_modal_transfer <- fromJSON(first_modal_transfer)
last_modal_transfer <- fromJSON(last_modal_transfer)
parameters <- fromJSON(parameters)

buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

# source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))
# source(file.path(package_path, "r_utils", "duplicate_cpprouting_graph.R"))
# source(file.path(package_path, "r_utils", "initialize_travel_costs.R"))
# source(file.path(package_path, "r_utils", "concatenate_graphs.R"))
# source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))
source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)
transport_zones <- as.data.table(st_drop_geometry(transport_zones))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

hash <- strsplit(basename(intermodal_graph_fp), "-")[[1]][1]
intermodal_graph <- read_cppr_graph(dirname(intermodal_graph_fp), hash)
intermodal_verts <- as.data.table(read_parquet(file.path(dirname(dirname(intermodal_graph_fp)), paste0(hash, "-vertices.parquet"))))

# Compute crowfly distances between transport zones to compute the number of 
# points within the origin and destination zones that should be used
# (between 5 for )
travel_costs <- CJ(
  from = transport_zones$transport_zone_id,
  to = transport_zones$transport_zone_id
)

travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "from", by.y = "transport_zone_id")
travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))

travel_costs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
travel_costs <- travel_costs[distance < 80e3]

travel_costs[, n_clusters := round(1 + 4*exp(-distance/1000/2))]


travel_costs <- merge(
  travel_costs,
  buildings_sample[, list(transport_zone_id, n_clusters, building_id, x, y, weight)],
  by.x = c("from", "n_clusters"),
  by.y = c("transport_zone_id", "n_clusters"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

travel_costs <- merge(
  travel_costs,
  buildings_sample[, list(transport_zone_id, n_clusters, building_id, x, y, weight)],
  by.x = c("to", "n_clusters"),
  by.y = c("transport_zone_id", "n_clusters"),
  all.x = TRUE,
  suffixes = c("_from_cluster", "_to_cluster"),
  allow.cartesian = TRUE
)

travel_costs <- travel_costs[building_id_from_cluster != building_id_to_cluster]


# Match transport zones centroids with graph vertices
start_verts <- intermodal_verts[grepl("s1-", vertex_id), list(vertex_id, x, y)]

knn_start <- get.knnx(
  start_verts[, list(x, y)],
  buildings_sample[, list(x, y)],
  k = 1
)

buildings_sample[, vertex_id_from := start_verts$vertex_id[knn_start$nn.index]]


last_verts <- intermodal_verts[grepl("l2-", vertex_id), list(vertex_id, x, y)]

knn_last <- get.knnx(
  last_verts[, list(x, y)],
  buildings_sample[, list(x, y)],
  k = 1
)

buildings_sample[, vertex_id_to := last_verts$vertex_id[knn_last$nn.index]]


travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, weight_from = weight, vertex_id_from)], by.x = "building_id_from_cluster", by.y = "building_id")
travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, weight_to = weight, vertex_id_to)], by.x = "building_id_to_cluster", by.y = "building_id")




# Compute the travel costs
info(logger, "Computing travel costs...")

graph_c <- cpp_contract(intermodal_graph)

travel_costs$perceived_time <- get_distance_pair(
  graph_c,
  travel_costs$vertex_id_from,
  travel_costs$vertex_id_to
)

travel_costs <- travel_costs[!is.na(perceived_time)]

modal_transfer_time <- 60*(first_modal_transfer$transfer_time + last_modal_transfer$transfer_time)
travel_costs[, perceived_time := perceived_time + modal_transfer_time]

travel_costs <- travel_costs[perceived_time < 3600.0*parameters[["max_perceived_time"]]]


get_distance_pair_aux <- function(graph, from, to, aux_name) {
  
  graph$attrib$aux <- graph$attrib[[aux_name]]
  
  value <- get_distance_pair(
    graph,
    from = from,
    to = to,
    algorithm = "NBA",
    constant = 0.1,
    aggregate_aux = TRUE
  )
  
  return(value)
}


for (aux_name in c("real_time", "start_time", "last_time", "start_distance", "mid_distance", "last_distance")) {
  
  info(logger, paste0("Computing auxiliary variable : ", aux_name))
  
  travel_costs[, (aux_name) := get_distance_pair_aux(
    intermodal_graph,
    from = travel_costs$vertex_id_from,
    to = travel_costs$vertex_id_to,
    aux_name = aux_name
  )]
  
}


travel_costs[, mid_time := real_time - start_time - last_time]

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
    last_time = weighted.mean(last_time, prob)/3600,
    perceived_time = weighted.mean(perceived_time, prob)/3600
  ),
  by = list(from, to)
]

write_parquet(travel_costs, output_file_path)