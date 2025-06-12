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
# tz_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\9da6c9b51734ddd0278a650c3b00fe30-transport_zones.gpkg'
# intermodal_graph_fp <- "D:\\data\\mobility\\projects\\haut-doubs\\car_public_transport_walk_intermodal_transport_graph\\simplified\\6fd0213e2de92fa22327ffb4b8516529-done"
# first_modal_transfer <- '{"max_travel_time": 0.3333333333333333, "average_speed": 50.0, "transfer_time": 1.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}'
# last_modal_transfer <- '{"max_travel_time": 0.3333333333333333, "average_speed": 50.0, "transfer_time": 1.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}'
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


info(logger, "Contracting graphs travel costs...")

graph_c <- cpp_contract(intermodal_graph)


# Compute the travel costs
info(logger, "Computing travel costs between representative buildings in transport zones...")

od_pairs <- unique(travel_costs[distance > 0.0, list(vertex_id_from, vertex_id_to)])

od_pairs$perceived_time <- get_distance_pair(
  graph_c,
  od_pairs$vertex_id_from,
  od_pairs$vertex_id_to,
  algorithm = "NBA",
  constant = 0.1
)

od_pairs <- od_pairs[!is.na(perceived_time)]

modal_transfer_time <- 60*(first_modal_transfer$transfer_time + last_modal_transfer$transfer_time)
od_pairs[, perceived_time := perceived_time + modal_transfer_time]

od_pairs <- od_pairs[perceived_time < 3600.0*parameters[["max_perceived_time"]]]

info(logger, "Reconstructing detailed distances and travel times for each leg...")

paths <- get_path_pair(
  graph_c,
  from = od_pairs$vertex_id_from,
  to = od_pairs$vertex_id_to,
  algorithm = "NBA",
  constant = 0.1,
  long = TRUE
)

paths <- as.data.table(paths)
paths[, prev_node := shift(node, type = "lag", n = 1), by = list(from, to)]

attrib <- cbind(
  as.data.table(intermodal_graph$data)[, list(from, to, perceived_time = dist)],
  as.data.table(intermodal_graph$attrib)[, list(distance = start_distance + mid_distance + last_distance, real_time = real_time)]
)

attrib <- merge(
  attrib,
  as.data.table(intermodal_graph$dict),
  by.x = "from",
  by.y = "id"
)

attrib <- merge(
  attrib,
  as.data.table(intermodal_graph$dict),
  by.x = "to",
  by.y = "id",
  suffixes = c("_from", "_to")
)

paths <- merge(
  paths,
  attrib,
  by.x = c("prev_node", "node"),
  by.y = c("ref_from", "ref_to"),
  sort = FALSE
)

paths[, is_first_leg := grepl("s1", prev_node)]
paths[, is_last_leg := grepl("l2", node)]
paths[, is_mid_leg := !is_first_leg & !is_last_leg]

paths[, start_distance := ifelse(is_first_leg, distance, 0.0)]
paths[, last_distance := ifelse(is_last_leg, distance, 0.0)]
paths[, mid_distance := ifelse(is_mid_leg, distance, 0.0)]

paths[, start_real_time := ifelse(is_first_leg, real_time, 0.0)]
paths[, last_real_time := ifelse(last_distance, real_time, 0.0)]
paths[, mid_real_time := ifelse(is_mid_leg, real_time, 0.0)]

paths[, start_perceived_time := ifelse(is_first_leg, perceived_time, 0.0)]
paths[, last_perceived_time := ifelse(is_last_leg, perceived_time, 0.0)]
paths[, mid_perceived_time := ifelse(is_mid_leg, perceived_time, 0.0)]

paths <- paths[, 
               list(
                 start_distance = sum(start_distance),
                 mid_distance = sum(mid_distance),
                 last_distance = sum(last_distance),
                 start_real_time = sum(start_real_time),
                 mid_real_time = sum(mid_real_time),
                 last_real_time = sum(last_real_time),
                 start_perceived_time = sum(start_perceived_time),
                 mid_perceived_time = sum(mid_perceived_time),
                 last_perceived_time = sum(last_perceived_time)
               ),
               by = list(
                 vertex_id_from = from.x,
                 vertex_id_to = to.x
               )
]

# Aggregate the result by transport zone

info(logger, "Aggregating results at transport zone level...")

travel_costs <- merge(
  travel_costs,
  od_pairs[, list(vertex_id_from, vertex_id_to)],
  by = c("vertex_id_from", "vertex_id_to")
)

travel_costs <- merge(
  travel_costs,
  paths,
  by = c("vertex_id_from", "vertex_id_to")
)

travel_costs[, prob := weight_from*weight_to]
travel_costs[, prob := prob/sum(prob), list(from, to)]

travel_costs <- travel_costs[,
                             list(
                               start_distance = sum(start_distance * prob)/1000,
                               mid_distance = sum(mid_distance * prob)/1000,
                               last_distance = sum(last_distance * prob)/1000,
                               
                               start_real_time = sum(start_real_time * prob)/3600,
                               mid_real_time = sum(mid_real_time * prob)/3600,
                               last_real_time = sum(last_real_time * prob)/3600,
                               
                               start_perceived_time = sum(start_perceived_time * prob)/3600,
                               mid_perceived_time = sum(mid_perceived_time * prob)/3600,
                               last_perceived_time = sum(last_perceived_time * prob)/3600
                             ),
                             by = list(from, to)
]

info(logger, "Saving the result...")

write_parquet(travel_costs, output_file_path)