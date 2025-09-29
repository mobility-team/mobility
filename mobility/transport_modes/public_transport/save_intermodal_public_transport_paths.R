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

package_path <- args[1]
tz_file_path <- args[2]
intermodal_graph_fp <- args[3]
first_modal_transfer <- args[4]
last_modal_transfer <- args[5]
from_ids <- args[6]
to_ids <- args[7]
output_file_path <- args[8]

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:/data/mobility/projects/haut-doubs/94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg'
# intermodal_graph_fp <- "D:/data/mobility/projects/haut-doubs/car_public_transport_walk_intermodal_transport_graph/simplified/aa1f3be279a76522333f0d5e37e850a8-done"
# first_modal_transfer <- '{"max_travel_time": 0.3333333333333333, "average_speed": 50.0, "transfer_time": 5.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}'
# last_modal_transfer <- '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "transfer_time": 1.0, "shortcuts_transfer_time": null, "shortcuts_locations": null}'
# from_ids <- 885
# to_ids <- 1034
# output_file_path <- 'D:/data/mobility/projects/haut-doubs/paths_2.gpkg'


first_modal_transfer <- fromJSON(first_modal_transfer)
last_modal_transfer <- fromJSON(last_modal_transfer)

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



xy <- travel_costs[
  from %in% from_ids &
    to %in% to_ids
]

paths <- get_multi_paths(
  intermodal_graph,
  xy$vertex_id_from,
  xy$vertex_id_to,
  long = TRUE
)

paths <- paths[!duplicated(paths), ]

paths <- as.data.table(paths)
paths <- merge(paths, intermodal_verts, by.x = "node", by.y = "vertex_id", sort = FALSE)

paths <- merge(
  paths,
  xy[, list(vertex_id_from, vertex_id_to, tz_id_from = from, tz_id_to = to)],
  by.x = c("from", "to"),
  by.y = c("vertex_id_from", "vertex_id_to")
)

paths[, index := 1:.N, by = list(from, to)]
paths[, od := .GRP, by = list(from, to)]
paths[, leg := substr(node, 1, 1)]

paths <- merge(paths, intermodal_graph$dict, by.x = "node", by.y = "ref", sort = FALSE)
paths[, previous_id := shift(id, type = "lag", n = 1), by = list(from, to)]

paths <- merge(
  paths,
  intermodal_graph$data,
  by.x = c("previous_id", "id"),
  by.y = c("from", "to"),
  all.x = TRUE,
  sort = FALSE
)

paths[, linestring_id := paste(from, to)]


paths <- sfheaders::sf_linestring(paths, x = "x", y = "y", keep = FALSE, linestring_id = "linestring_id")
st_crs(paths) <- 3035



st_write(paths, output_file_path, delete_dsn = TRUE)
