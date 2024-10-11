library(dodgr)
library(log4r)
library(sfheaders)
library(nngeo)
library(data.table)
library(reshape2)
library(arrow)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(FNN)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_fp <- args[2]
graph_fp <- args[3]
max_speed <- as.numeric(args[4])
max_time <- as.numeric(args[5])
output_fp <- args[6]

# package_path <- "D:/dev/mobility_oss/mobility"
# tz_fp <- "D:\\data\\mobility\\projects\\study_area\\d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg"
# max_speed <- 80.0
# max_time <- 1.0
# graph_fp <- "D:\\data\\mobility\\projects\\study_area\\path_graph_walk\\contracted\\50afbe9bead045124493d2ad51a3904d-done"

buildings_sample_fp <- file.path(
  dirname(tz_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_fp)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_fp)
transport_zones <- as.data.table(st_drop_geometry(transport_zones))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Load cpprouting graph
hash <- strsplit(basename(graph_fp), "-")[[1]][1]
graph <- read_cppr_graph(dirname(graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(graph_fp)), paste0(hash, "-vertices.parquet")))

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
travel_costs[, time := distance/1000/max_speed]
travel_costs <- travel_costs[time < max_time]

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
knn <- get.knnx(
  vertices[, list(x, y)],
  buildings_sample[, list(x, y)],
  k = 1
)

buildings_sample[, vertex_id := vertices$vertex_id[knn$nn.index]]


travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, vertex_id)], by.x = "building_id_from_cluster", by.y = "building_id")
travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, vertex_id)], by.x = "building_id_to_cluster", by.y = "building_id", suffixes = c("_from", "_to"))



# Compute the distances between clusters
info(logger, "Computing travel distances...")

travel_costs$distance <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = TRUE
)

# Compute the travel time between clusters
info(logger, "Computing travel times...")

travel_costs$time <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = FALSE
)

travel_costs[, prob := weight_from_cluster*weight_to_cluster]
travel_costs[, prob := prob/sum(prob), list(from, to)]

travel_costs <- travel_costs[, list(
    distance = weighted.mean(distance, prob),
    time = weighted.mean(time, prob)
  ),
  by = list(from, to)
]


travel_costs[, distance := distance/1000]
travel_costs[, time := time/3600]

travel_costs <- travel_costs[, list(from, to, distance, time)]
setnames(travel_costs, c("from", "to", "distance", "time"))

write_parquet(travel_costs, output_fp)
