library(log4r)
library(sf)
library(data.table)
library(arrow)
library(cppRoutingCCH)
library(FNN)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\dev\mobility_oss\mobility',
#   'D:/data/mobility/projects/grand-geneve/9f060eb2ec610d2a3bdb3bd731e739c6-transport_zones.gpkg',
#   'D:/data/mobility/projects/grand-geneve/path_graph_car/contracted/6e92ea1e35280a9d83e44d4215a99577-car-contracted-path-graph',
#   'ch',
#   '',
#   '60.0',
#   'D:\data\mobility\projects\grand-geneve\1b8a32aa54d7ce59db7f7a9f4c9da87e-travel_costs_free_flow_walk.parquet'
# )

package_path <- args[1]
tz_fp <- args[2]
graph_fp <- args[3]
backend <- args[4]
cch_graph_fp <- args[5]
max_beeline_distance <- as.numeric(args[6])
output_fp <- args[7]

buildings_sample_fp <- file.path(
  dirname(tz_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_fp)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "transport", "graphs", "core", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_fp)
transport_zones <- as.data.table(st_drop_geometry(transport_zones))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Load the routing graph selected by Python. Static costs use contracted CH;
# congestion-bound costs use current graph weights with reusable CCH.
routing <- read_cppr_path_routing_graph(graph_fp, backend, cch_graph_fp)
graph <- routing$graph
routing_graph <- routing$routing_graph
distance_values <- routing$distance_values

vertices_hash <- strsplit(basename(graph_fp), "-")[[1]][1]
vertices <- routing$vertices
od_vertex_map_fp <- file.path(dirname(dirname(graph_fp)), paste0(vertices_hash, "-od-vertex-map.parquet"))
od_vertex_map <- NULL
if (file.exists(od_vertex_map_fp)) {
  od_vertex_map <- as.data.table(read_parquet(od_vertex_map_fp))
}

# Compute crowfly distances between transport zones to choose how many
# representative buildings to use inside each origin and destination zone.
travel_costs <- CJ(
  from = transport_zones$transport_zone_id,
  to = transport_zones$transport_zone_id
)

travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "from", by.y = "transport_zone_id")
travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))

travel_costs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
travel_costs <- travel_costs[distance < max_beeline_distance * 1000]

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

# Match buildings with graph vertices. The OD vertex map is the normal path;
# nearest-neighbour fallback is kept for older graph caches without that map.
if (!is.null(od_vertex_map)) {
  buildings_sample <- merge(
    buildings_sample,
    od_vertex_map[, list(building_id, vertex_id)],
    by = "building_id",
    all.x = TRUE,
    sort = FALSE
  )

  if (any(is.na(buildings_sample$vertex_id))) {
      stop("OD vertex map is incomplete for travel-cost preparation.")
  }
} else {
  knn <- get.knnx(
    vertices[, list(x, y)],
    buildings_sample[, list(x, y)],
    k = 1
  )

  buildings_sample[, vertex_id := vertices$vertex_id[knn$nn.index]]
}

graph_refs <- as.character(graph$dict$ref)
buildings_sample[, vertex_id := as.character(vertex_id)]
if (any(!buildings_sample$vertex_id %in% graph_refs)) {
  stop("Some building vertex ids are missing from the graph dictionary.")
}

travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, vertex_id)], by.x = "building_id_from_cluster", by.y = "building_id")
travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, vertex_id)], by.x = "building_id_to_cluster", by.y = "building_id", suffixes = c("_from", "_to"))

info(logger, paste0("Computing travel times and distances with ", toupper(backend), "..."))

path_values <- get_path_values_pair(
  routing_graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  values = data.frame(distance = distance_values)
)

travel_costs$time <- path_values$cost
travel_costs$distance <- path_values$distance

dropped_rows <- travel_costs[is.na(time) | is.na(distance), .N]
if (dropped_rows > 0) {
  warning(sprintf(
    "Dropping %s travel-cost rows with NA time or distance before aggregation.",
    format(dropped_rows, big.mark = ",")
  ))
}

travel_costs <- travel_costs[!is.na(time) & !is.na(distance)]

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
