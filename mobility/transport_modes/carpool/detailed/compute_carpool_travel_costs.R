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

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_file_path <- args[2]
graph_file_path <- args[3]
parking_locations <- args[3]
output_file_path <- args[4]



package_path <- "D:/dev/mobility_oss/mobility"
tz_file_path <- "D:/data/mobility/projects/grand-geneve/c30098e5ececd6613a1e1d5261e0ba07-transport_zones.gpkg"
graph_file_path <- "D:/data/mobility/projects/grand-geneve/cppr_car"
parking_locations <- "[[46.147195, 6.096399]]"
# output_file_path <- args[3]


source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

# Load transport zones
transport_zones <- st_read(tz_file_path)
transport_zones_crs <- st_crs(transport_zones)

# Load cpprouting graph
cppr_data <- read_cpprouting(graph_file_path)
graph <- cppr_data[[1]]
vertices <- as.data.table(cppr_data[[2]])


# Cluster network vertices inside each transport zone
info(logger, "Clustering network vertices...")

vertices_geo <- sfheaders::sf_point(vertices, x = "x", y = "y", keep = TRUE)
st_crs(vertices_geo) <- 4326
vertices_geo <- st_transform(vertices_geo, st_crs(transport_zones))

transport_zones$area <- as.numeric(st_area(transport_zones))/1e6
vertices_geo <- st_join(vertices_geo, transport_zones)

vertices_geo <- cbind(as.data.table(vertices_geo)[, list(id = vertex_id, transport_zone_id, area)], st_coordinates(vertices_geo))
vertices_geo <- vertices_geo[!is.na(transport_zone_id)]
vertices_geo <- vertices_geo[!duplicated(vertices_geo[, list(X, Y)])]

# Cluster vertices so the density of vertices in each cluster does not exceed 0.5 points/km²
vertices_geo[, vertices_density := .N/area, by = transport_zone_id]

cluster_vertices <- function(transport_zone_id, X, Y, vertices_density, area, N) {
  kmeans(
    cbind(X, Y),
    centers = max(
      1,
      ifelse(
        vertices_density > 50,
        ceiling(vertices_density/50),
        1
      )
    )
  )$cluster
}

vertices_geo[,
   cluster := cluster_vertices(transport_zone_id, X, Y, vertices_density, area, .N),
   by = transport_zone_id
]

vertices_geo[, X_mean := mean(X), by = list(transport_zone_id, cluster)]
vertices_geo[, Y_mean := mean(Y), by = list(transport_zone_id, cluster)]
vertices_geo[, d_cluster_center := sqrt((X - X_mean)^2 + (Y - Y_mean)^2)]
vertices_geo[, cluster_center := d_cluster_center == min(d_cluster_center), by = list(transport_zone_id, cluster)]

vertices_geo_cluster <- vertices_geo[cluster_center == TRUE]

vertices_geo_cluster_zones <- sfheaders::sf_point(vertices_geo_cluster, x = "X", y = "Y", keep = TRUE)
st_crs(vertices_geo_cluster_zones) <- transport_zones_crs

v <- st_voronoi(do.call(c, st_geometry(vertices_geo_cluster_zones)))
v <- st_collection_extract(v)
st_crs(v) <- transport_zones_crs

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

v <- st_intersection(v, transport_zones_boundary)


# library(osmdata)
bbox <- st_bbox(transport_zones)

osm_file_path <- "D:/data/mobility/projects/grand-geneve/buildings.pbf"

buildings <- st_read(osm_file_path, layer = "multipolygons", query = "select osm_id from multipolygons")
buildings <- st_transform(buildings, 3035)
buildings$area <- as.numeric(st_area(buildings))
buildings <- buildings[buildings$area > 20, ]

buildings <- st_centroid(buildings)
buildings <- st_join(buildings, transport_zones[, "transport_zone_id"])

buildings_dt <- cbind(
  as.data.table(st_drop_geometry(buildings)),
  as.data.table(st_coordinates(buildings))
)
buildings_dt <- buildings_dt[!is.na(transport_zone_id)]



cluster_buildings <- function(transport_zone_id, X, Y, area) {
  kmeans(
    cbind(X, Y),
    centers = max(
      1,
      ifelse(
        sum(area) > 1e5,
        ceiling(sum(area)/1e5),
        1
      )
    )
  )$cluster
}

buildings_dt[,
   cluster := cluster_buildings(transport_zone_id, X, Y, area),
   by = transport_zone_id
]

buildings_dt[, X_mean := mean(X), by = list(transport_zone_id, cluster)]
buildings_dt[, Y_mean := mean(Y), by = list(transport_zone_id, cluster)]
buildings_dt[, d_cluster_center := sqrt((X - X_mean)^2 + (Y - Y_mean)^2)]
buildings_dt[, cluster_center := d_cluster_center == min(d_cluster_center), by = list(transport_zone_id, cluster)]

buildings_dt_cluster <- buildings_dt[cluster_center == TRUE]

plot(buildings_dt_cluster$X, buildings_dt_cluster$Y)


v <- sfheaders::sf_point(buildings_dt_cluster, x = "X", y = "Y", keep = TRUE)
st_crs(v) <- transport_zones_crs

v <- st_voronoi(do.call(c, st_geometry(v)))
v <- st_collection_extract(v)
st_crs(v) <- transport_zones_crs

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

v <- st_intersection(v, transport_zones_boundary)


# centroids <- cbind(as.data.table(st_coordinates(st_centroid(buildings))), buildings$area)
# setnames(centroids, c("x", "y", "area"))

osm_file_path <- "D:/data/mobility/projects/grand-geneve/lakes.pbf"

lakes <- st_read(osm_file_path, layer = "multipolygons", query = "select osm_id from multipolygons")
lakes <- st_transform(lakes, 3035)
lakes <- lakes[as.numeric(st_area(lakes)) > 100000, ]


v <- st_difference(v, st_union(lakes))



a <- 128*sqrt(st_area(v)/pi)/45/pi
summary(a)


available_graph_nodes <- graph$dict[graph$dict$id %in% graph$data$from | graph$dict$id %in% graph$data$to, "ref"]

library(FNN)

knn <- get.knnx(
  data = vertices_geo[id %in% available_graph_nodes, list(X, Y)],
  query = vertices_geo_cluster[, list(X, Y)],
  k = 1
)



to_node_id <- sample(vertices_geo_cluster$id, 1)
from_node_ids <- setdiff(vertices_geo_cluster$id, to_node_id)




vertices_geo[id %in% graph$dict$ref]


paths <- get_path_pair(graph, from_node_ids, rep(to_node_id, length(from_node_ids)), long = TRUE)
paths <- as.data.table(paths)

paths <- merge(paths, vertices, by.x = "node", by.y = "vertex_id", sort = FALSE)

paths[, next_node := shift(node, type = "lead", n = 1), by = list(from, to)]
paths[, sequence_index := 1:.N, by = list(from, to)]


costs <- as.data.table(graph$original$data)
setnames(costs, c("from", "to", "dist"), c("cppr_id_from", "cppr_id_to", "time"))
costs$distance <- graph$original$attrib$aux

costs <- merge(costs, graph$dict, by.x = "cppr_id_from", by.y = "id", sort = FALSE)
costs <- merge(costs, graph$dict, by.x = "cppr_id_to", by.y = "id", sort = FALSE, suffixes = c("_from", "_to"))

paths <- merge(
  paths,
  costs[, list(node = ref_from, next_node = ref_to, time, distance)],
  by = c("node", "next_node"),
  sort = FALSE
)

paths[, remaining_time := sum(time) - cumsum(time), by = list(from, to)]
paths[, remaining_distance := sum(distance) - cumsum(distance), by = list(from, to)]


carpool_nodes <- sample(graph$dict$ref, 500)

paths[, carpool_node := node %in% carpool_nodes]


paths_carpool <- paths[node == from | carpool_node == TRUE]

paths_carpool[, time_to_carpool := remaining_time[carpool_node == FALSE] - remaining_time, by = list(from, to)]
paths_carpool[, distance_to_carpool := remaining_distance[carpool_node == FALSE] - remaining_distance, by = list(from, to)]

ct <- 20.0
cd <- 0.1

cd_carpool <- 0.05
add_time_carpool <- 5*60
add_dist_carpool <- 1.0

paths_carpool[carpool_node == TRUE, utility := -(time_to_carpool + remaining_time + add_time_carpool)/3600*ct - (distance_to_carpool + add_dist_carpool)/1000*cd - remaining_distance/1000*cd_carpool]
paths_carpool[carpool_node == FALSE, utility := -remaining_time/3600*ct - remaining_distance/1000*cd]

paths_carpool[, p := exp(utility)/sum(exp(utility)), by = list(from, to)]


carpool_prob <- paths_carpool[, list(from, to, carpool = carpool_node, p)]


flows <- merge(paths[, list(from, to, node, next_node)], carpool_prob, by = c("from", "to"), allow.cartesian = TRUE)

flows <- flows[, list(volume = sum(p)), by = list(node, next_node, carpool)]


library(sfheaders)


vertices_subset <- vertices[vertex_id %in% paths$node]

vertices_sf <- sfheaders::sf_point(vertices_subset, x = "x", y = "y", keep = TRUE)
st_crs(vertices_sf) <- 4326

vertices_sf <- st_transform(vertices_sf, 3035)

vertices_dt <- cbind(vertices_sf$vertex_id, as.data.table(st_coordinates(vertices_sf)))
setnames(vertices_dt, c("node", "x", "y"))


flows <- merge(flows, vertices_dt, by = "node")
flows <- merge(flows, vertices_dt, by.x = "next_node", by.y = "node", suffixes = c("_from", "_to"))

flows

library(ggplot2)

p <- ggplot(flows)
p <- p + geom_segment(aes(x = x_from, y = y_from, xend = x_to, yend = y_to, linewidth = volume), lineend = "round")
p <- p + scale_linewidth_continuous(range = c(0, 5))
p <- p + geom_point(data = vertices_dt[node %in% carpool_nodes], aes(x = x, y = y), color = "red")
p <- p + facet_wrap(~carpool)
p <- p + theme_minimal()
p <- p + theme(legend.position = "none")
p <- p + coord_equal()
p

ggsave(plot = p, filename = "C:/Users/pouchaif/Desktop/carpool.svg", width = 12, height = 8)
