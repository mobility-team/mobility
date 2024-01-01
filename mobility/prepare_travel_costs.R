source("mobility/load_packages.R")
packages <- c("dodgr", "log4r", "optparse", "sf", "sfheaders", "nngeo", "data.table", "reshape2", "arrow")
load_packages(packages)

logger <- logger(appenders = console_appender())

option_list = list(
  make_option(c("-t", "--tz-file-path"), type = "character"),
  make_option(c("-g", "--dodgr-graph-file-path"), type = "character"),
  make_option(c("-o", "--output-file-path"), type = "character")
)

opt_parser = OptionParser(option_list = option_list)
opt = parse_args(opt_parser)


transport_zones <- st_read(opt[["tz-file-path"]])

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

graph <- readRDS(opt[["dodgr-graph-file-path"]])
  
graph$d_weighted <- graph$time_weighted
vertices <- dodgr_vertices(graph)

# Cluster network vertices inside each transport zone
info(logger, "Clustering network vertices...")

vertices_geo <- sfheaders::sf_point(vertices, x = "x", y = "y", keep = TRUE)
st_crs(vertices_geo) <- 4326
vertices_geo <- st_transform(vertices_geo, 2154)

transport_zones$area <- as.numeric(st_area(transport_zones))/1e6
vertices_geo <- st_join(vertices_geo, transport_zones)

vertices_geo <- cbind(as.data.table(vertices_geo)[, list(id, transport_zone_id, area)], st_coordinates(vertices_geo))
vertices_geo <- vertices_geo[!is.na(transport_zone_id)]
vertices_geo <- vertices_geo[!duplicated(vertices_geo[, list(X, Y)])]

# Cluster vertices so the density of vertices in each cluster does not exceed 0.05 points/km²
vertices_geo[, vertices_density := .N/area, by = transport_zone_id]

cluster_vertices <- function(transport_zone_id, X, Y, vertices_density, area, N) {
  kmeans(
    cbind(X, Y),
    centers = max(
      1,
      ifelse(
        vertices_density > 0.05,
        round(area*0.05),
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
st_crs(vertices_geo_cluster_zones) <- 2154

v <- st_voronoi(do.call(c, st_geometry(vertices_geo_cluster_zones)))
v <- st_collection_extract(v)
st_crs(v) <- 2154

v <- st_intersection(v, transport_zones_boundary)

# Compute a typical distance within each cluster
# hyp average distance between two points of a disc with the area of the cluster

vertices_geo_cluster$area <- as.numeric(st_area(v))
vertices_geo_cluster$d_internal <- 128*sqrt(vertices_geo_cluster$area/pi)/45/pi

internal_distances <- data.table(
  from = vertices_geo_cluster$id,
  to = vertices_geo_cluster$id,
  distance = vertices_geo_cluster$d_internal/1000
)


# Compute the travel time between clusters
info(logger, "Computing travel times...")

travel_times <- dodgr_times(
  graph = graph,
  from = vertices_geo_cluster$id,
  to = vertices_geo_cluster$id,
  shortest = FALSE
)

travel_times <- as.data.table(reshape2::melt(travel_times))
setnames(travel_times, c("from", "to", "value"))
travel_times[, from := as.character(from)]
travel_times[, to := as.character(to)]
travel_times[, value := value/3600]
travel_times <- travel_times[!duplicated(travel_times)]


# Problem for some routes (< 0.1% of routes) : dodgr returns NA times (because it find a route between points ?)
travel_times <- travel_times[!is.na(value)]

setnames(travel_times, "value", "time")


# Compute the distances between clusters
info(logger, "Computing travel distances...")


travel_distances <- dodgr_dists(
  graph = graph,
  from = vertices_geo_cluster$id,
  to = vertices_geo_cluster$id,
  shortest = FALSE
)

travel_distances <- as.data.table(reshape2::melt(travel_distances))
setnames(travel_distances, c("from", "to", "value"))
travel_distances[, from := as.character(from)]
travel_distances[, to := as.character(to)]
travel_distances[, value := value/1000]
travel_distances <- travel_distances[!duplicated(travel_distances)]


# Problem for some routes (< 0.1% of routes) : dodgr returns NA distances (because it find a route between points ?)
travel_distances <- travel_distances[!is.na(value)]

setnames(travel_distances, "value", "distance")


# Estimate speed for the internal trips
short_distance_speed <- merge(travel_distances, travel_times, by = c("from", "to"))
short_distance_speed <- short_distance_speed[, list(speed = median(distance[distance < quantile(distance, 0.1)]/time[distance < quantile(distance, 0.1)], na.rm = TRUE)), by = from]

internal_times <- merge(internal_distances, short_distance_speed, by = "from")
internal_times[, time := distance/speed]


# Add internal times and distances to the other trips
travel_distances <- rbindlist(
  list(
    travel_distances[from != to],
    internal_distances
  )
)

travel_times <- rbindlist(
  list(
    travel_times[from != to],
    internal_times[, list(from, to, time)]
  )
)

travel_costs <- merge(travel_times, travel_distances, by = c("from", "to"))

# Aggregate the travel costs between transport zones
info(logger, "Aggregating by transport zone...")

travel_costs <- merge(travel_costs, vertices_geo_cluster[, list(id, transport_zone_id)], by.x = "from", by.y = "id")
travel_costs <- merge(travel_costs, vertices_geo_cluster[, list(id, transport_zone_id)], by.x = "to", by.y = "id")

travel_costs <- travel_costs[,
   list(
     distance = median(distance),
     time = median(time)
   ),
   list(transport_zone_id.x, transport_zone_id.y)
]

setnames(travel_costs, c("from", "to", "distance", "time"))

write_parquet(travel_costs, opt[["output-file-path"]])
