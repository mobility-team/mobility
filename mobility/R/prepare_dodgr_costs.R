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

tz_file_path <- args[1]
graph_file_path <- args[2]
output_file_path <- args[3]

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)
transport_zones_crs <- st_crs(transport_zones)

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)


# Load cpprouting graph

duckdb_parquet_to_df <- function(con, path) {
  dbExecute(
    con,
    sprintf(
      "CREATE TABLE df AS SELECT * FROM '%s'",
      path
    )
  )
  df <- dbGetQuery(con, "SELECT * FROM df")
  dbExecute(con, "DROP TABLE df")
  return(df)
}


duckdb_parquet_to_vector <- function(con, path) {
  dbExecute(
    con,
    sprintf(
      "CREATE TABLE df AS SELECT * FROM '%s'",
      path
    )
  )
  df <- dbGetQuery(con, "SELECT * FROM df")
  dbExecute(con, "DROP TABLE df")
  return(df[[1]])
}


read_cpprouting_contracted_graph <- function(path) {
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  graph <- list()
  graph[["data"]] <- duckdb_parquet_to_df(con, file.path(path, "data.parquet"))
  graph[["coords"]] <- duckdb_parquet_to_df(con, file.path(path, "coords.parquet"))
  graph[["rank"]] <- duckdb_parquet_to_vector(con, file.path(path, "rank.parquet"))
  graph[["shortcuts"]] <- duckdb_parquet_to_df(con, file.path(path, "shortcuts.parquet"))
  graph[["nbnode"]] <- read_json(file.path(path, "nbnode.json"), simplifyVector = TRUE)
  graph[["dict"]] <- duckdb_parquet_to_df(con, file.path(path, "dict.parquet"))
  graph[["original"]] <- list(data = NULL, attrib = list(aux = NULL, cap = NULL, alpha = NULL, beta = NULL))
  graph[["original"]][["data"]] <- duckdb_parquet_to_df(con, file.path(path, "original_data.parquet"))
  graph[["original"]][["attrib"]][["aux"]] <- duckdb_parquet_to_vector(con, file.path(path, "original_data_attrib_aux.parquet"))
  dbDisconnect(con, shutdown = TRUE)
  return(graph)
}

graph <- read_cpprouting_contracted_graph(graph_file_path)


vertices <- graph$coords
graph$coords <- NULL



# Cluster network vertices inside each transport zone
info(logger, "Clustering network vertices...")

vertices_geo <- sfheaders::sf_point(vertices, x = "x", y = "y", keep = TRUE)
st_crs(vertices_geo) <- 4326
vertices_geo <- st_transform(vertices_geo, transport_zones_crs)

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
        vertices_density > 100,
        ceiling(vertices_density/100),
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

travel_times <- get_distance_matrix(
  graph,
  from = vertices_geo_cluster$id,
  to = vertices_geo_cluster$id,
  algorithm = "mch"
)


travel_times <- as.data.table(reshape2::melt(travel_times))
setnames(travel_times, c("from", "to", "value"))
travel_times[, from := as.character(from)]
travel_times[, to := as.character(to)]
travel_times[, value := value/3600]
travel_times <- travel_times[from != to]
travel_times <- travel_times[!is.na(value)]

setnames(travel_times, "value", "time")


# Compute the distances between clusters
info(logger, "Computing travel distances...")

travel_distances <- get_distance_matrix(
  graph,
  from = vertices_geo_cluster$id,
  to = vertices_geo_cluster$id,
  algorithm = "mch",
  aggregate_aux = TRUE
)

travel_distances <- as.data.table(reshape2::melt(travel_distances))
setnames(travel_distances, c("from", "to", "value"))
travel_distances[, from := as.character(from)]
travel_distances[, to := as.character(to)]
travel_distances[, value := value/1000]
travel_distances <- travel_distances[from != to]
travel_distances <- travel_distances[!is.na(value)]

setnames(travel_distances, "value", "distance")


# Estimate speed for the internal trips
short_distance_speed <- merge(
  travel_times[, .SD[which.min(time)], by = from],
  travel_distances,
  by = c("from", "to")
)

short_distance_speed[, speed := distance/time]
short_distance_speed[speed > 50.0, speed := 50.0]

internal_times <- merge(
  internal_distances,
  short_distance_speed[, list(from, speed)],
  by = "from"
)

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

travel_costs <- merge(
  travel_costs,
  vertices_geo_cluster[, list(id, transport_zone_id)],
  by.x = "from",
  by.y = "id"
)

travel_costs <- merge(
  travel_costs,
  vertices_geo_cluster[, list(id, transport_zone_id)],
  by.x = "to",
  by.y = "id"
)

travel_costs <- travel_costs[,
   list(
     distance = median(distance),
     time = median(time)
   ),
   list(transport_zone_id.x, transport_zone_id.y)
]

setnames(travel_costs, c("from", "to", "distance", "time"))

write_parquet(travel_costs, output_file_path)
