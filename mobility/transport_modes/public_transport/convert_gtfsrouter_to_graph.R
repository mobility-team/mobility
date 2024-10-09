library(log4r)
library(gtfsrouter)
library(data.table)
library(sf)
library(cppRouting)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_fp <- args[2]
router_fp <- args[3]
start_time_min <- as.numeric(args[4])
start_time_max <- as.numeric(args[5])
output_file_path <- args[6]

package_path <- 'D:/dev/mobility_oss/mobility'
tz_fp <- "D:/data/mobility/projects/haut-doubs/94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg"
router_fp <- "D:/data/mobility/projects/haut-doubs/0c828f5d2dc7774ee06699b2812eb93a-gtfs_router.rds"
start_time_min <- 6.5
start_time_max <- 7.5

source(file.path(package_path, "r_utils", "cpprouting_io.R"))
logger <- logger(appenders = console_appender())

# Load the gtfsrouter object prepared by prepare_gtfs_router.R
router <- readRDS(router_fp)

# Prepare the graph vertices coordinates
verts <- router$stops[, list(stop_id, stop_lon, stop_lat)]
verts_sf <- sfheaders::sf_point(verts, x = "stop_lon", y = "stop_lat", keep = TRUE)
st_crs(verts_sf) <- 4326
verts_sf <- st_transform(verts_sf, 3035)
verts <- as.data.table(cbind(st_drop_geometry(verts_sf), st_coordinates(verts_sf)))
setnames(verts, c("vertex_id", "x", "y"))

verts <- rbindlist(
  list(
    verts[, list(vertex_id = paste0("dep-", vertex_id), x, y)],
    verts[, list(vertex_id = paste0("arr-", vertex_id), x, y)]
  )
)

# Prepare travel time between consecutive stops, and wait times between the 
# arrival and the departure of the vehicle at each stop
stop_times <- router$stop_times[, list(
  trip_id,
  from_stop_id = previous_stop_id,
  previous_stop_id,
  to_stop_id = stop_id,
  arrival_time,
  departure_time,
  previous_departure_time,
  arrival_time_bin = round(arrival_time/(5*60))*(5*60),
  departure_time_bin = round(departure_time/(5*60))*(5*60),
  previous_departure_time_bin = round(previous_departure_time/(5*60))*(5*60)
)]

stop_times <- stop_times[from_stop_id != to_stop_id]
stop_times <- stop_times[departure_time > start_time_min*3600 & departure_time < start_time_max*3600]

stop_times[, travel_time := as.double(arrival_time - previous_departure_time)]
stop_times[, stop_wait_time := as.double(departure_time - arrival_time)]

# Filter out stops outside the transport zones
transport_zones <- st_read(tz_fp, quiet = TRUE)
intersects <- st_intersects(verts_sf, transport_zones)
intersects <- lengths(intersects) > 0
stop_ids <- verts_sf$stop_id[intersects]

stop_times <- stop_times[from_stop_id %in% stop_ids & to_stop_id %in% stop_ids]

# Remove abnormal travel times (negative and verly low, inf to 10 s)
stop_times[, n_abnormal := sum(travel_time < 10.0), by = trip_id]
stop_times <- stop_times[!(n_abnormal > 0.0)]

# Create time aware stop ids
stop_times[, from_stop_id := paste0("dep-", from_stop_id, "-", previous_departure_time_bin)]
stop_times[, to_stop_id := paste0("arr-", to_stop_id, "-", arrival_time_bin)]

# Prepare the average travel time between consecutive stops
travel_times <- stop_times[, list(time = mean(travel_time)), by = list(from_stop_id, to_stop_id)]

# Prepare wait times at each stop by creating an edge between two virtual stops
# at each stop (arrival stop / departure stop)
stop_wait_times <- stop_times[stop_wait_time >= 0.0, list(time = mean(stop_wait_time)), by = list(from_stop_id, to_stop_id)]

# Prepare transfers between stops
transfers <- merge(
  router$transfers[, list(from_stop_id, to_stop_id, time = min_transfer_time)],
  data.table(index = 1:nrow(router$stop_ids), stop_id = router$stop_ids$stop_ids),
  by.x = "from_stop_id",
  by.y = "index"
)

transfers <- merge(
  transfers,
  data.table(index = 1:nrow(router$stop_ids), stop_id = router$stop_ids$stop_ids),
  by.x = "to_stop_id",
  by.y = "index",
  suffixes = c("_from", "_to")
)

transfers <- transfers[, list(
  from_stop_id = paste0("arr-", stop_id_from),
  to_stop_id = paste0("dep-", stop_id_to),
  time
)]

# Concatenate all times
times <- rbindlist(
  list(
    travel_times,
    stop_wait_times,
    transfers
  )
)

# Estimate the distance between consecutive stops
distances <- copy(travel_times)
distances <- merge(distances, verts, by.x = "from_stop_id", by.y = "vertex_id", sort = FALSE)
distances <- merge(distances, verts, by.x = "to_stop_id", by.y = "vertex_id", suffixes = c("_from", "_to"), sort = FALSE)
distances[, distance := sqrt((x_to - x_from)^2 + (y_to - y_from)^2)]

distances <- c(
  distances$distance,
  rep(0.0, nrow(stop_wait_times)),
  rep(0.0, nrow(transfers))
)

# Filter out vertices that do not have edges
verts <- verts[vertex_id %in% unique(c(times$from_stop_id, times$to_stop_id))]

# Create and save the cpprouting graph
info(logger, "Saving cppRouting graph and vertices coordinates...")

graph <- makegraph(times, aux = distances)
save_cppr_graph(graph, dirname(output_file_path))
write_parquet(verts, file.path(dirname(dirname(output_file_path)), "vertices.parquet"))

file.create(output_file_path)