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

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_fp <- "D:/data/mobility/projects/experiments/6bd940d1b76b6128d0aa3840fc09df07-transport_zones.gpkgg"
# router_fp <- 'D:/data/mobility/projects/experiments/8a3e4753ed52c0dbbda6d3ff240fab31-gtfs_router.rds'
# start_time_min <- 6.5
# start_time_max <- 7.5

source(file.path(package_path, "r_utils", "cpprouting_io.R"))
logger <- logger(appenders = console_appender())

# Load the gtfsrouter object prepared by prepare_gtfs_router.R
router <- readRDS(router_fp)


# Prepare travel time between consecutive stops, and wait times between the 
# arrival and the departure of the vehicle at each stop
stop_times <- copy(router$stop_times)
stop_times <- stop_times[order(trip_id, stop_sequence)]

stop_times[, previous_stop_id := shift(stop_id, type = "lag", n = 1), by = trip_id]
stop_times[, previous_departure_time := shift(departure_time, type = "lag", n = 1), by = trip_id]

stop_times <- stop_times[!is.na(previous_stop_id)]

setnames(stop_times, c("stop_id", "previous_stop_id"), c("from_stop_id", "to_stop_id"))

stop_times <- stop_times[from_stop_id != to_stop_id]
stop_times <- stop_times[departure_time > start_time_min*3600 & departure_time < start_time_max*3600]

stop_times[, travel_time := as.double(arrival_time - previous_departure_time)]
stop_times[, wait_time := as.double(departure_time - arrival_time)]


# Identify uniquely stops (differentiating between routes)
stop_times <- merge(stop_times, router$trips[, list(trip_id, route_id)], by = "trip_id")

stops_routes <- rbindlist(
  list(
    stop_times[, list(gtfs_stop_id = from_stop_id, route_id)],
    stop_times[, list(gtfs_stop_id = to_stop_id, route_id)]
  )
)

stops_routes <- unique(stops_routes)

stops_routes <- rbindlist(
  list(
    stops_routes[, list(stop_type = "arrival", gtfs_stop_id, route_id)],
    stops_routes[, list(stop_type = "departure", gtfs_stop_id, route_id)]
  )
)

stops_routes[, stop_index := as.numeric(factor(paste(stop_type, route_id, gtfs_stop_id)))]

# Prepare the graph vertices coordinates
verts <- router$stops[, list(stop_id, stop_lon, stop_lat)]
verts <- verts[stop_id %in% unique(stops_routes$gtfs_stop_id)]

verts_sf <- sfheaders::sf_point(verts, x = "stop_lon", y = "stop_lat", keep = TRUE)
st_crs(verts_sf) <- 4326
verts_sf <- st_transform(verts_sf, 3035)
verts <- as.data.table(cbind(st_drop_geometry(verts_sf), st_coordinates(verts_sf)))
setnames(verts, c("vertex_id", "x", "y"))

# Group stops by spatial proximity (1 m bins)
verts[, x_bin := round(x)]
verts[, y_bin := round(y)]
verts[, stop_group_index := .GRP, by = list(x_bin, y_bin)]
verts[, access_stop_group_index := stop_group_index + max(stops_routes$stop_index)]
verts[, exit_stop_group_index := stop_group_index + max(verts$access_stop_group_index)]

verts_groups <- rbindlist(
  list(
    verts[, list(group_type = "access", x_bin = x_bin[1], y_bin = y_bin[1]), by = list(stop_group_index = access_stop_group_index)],
    verts[, list(group_type = "exit", x_bin = x_bin[1], y_bin = y_bin[1]), by = list(stop_group_index = exit_stop_group_index)]
  )
)


stops_routes <- merge(
  stops_routes,
  verts[, list(vertex_id, access_stop_group_index, exit_stop_group_index)],
  by.x = "gtfs_stop_id",
  by.y = "vertex_id"
)


stop_times <- merge(
  stop_times,
  stops_routes[stop_type == "departure", list(route_id, gtfs_stop_id, prev_dep_stop_index = stop_index)],
  by.x = c("route_id", "from_stop_id"),
  by.y = c("route_id", "gtfs_stop_id")
)

stop_times <- merge(
  stop_times,
  stops_routes[stop_type == "arrival", list(route_id, gtfs_stop_id, arrival_stop_index = stop_index)],
  by.x = c("route_id", "to_stop_id"),
  by.y = c("route_id", "gtfs_stop_id")
)

stop_times <- merge(
  stop_times,
  stops_routes[stop_type == "departure", list(route_id, gtfs_stop_id, next_dep_stop_index = stop_index)],
  by.x = c("route_id", "to_stop_id"),
  by.y = c("route_id", "gtfs_stop_id"),
)

stop_times <- stop_times[, list(
  prev_dep_stop_index,
  arrival_stop_index, next_dep_stop_index,
  arrival_time,
  departure_time,
  travel_time,
  wait_time
)]

# stop_times[trip_id == "11-33009"][order(stop_sequence)]


# Remove abnormal travel times (negative and very low, inf to 10 s)
# stop_times[, n_abnormal := sum(travel_time < 10.0), by = trip_id]
# stop_times <- stop_times[!(n_abnormal > 0.0)]

# Prepare the average travel time between consecutive stops
travel_times <- stop_times[,
  list(
    time = mean(travel_time)
  ),
  by = list(
    from = prev_dep_stop_index,
    to = arrival_stop_index
  )
]

# Prepare wait times at each stop by creating an edge between two virtual stops
# at each stop (arrival stop / departure stop)
stop_wait_times <- stop_times[,
  list(
    time = mean(wait_time)
  ),
  by = list(
    from = arrival_stop_index,
    to = next_dep_stop_index
  )
]

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

transfers <- merge(
  transfers,
  stops_routes[stop_type == "arrival"],
  by.x = "stop_id_from",
  by.y = "gtfs_stop_id"
)

transfers <- merge(
  transfers,
  stops_routes[stop_type == "departure"],
  by.x = "stop_id_to",
  by.y = "gtfs_stop_id",
  suffixes = c("_from", "_to"),
  allow.cartesian = TRUE
)

# Keep only transfers between different routes
transfers <- transfers[route_id_from != route_id_to]
transfers <- transfers[, list(from = stop_index_from, to = stop_index_to, transfer_time = time)]

# Add transfers between virtual access stops (groups of stops that are close)
# and actual stops. These access stops will be used as entry and exit points 
# for the public transport network.
access_times <- stops_routes[
  stop_type == "departure",
  list(
    from = access_stop_group_index,
    to = stop_index,
    time = 0.0
  )
]

exit_times <- stops_routes[
  stop_type == "arrival",
  list(
    from = stop_index,
    to = exit_stop_group_index,
    time = 0.0
  )
]


# For each arrival at a given route/stop, find what route/stops are accessible with a transfer
arrivals <- stop_times[, list(from = arrival_stop_index, arrival_time)]
arrivals <- merge(arrivals, transfers, by = "from", allow.cartesian = TRUE)
arrivals[, arrival_time_plus_transfer := arrival_time + transfer_time]
arrivals <- arrivals[order(arrival_time_plus_transfer)]

# For each from/to/arrival_time combination, find the next departures
transfer_times <- arrivals[,
  list(from, to, arrival_time_plus_transfer_cp = arrival_time_plus_transfer, arrival_time_plus_transfer) 
][
  stop_times[, list(to = next_dep_stop_index, departure_time_cp = departure_time, departure_time)],
  on = list(to, arrival_time_plus_transfer_cp < departure_time_cp),
  list(from, to, arrival_time_plus_transfer, departure_time),
  mult = "all",
  nomatch = 0
]

transfer_times[, transfer_time := departure_time - arrival_time_plus_transfer]

transfer_times <- transfer_times[,
    list(
      transfer_time = min(transfer_time)
    ),
    by = list(from, to, arrival_time_plus_transfer)
]


transfer_times <- transfer_times[,
  list(
    time = mean(transfer_time)
  ),
  by = list(from, to)
]


# Concatenate all times
times <- rbindlist(
  list(
    travel_times,
    stop_wait_times,
    transfer_times,
    access_times,
    exit_times
  )
)

# Estimate the distance between consecutive stops
distances <- copy(travel_times)

distances <- merge(distances, stops_routes[stop_type == "departure", list(stop_index, gtfs_stop_id)], by.x = "from", by.y = "stop_index", sort = FALSE)
distances <- merge(distances, stops_routes[stop_type == "arrival", list(stop_index, gtfs_stop_id)], by.x = "to", by.y = "stop_index", sort = FALSE, suffixes = c("_from", "_to"))

distances <- merge(distances, verts, by.x = "gtfs_stop_id_from", by.y = "vertex_id", sort = FALSE)
distances <- merge(distances, verts, by.x = "gtfs_stop_id_to", by.y = "vertex_id", suffixes = c("_from", "_to"), sort = FALSE)

distances[, distance := sqrt((x_to - x_from)^2 + (y_to - y_from)^2)]

distances <- c(
  distances$distance,
  rep(0.0, nrow(stop_wait_times)),
  rep(0.0, nrow(transfer_times)),
  rep(0.0, nrow(access_times)),
  rep(0.0, nrow(exit_times))
)

# Create and save the cpprouting graph
info(logger, "Saving cppRouting graph and vertices coordinates...")

graph <- makegraph(times, aux = distances)

stops_verts <- merge(
  stops_routes[, list(vertex_id = stop_index, vertex_type = stop_type, gtfs_stop_id)],
  verts[, list(gtfs_stop_id = vertex_id, x, y)],
  by = "gtfs_stop_id"
)

access_verts <- verts_groups[, list(vertex_id = stop_group_index, vertex_type = group_type, x = x_bin, y = y_bin)]

verts <- rbindlist(
  list(
    stops_verts[, list(vertex_id, vertex_type, x, y)],
    access_verts
  )
)

save_cppr_graph(graph, dirname(output_file_path))
write_parquet(verts, file.path(dirname(dirname(output_file_path)), "vertices.parquet"))

file.create(output_file_path)