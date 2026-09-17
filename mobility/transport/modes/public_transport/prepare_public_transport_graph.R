library(log4r)
library(arrow)
library(data.table)
library(sf)
library(jsonlite)
library(cppRoutingCCH)
library(dbscan)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_file_path <- args[2]
gtfs_table_paths <- fromJSON(args[3])
parameters <- args[4]
output_file_path <- args[5]

source(file.path(package_path, "transport", "graphs", "core", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

parameters <- fromJSON(parameters)

info(logger, "Loading GTFS schedules and stops...")

# Read the dated timetable written by the Python GTFS asset.
router <- lapply(gtfs_table_paths, function(path) {
  as.data.table(read_parquet(path))
})

# Prepare travel time between consecutive stops, and wait times between the 
# arrival and the departure of the vehicle at each stop

stop_times <- router$stop_times[order(trip_id, stop_sequence)]

stop_times[, previous_stop_id := shift(stop_id, type = "lag", n = 1), by = trip_id]
stop_times[, previous_departure_time := shift(departure_time, type = "lag", n = 1), by = trip_id]

stop_times <- stop_times[!is.na(previous_stop_id)]

setnames(stop_times, c("previous_stop_id", "stop_id"), c("from_stop_id", "to_stop_id"))

stop_times <- stop_times[from_stop_id != to_stop_id]
stop_times <- stop_times[departure_time > parameters$start_time_min*3600 & departure_time < parameters$start_time_max*3600]

stop_times[, travel_time := as.double(arrival_time - previous_departure_time)]
stop_times[, wait_time := as.double(departure_time - arrival_time)]



stop_times <- merge(stop_times, router$trips[, list(trip_id, route_id)], by = "trip_id")



# Identify uniquely stops (differentiating between routes)
stops_routes <- rbindlist(
  list(
    stop_times[, list(gtfs_stop_id = from_stop_id, route_id)],
    stop_times[, list(gtfs_stop_id = to_stop_id, route_id)]
  )
)

stops_routes <- unique(stops_routes)

# Keep onboard travel through restricted stops, but only allow ordinary
# boarding/alighting where the selected timetable explicitly permits it.
permissions <- merge(
  router$stop_times[departure_time > parameters$start_time_min*3600 &
                    departure_time < parameters$start_time_max*3600],
  router$trips[, list(trip_id, route_id)], by = "trip_id"
)
permissions <- permissions[, list(can_board = any(pickup_type == 0), can_alight = any(drop_off_type == 0)),
                           by = list(route_id, gtfs_stop_id = stop_id)]

stops_routes <- rbindlist(
  list(
    stops_routes[, list(stop_type = "arrival", gtfs_stop_id, route_id)],
    stops_routes[, list(stop_type = "departure", gtfs_stop_id, route_id)]
  )
)
stops_routes <- merge(stops_routes, permissions, by = c("route_id", "gtfs_stop_id"))

stops_routes[, stop_index := as.numeric(factor(paste(stop_type, route_id, gtfs_stop_id)))]

# Prepare the graph vertices coordinates
verts <- router$stops[, list(stop_id, stop_lon, stop_lat)]
verts <- verts[stop_id %in% unique(stops_routes$gtfs_stop_id)]

verts_sf <- sfheaders::sf_point(verts, x = "stop_lon", y = "stop_lat", keep = TRUE)
st_crs(verts_sf) <- 4326
verts_sf <- st_transform(verts_sf, 3035)
verts <- as.data.table(cbind(st_drop_geometry(verts_sf), st_coordinates(verts_sf)))
setnames(verts, c("vertex_id", "x", "y"))

# Group stops by spatial proximity
verts[, stop_group_index := dbscan(verts[, list(x, y)], eps = 40.0, minPts = 1)$cluster]
verts[, x_bin := mean(x), by = stop_group_index]
verts[, y_bin := mean(y), by = stop_group_index]

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

# Add route types
stop_times <- merge(stop_times, router$routes[, list(route_id, vehicle_capacity)], by = "route_id")

stop_times <- stop_times[, list(
  prev_dep_stop_index,
  arrival_stop_index,
  next_dep_stop_index,
  arrival_time,
  departure_time,
  travel_time,
  wait_time,
  vehicle_capacity,
  can_board = pickup_type == 0,
  can_alight = drop_off_type == 0
)]

stop_times <- unique(stop_times)

stops_routes <- stops_routes[
  stop_index %in% stop_times$prev_dep_stop_index |
  stop_index %in% stop_times$arrival_stop_index |
  stop_index %in% stop_times$next_dep_stop_index
]

# Departure events must include each trip's first stop. The segment table above
# starts at the second stop and cannot supply all boarding or transfer times.
departures <- merge(
  router$stop_times[pickup_type == 0 & departure_time > parameters$start_time_min*3600 &
                    departure_time < parameters$start_time_max*3600,
                    list(trip_id, stop_id, departure_time)],
  router$trips[, list(trip_id, route_id)], by = "trip_id"
)
departures <- merge(
  departures,
  stops_routes[stop_type == "departure", list(route_id, stop_id = gtfs_stop_id, to = stop_index)],
  by = c("route_id", "stop_id")
)

info(logger, "Computing average travel times between stops...")

# Prepare the average travel time between consecutive stops
travel_times <- stop_times[,
  list(
    time = mean(travel_time),
    capacity = sum(vehicle_capacity)
  ),
  by = list(
    from = prev_dep_stop_index,
    to = arrival_stop_index
  )
]

info(logger, "Computing average waiting times at stops...")

# Prepare wait times at each stop by creating an edge between two virtual stops
# at each stop (arrival stop / departure stop)
stop_wait_times <- stop_times[,
  list(
    time = mean(wait_time),
    capacity = sum(vehicle_capacity)
  ),
  by = list(
    from = arrival_stop_index,
    to = next_dep_stop_index
  )
]

stop_wait_times[, perceived_time := time*parameters[["wait_time_coeff"]]]

info(logger, "Computing average transfer times between services and stops...")

# Prepare transfers between stops
transfers <- merge(
  router$transfers,
  stops_routes[stop_type == "arrival" & can_alight, list(gtfs_stop_id, stop_index, arrival_route_id = route_id)],
  by.x = "from_stop_id",
  by.y = "gtfs_stop_id",
  allow.cartesian = TRUE
)

transfers <- merge(
  transfers,
  stops_routes[stop_type == "departure" & can_board, list(gtfs_stop_id, stop_index, departure_route_id = route_id)],
  by.x = "to_stop_id",
  by.y = "gtfs_stop_id",
  suffixes = c("_from", "_to"),
  allow.cartesian = TRUE
)

# Apply the most specific matching rule before removing forbidden connections.
# Generated walking links have specificity -1 and cannot override explicit rules.
transfers <- transfers[(from_route_id == "" | from_route_id == arrival_route_id) &
                       (to_route_id == "" | to_route_id == departure_route_id)]
transfers <- transfers[, .SD[specificity == max(specificity)], by = list(stop_index_from, stop_index_to)]
transfers <- transfers[, list(forbidden = any(transfer_type == 3), min_transfer_time = max(min_transfer_time)),
                       by = list(from = stop_index_from, to = stop_index_to)]
transfers <- transfers[forbidden == FALSE]

# For each arrival at a given route/stop, find what route/stops are accessible with a transfer
arrivals <- stop_times[can_alight == TRUE, list(from = arrival_stop_index, arrival_time)]
arrivals <- merge(arrivals, transfers, by = "from", allow.cartesian = TRUE)
arrivals[, ready_time := arrival_time + min_transfer_time]

# Take the first departure the passenger can reach, including one leaving
# exactly when they are ready. Count the full time from arrival, so the
# minimum connection time is included as well as any subsequent waiting.
transfer_times <- departures[order(departure_time)][
  unique(arrivals[, list(from, to, arrival_time, ready_time)]),
  on = list(to, departure_time >= ready_time),
  list(from = i.from, to = i.to, time = x.departure_time - i.arrival_time),
  mult = "first",
  nomatch = 0
]

transfer_times <- transfer_times[,
   list(
     time = mean(time)
   ),
   by = list(from, to)
]

transfer_times[, perceived_time := time*parameters[["transfer_time_coeff"]]]

# Remove connections whose average total transfer time is 20 minutes or more.
transfer_times <- transfer_times[time < 20.0*60.0]


# Add transfers between virtual access stops (groups of stops that are close)
# and actual stops. These access stops will be used as entry and exit points 
# for the public transport network.
info(logger, "Adding virtual access and exit nodes to all stops and services accessible at each location...")

headway_times <- departures[order(departure_time)][, list(average_headway = mean(diff(departure_time))), by = to]

access_times <- stops_routes[
  stop_type == "departure" & can_board & stop_index %in% departures$to,
  list(
    from = access_stop_group_index,
    to = stop_index
  )
]

access_times <- merge(
  access_times,
  headway_times,
  by = "to",
  all.x = TRUE
)

# Wait time at the start of the journey is at most 5 min
# or half the average headway if it leads to a wait time of less than 5 min
access_times[, time := ifelse(
  is.na(average_headway),
  5.0*60.0,
  pmin(5.0*60.0, 0.5*average_headway)
)]

# Add a perceived risk of waiting for the next departure in case the first 
# one does not show up or the user misses it
# (taking one hour of headway when it is not known)
access_times[, perceived_time := ifelse(
  is.na(average_headway),
  time + parameters[["no_show_perceived_prob"]]*60.0*60.0,
  time + parameters[["no_show_perceived_prob"]]*average_headway
)]


exit_times <- stops_routes[
  stop_type == "arrival" & can_alight,
  list(
    from = stop_index,
    to = exit_stop_group_index,
    time = 0.0
  )
]


# Compute the minimum time difference between the target arrival time at destination
# and the service arrival times
stop_times[, delta_target_arrival := parameters[["target_time"]]*3600.0 - arrival_time]
stop_times[delta_target_arrival < 0.0, delta_target_arrival := 0.0]

target_time_diff <- stop_times[delta_target_arrival > 0.0, list(arrival_stop_index, delta_target_arrival)]

# Estimate the distance between consecutive stops
info(logger, "Estimating distances between stops...")

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
info(logger, "Building cpprouting graph...")

# Concatenate all times
perceived_times <- rbindlist(
  list(
    travel_times[, list(from, to, time)],
    stop_wait_times[, list(from, to, time = perceived_time)],
    transfer_times[, list(from, to, time = perceived_time)],
    access_times[, list(from, to, time = perceived_time)],
    exit_times[, list(from, to, time = 0.0)]
  )
)

real_time <- c(
  travel_times$time,
  stop_wait_times$time,
  transfer_times$time,
  access_times$time,
  exit_times$time
) 

capacity <- c(
  travel_times$capacity,
  stop_wait_times$capacity,
  rep(1.0e9, nrow(transfer_times)),
  rep(1.0e9, nrow(access_times)),
  rep(1.0e9, nrow(exit_times))
)

alpha <- c(
  rep(0.1, nrow(travel_times)),
  rep(0.1, nrow(stop_wait_times)),
  rep(0.1, nrow(transfer_times)),
  rep(0.1, nrow(access_times)),
  rep(0.1, nrow(exit_times))
)

beta <- c(
  rep(1.0, nrow(travel_times)),
  rep(1.0, nrow(stop_wait_times)),
  rep(1.0, nrow(transfer_times)),
  rep(1.0, nrow(access_times)),
  rep(1.0, nrow(exit_times))
)


graph <- makegraph(
  perceived_times,
  aux = distances,
  capacity = capacity,
  alpha = alpha,
  beta = beta
)

graph$attrib$real_time <- real_time

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

verts <- verts[vertex_id %in% unique(perceived_times$from) | vertex_id %in% unique(perceived_times$to)]

info(logger, "Saving cppRouting graph and vertices coordinates...")

hash <- strsplit(basename(output_file_path), "-")[[1]][1]

save_cppr_graph(graph, dirname(output_file_path), hash)
write_parquet(verts, file.path(dirname(dirname(output_file_path)), paste0(hash, "-vertices.parquet")))
write_parquet(target_time_diff, file.path(dirname(dirname(output_file_path)), paste0(hash, "-delta-target-arrival.parquet")))

file.create(output_file_path)
