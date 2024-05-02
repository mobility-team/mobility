library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(readxl)

args <- commandArgs(trailingOnly = TRUE)

tz_file_path <- args[1]
gtfs_file_path <- args[2]
gtfs_route_types_path <- args[3]
output_file_path <- args[4]

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

transport_zones_buffer <- st_union(st_buffer(st_geometry(transport_zones), 20e3))
transport_zones_buffer <- st_transform(transport_zones_buffer, 4326)
transport_zones <- st_transform(transport_zones, 4326)


gtfs <- readRDS(gtfs_file_path)

info(logger, "Preparing the timetable for one day (tuesday)...")

gtfs_tt <- gtfs_timetable(gtfs, day = "tuesday")
gtfs_tt$timetable <- gtfs_tt$timetable[order(departure_time)]


# Cluster stops in each transport zone to limit the number of origin - destinations
info(logger, "Clustering stops...")

stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
st_crs(stops) <- 4326

stops <- st_join(stops, transport_zones)
stops <- st_transform(stops, 2154)

stops <- cbind(as.data.table(stops)[, list(stop_id, transport_zone_id)], st_coordinates(stops))
stops <- stops[!is.na(transport_zone_id)]
stops <- stops[!duplicated(stops[, list(X, Y)])]

stops[,
      cluster := kmeans(
        cbind(X, Y),
        centers = ifelse(.N == 1, 1, min(.N-1, ceiling(.N*0.1)))
      )$cluster,
      by = transport_zone_id
]

stops[, N_stops := .N, by = list(transport_zone_id, cluster)]
stops[, X_mean := mean(X), by = list(transport_zone_id, cluster)]
stops[, Y_mean := mean(Y), by = list(transport_zone_id, cluster)]
stops[, d_cluster_center := sqrt((X - X_mean)^2 + (Y - Y_mean)^2)]
stops[, cluster_center := d_cluster_center == min(d_cluster_center), by = list(transport_zone_id, cluster)]

stops_cluster <- stops[cluster_center == TRUE]
stops_cluster <- stops_cluster[!duplicated(stops_cluster[, list(X_mean, Y_mean)])]


info(logger, "Finding fastest routes between accessible stop clusters at 8 AM (this can take a while)...")

travel_times <- stops_cluster[,
    gtfs_traveltimes(
      gtfs_tt,
      from = stop_id,
      from_is_id = TRUE,
      start_time_limits = c(7.5, 8.5)*3600,
      max_traveltime = 1*3600,
      minimise_transfers = FALSE,
      day = "tuesday"
    ),
    by = list(transport_zone_id, cluster, stop_id)
]

travel_times <- travel_times[, c(1, 2, 3, 4, 5, 6, 7)]
setnames(travel_times, c("transport_zone_id", "cluster", "from_stop_id", "start_time", "duration", "ntransfers", "to_stop_id"))

# Remove all stops which are not cluster centers
# travel_times <- travel_times[to_stop_id %in% stops_cluster$stop_id]

info(logger, "Identifying public transport modes used for each route...")

# Remove loops
travel_times <- travel_times[from_stop_id != to_stop_id]

# Add the mode
route_types <- as.data.table(read_excel(gtfs_route_types_path))
route_types <- route_types[, list(route_type, route_type_label)]


route_modes <- unique(gtfs$stop_times[, list(trip_id, stop_id)])
route_modes <- merge(route_modes, gtfs$trips[, list(trip_id, service_id, route_id)], by = "trip_id")
route_modes <- merge(route_modes, gtfs$routes[, list(route_id, route_type)], by = "route_id")
route_modes <- merge(route_modes, route_types, by = "route_type", all.x = TRUE)
route_modes <- route_modes[, list(route_type_label = route_type_label[1]), by = list(stop_id)]

travel_times <- merge(travel_times, route_modes, by.x = "from_stop_id", by.y = "stop_id", all.x = TRUE)
travel_times <- merge(travel_times, route_modes, by.x = "to_stop_id", by.y = "stop_id", all.x = TRUE, suffixes = c("_from", "_to"))

travel_times[, route_type := paste0(route_type_label_from, "+", route_type_label_to)]


# Compute the median time of travel between transport zones
info(logger, "Computing median travel times and distances between accessible stop clusters...")


travel_times <- merge(travel_times, stops[, list(stop_id, X, Y)], by.x = "from_stop_id", by.y = "stop_id")
travel_times <- merge(travel_times, stops[, list(stop_id, transport_zone_id, cluster, X, Y)], by.x = "to_stop_id", by.y = "stop_id")

travel_times[, duration := as.numeric(duration)]
travel_times[, distance := sqrt((X.x - X.y)^2 + (Y.x - Y.y)^2)]
travel_times[, distance := distance*(1.1+0.3*exp(-distance/20))]


setnames(travel_times, c("transport_zone_id.x", "transport_zone_id.y"), c("from", "to"))
travel_times <- travel_times[!is.na(to)]

travel_times[, duration := duration/3600]
travel_times[, distance := distance/1000]


# For each transport zone to transport zone link, take the trip with the median time of travel
travel_costs <- travel_times[,
   list(
     distance = distance[which.min(abs(median(duration) - duration))][1],
     time = median(duration),
     mode = route_type[which.min(abs(median(duration) - duration))][1]
   ),
   by = list(from, to)
]



write_parquet(travel_costs, output_file_path)

