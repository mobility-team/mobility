library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)

args <- commandArgs(trailingOnly = TRUE)

tz_file_path <- args[1]
gtfs_file_path <- args[2]
output_file_path <- args[3]

gtfs_file_path <- list(
  list(
    name = "montpellier",
    file = gtfs_file_path
  )
)

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

transport_zones_buffer <- st_union(st_buffer(st_geometry(transport_zones), 20e3))
transport_zones_buffer <- st_transform(transport_zones_buffer, 4326)
transport_zones <- st_transform(transport_zones, 4326)

# Prepare each dataset
gtfs_all <- lapply(gtfs_file_path, function(dataset) {
  
  message(paste0("Loading GTFS file : ", dataset$name))
  
  # Load the GTFS data
  gtfs <- extract_gtfs(dataset$file)
  
  # Keep only stops within the region
  stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
  st_crs(stops) <- 4326
  stops <- st_intersection(stops, transport_zones_buffer)
  
  gtfs$stops <- gtfs$stops[stop_id %in% stops$stop_id]
  gtfs$stop_times <- gtfs$stop_times[stop_id %in% stops$stop_id]
  
  gtfs$stop_times <- gtfs$stop_times[order(trip_id, arrival_time)]
  
  # Create the calendar table if missing
  if (!("calendar" %in% names(gtfs)) | nrow(gtfs$calendar) == 0) {
    
    message("Missing calendar, building one from the calendar dates.")
    
    cal <- copy(gtfs$calendar_dates)
    cal[, date_formatted := ymd(date)]
    cal[, day := wday(date_formatted, label = TRUE, abbr = FALSE, locale = "English")]
    cal[, day := tolower(day)]
    
    cal[, start_date := min(date), by = service_id] 
    cal[, end_date := max(date), by = service_id]
    
    cal <- cal[, .N, by = list(service_id, start_date, end_date, day)]
    cal[, p := N/max(N), by = list(service_id, start_date, end_date)]
    cal[, service_level := ifelse(p > 0.5, 1.0, 0.0)]
    
    cal <- dcast(cal, service_id + start_date + end_date ~ day, value.var = "service_level", fill = 0)
    gtfs$calendar <- cal
  }
  
  # Make all ids unique
  columns <- c("service_id", "stop_id", "agency_id", "trip_id", "route_id")
  for (table in names(gtfs)) {
    for (col in columns) {
      if (col %in% colnames(gtfs[[table]])) {
        gtfs[[table]][, (col) := paste0(dataset$name, "-", get(col))]
      }
    }
  }
  
  return(gtfs)
})

# Merge all datasets
gtfs <- list()

for (table in names(gtfs_all[[1]])) {
  df <- rbindlist(
    lapply(gtfs_all, "[[", table),
    fill = TRUE,
    use.names = TRUE
  )
  gtfs[[table]] <- df[!duplicated(df), ]
}

attr(gtfs, "filtered") <- FALSE

# Prepare GTFS object for routing
gtfs$transfers <- gtfs_transfer_table(gtfs, d_limit = 200)
gtfs_tt <- gtfs_timetable(gtfs, day = "tuesday")
gtfs_tt$timetable <- gtfs_tt$timetable[order(departure_time)]

# Cluster stops in each transport zone to limit the number of origin - destinations

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


# stops_zones <- sfheaders::sf_point(stops_cluster, x = "X_mean", y = "Y_mean", keep = TRUE)
# st_crs(stops_zones) <- 2154
# 
# v <- st_voronoi(do.call(c, st_geometry(stops_zones)))
# v <- st_collection_extract(v)
# st_crs(v) <- 2154
# 
# v <- st_intersection(v, transport_zones_boundary)
# 
# stops_cluster$area <- as.numeric(st_area(v))
# stops_cluster[, stops_density := N_stops/area*1e6]

# stops <- merge(stops, stops_cluster[, list(stop_id, stops_density)], by = "stop_id")

# Compute a typical distance around each stop
# hyp average distance between a random point and the center of a disc which area = area of the zone

# stops$d_internal <- 2/3*sqrt(as.numeric(st_area(v))/pi)/1000
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

# Remove loops
travel_times <- travel_times[from_stop_id != to_stop_id]

# Add the mode
route_types <- list(
  "0"	= "tramway",
  "1"	= "subway",
  "2"	= "train",
  "3"	= "bus",
  "4"	= "ferry",
  "5"	= "cable_car",
  "6"	= "aerial_tramway",
  "7"	= "aerial_tramway",
  "11"	= "trolley_bus",
  "12"	= "monorail",
  "101"	= "train",
  "102"	= "train",
  "103"	= "train",
  "106"	= "train",
  "109"	= "train",
  "117"	= "train",
  "700"	= "bus",
  "900"	= "tramway",
  "1000"	= "ferry",
  "1300"	= "aerial_tramway",
  "1400"	= "aerial_tramway"
)

route_types <- data.table(
  route_type = as.integer(names(route_types)),
  route_type_label = as.character(route_types)
)

route_modes <- unique(gtfs$stop_times[, list(trip_id, stop_id)])
route_modes <- merge(route_modes, gtfs$trips[, list(trip_id, service_id, route_id)], by = "trip_id")
route_modes <- merge(route_modes, gtfs$routes[, list(route_id, route_type)], by = "route_id")
route_modes <- merge(route_modes, route_types, by = "route_type")
route_modes <- route_modes[, list(route_type_label = route_type_label[1]), by = list(stop_id)]

travel_times <- merge(travel_times, route_modes, by.x = "from_stop_id", by.y = "stop_id", all.x = TRUE)
travel_times <- merge(travel_times, route_modes, by.x = "to_stop_id", by.y = "stop_id", all.x = TRUE, suffixes = c("_from", "_to"))

travel_times[, route_type := paste0(route_type_label_from, "+", route_type_label_to)]


# Compute the median time of travel between transport zones
# travel_times <- travel_times[, list(transport_zone_id, duration, stop_id)]
travel_times <- merge(travel_times, stops[, list(stop_id, X, Y)], by.x = "from_stop_id", by.y = "stop_id")
travel_times <- merge(travel_times, stops[, list(stop_id, transport_zone_id, cluster, X, Y)], by.x = "to_stop_id", by.y = "stop_id")

travel_times[, duration := as.numeric(duration)]
travel_times[, distance := sqrt((X.x - X.y)^2 + (Y.x - Y.y)^2)]
travel_times[, distance := distance*(1.1+0.3*exp(-distance/20))]
# travel_times[, speed := distance/duration*3.6]

# Add the additional walking time to get to the first stop and to reach the destination from the last stop
# hyp 4 km/h
# travel_times <- merge(travel_times, stops[, list(stop_id, d_internal)], by.x = c("from_stop_id"), by.y = c("stop_id"))
# travel_times <- merge(travel_times, stops[, list(stop_id, d_internal)], by.x = c("to_stop_id"), by.y = c("stop_id"))
# travel_times[, duration := duration + 3600*(d_internal.x + d_internal.y)/4]


setnames(travel_times, c("transport_zone_id.x", "transport_zone_id.y"), c("from", "to"))
travel_times <- travel_times[!is.na(to)]

travel_times[, duration := duration/3600]
travel_times[, distance := distance/1000]

# For each cluster to cluster link, take the trip with the median time of travel
travel_costs <- travel_times[,
   list(
     distance = distance[which.min(abs(median(duration) - duration))][1],
     time = median(duration),
     mode = route_type[which.min(abs(median(duration) - duration))][1]
   ),
   by = list(from, to, cluster.x, cluster.y)
]

# For each transport zone to transport zone link, take the trip with the median time of travel
travel_costs <- travel_costs[,
   list(
     distance = distance[which.min(abs(median(time) - time))][1],
     time = median(time),
     mode = mode[which.min(abs(median(time) - time))][1]
   ),
   by = list(from, to)
]


write_parquet(travel_costs, output_file_path)
