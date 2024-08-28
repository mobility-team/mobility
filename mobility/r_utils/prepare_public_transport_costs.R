library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(readxl)
library(future.apply)
library(lubridate)

args <- commandArgs(trailingOnly = TRUE)

tz_file_path <- args[1]

gtfs_file_path <- args[2]
gtfs_route_types_path <- args[3]

start_time_min <- as.numeric(args[4])
start_time_max <- as.numeric(args[5])
max_traveltime <- as.numeric(args[6])

output_file_path <- args[7]

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

transport_zones_buffer <- st_union(st_buffer(st_geometry(transport_zones), 20e3))
transport_zones_buffer <- st_transform(transport_zones_buffer, 4326)
transport_zones <- st_transform(transport_zones, 4326)


gtfs <- readRDS(gtfs_file_path)

info(logger, "Preparing stops...")

stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
st_crs(stops) <- 4326

stops <- st_join(stops, transport_zones)
stops <- st_transform(stops, 2154)

stops <- cbind(as.data.table(stops)[, list(stop_id, transport_zone_id)], st_coordinates(stops))
stops <- stops[!is.na(transport_zone_id)]
stops <- stops[!duplicated(stops[, list(X, Y)])]

# Add the mode
route_types <- as.data.table(read_excel(gtfs_route_types_path))
route_types <- route_types[, list(route_type, route_type_label)]

route_modes <- unique(gtfs$stop_times[, list(trip_id, stop_id)])
route_modes <- merge(route_modes, gtfs$trips[, list(trip_id, service_id, route_id)], by = "trip_id")
route_modes <- merge(route_modes, gtfs$routes[, list(route_id, route_type)], by = "route_id")
route_modes <- merge(route_modes, route_types, by = "route_type", all.x = TRUE)
route_modes <- route_modes[, list(route_type_label = route_type_label[1]), by = list(stop_id)]


info(
  logger,
  sprintf(
    "Finding fastest routes between accessible stops between %s and %s hours (this can take a while)...",
    as.character(start_time_min),
    as.character(start_time_max)
  )
)

# Uses all the available logical cores but 2, to speed us calculations
plan(multisession, workers = min(parallel::detectCores()-2))

travel_costs <- future_lapply(seq(length(stops$stop_id)), future.seed = TRUE, FUN = function(i) {
  
  # Finds travel times from that stop to every other stop within the max_traveltime limit
  tt <- gtfs_traveltimes(
    gtfs = gtfs,
    from = stops$stop_id[i],
    from_is_id = TRUE,
    start_time_limits = c(start_time_min, start_time_max)*3600,
    max_traveltime = max_traveltime*3600,
    minimise_transfers = TRUE,
    quiet = FALSE
  )
  
  if (nrow(tt) == 0) {
    
    tt <- NULL

  } else {
    
    tt <- as.data.table(tt)
    
    # Keeps only actual durations
    tt <- tt[!grepl("-", duration)]
    tt[, duration := as.numeric(lubridate::hms(duration))]
    
    # Adds transport_zone_id for each origin stop
    tt[, from_transport_zone_id := stops$transport_zone_id[i]]
    tt[, from_stop_id := stops$stop_id[i]]
    
    setnames(tt, "stop_id", "to_stop_id")
    tt <- tt[, list(from_transport_zone_id, from_stop_id, to_stop_id, duration)]
    
    # Merges in a single table by origin and destination stops
    tt <- merge(
      tt,
      stops[, list(stop_id, X, Y)],
      by.x = "from_stop_id",
      by.y = "stop_id"
    )
    
    tt <- merge(
      tt,
      stops[, list(stop_id, to_transport_zone_id = transport_zone_id, X, Y)],
      by.x = "to_stop_id",
      by.y = "stop_id"
    )
    
    # Euclidian distance between stops plus a small detour 
    tt[, distance := sqrt((X.x - X.y)^2 + (Y.x - Y.y)^2)]
    tt[, distance := distance*(1.1+0.3*exp(-distance/20))]
    
    # Adds route modes for stops
    tt <- merge(
      tt,
      route_modes,
      by.x = "from_stop_id",
      by.y = "stop_id",
      all.x = TRUE
    )
    
    tt <- merge(
      tt,
      route_modes,
      by.x = "to_stop_id",
      by.y = "stop_id",
      all.x = TRUE,
      suffixes = c("_from", "_to")
    )
    
    tt[, route_type := paste0(route_type_label_from, "+", route_type_label_to)]
    
    tt <- tt[, list(
      from = from_transport_zone_id,
      to = to_transport_zone_id,
      distance = distance,
      time = duration,
      route_type
    )]
    
  }

  return(tt)
  
})

#Back to single core use
plan(sequential)

#Eliminating null durations
travel_costs <- Filter(function(x) !is.null(x), travel_costs)
travel_costs <- rbindlist(travel_costs)

# Takes the distance and the mode of a median journey between transport zones, but the minimal time plus a constant
travel_costs <- travel_costs[,
   list(
     distance = distance[which.min(abs(median(time) - time))][1],
     time = 5*60 + min(time),
     mode = route_type[which.min(abs(median(time) - time))][1]
   ),
   by = list(from, to)
]

travel_costs[, from := as.integer(from)]
travel_costs[, to := as.integer(to)]
travel_costs[, distance := distance/1000]
travel_costs[, time := time/3600]

write_parquet(travel_costs, output_file_path)