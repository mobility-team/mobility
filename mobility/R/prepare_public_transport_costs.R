library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(readxl)
library(parallel)
library(pbapply)

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
        centers = ifelse(.N == 1, 1, min(.N-1, ceiling(.N*0.05)))
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


# Add the mode
route_types <- as.data.table(read_excel(gtfs_route_types_path))
route_types <- route_types[, list(route_type, route_type_label)]


route_modes <- unique(gtfs$stop_times[, list(trip_id, stop_id)])
route_modes <- merge(route_modes, gtfs$trips[, list(trip_id, service_id, route_id)], by = "trip_id")
route_modes <- merge(route_modes, gtfs$routes[, list(route_id, route_type)], by = "route_id")
route_modes <- merge(route_modes, route_types, by = "route_type", all.x = TRUE)
route_modes <- route_modes[, list(route_type_label = route_type_label[1]), by = list(stop_id)]


info(logger, "Finding fastest routes between accessible stop clusters at 8 AM (this can take a while)...")


gtfs_travel_costs_parallel <- function(gtfs, transport_zone_ids, stop_ids, stops, route_modes, n_parallel, ...) {
  
  env <- new.env()
  env$gtfs <- gtfs
  env$route_modes <- route_modes
  env$stops <- stops
  env$transport_zone_ids <- transport_zone_ids
  
  cl <- makeCluster(n_parallel)
  
  clusterExport(cl, varlist = c("gtfs", "route_modes", "stops", "stop_ids", "transport_zone_ids"), envir = env)
  clusterEvalQ(cl, library(data.table))
  clusterEvalQ(cl, library(gtfsrouter))
  
  travel_costs <- pblapply(seq(length(stop_ids)), function(i) {
    tt <- gtfs_traveltimes(
      gtfs = gtfs,
      from = stop_ids[i],
      ...
    )
    
    tt <- as.data.table(tt)
    
    tt[, from_transport_zone_id := transport_zone_ids[i]]
    tt[, from_stop_id := stop_ids[i]]
    
    tt[, start_time := as.numeric(start_time)]
    tt[, duration := as.numeric(duration)]
    setnames(tt, "stop_id", "to_stop_id")
    tt <- tt[, list(from_transport_zone_id, from_stop_id, to_stop_id, duration)]
    
    tt <- merge(tt, stops[, list(stop_id, X, Y)], by.x = "from_stop_id", by.y = "stop_id")
    tt <- merge(tt, stops[, list(stop_id, to_transport_zone_id = transport_zone_id, cluster, X, Y)], by.x = "to_stop_id", by.y = "stop_id")
    
    tt[, distance := sqrt((X.x - X.y)^2 + (Y.x - Y.y)^2)]
    tt[, distance := distance*(1.1+0.3*exp(-distance/20))]
    
    tt <- merge(tt, route_modes, by.x = "from_stop_id", by.y = "stop_id", all.x = TRUE)
    tt <- merge(tt, route_modes, by.x = "to_stop_id", by.y = "stop_id", all.x = TRUE, suffixes = c("_from", "_to"))
    tt[, route_type := paste0(route_type_label_from, "+", route_type_label_to)]
    
    tt <- tt[,
       list(
         distance = distance[which.min(abs(median(duration) - duration))][1],
         time = median(duration),
         mode = route_type[which.min(abs(median(duration) - duration))][1]
       ),
       by = list(from = from_transport_zone_id, to = to_transport_zone_id)
    ]
    
    
    return(tt)
  }, cl = cl)
  
  stopCluster(cl)
  
  travel_costs <- rbindlist(travel_costs)
  
  return(travel_costs)
  
}


gtfs_travel_costs <- function(gtfs, transport_zone_ids, stop_ids, stops, route_modes, ...) {
  
  travel_costs <- lapply(seq(length(stop_ids)), function(i) {
    
    tt <- gtfs_traveltimes(
      gtfs = gtfs,
      from = stop_ids[i],
      ...
    )
    
    tt <- gtfs_traveltimes(
      gtfs = gtfs,
      from = stop_ids[i],
      from_is_id = TRUE,
      start_time_limits = c(7.5, 8.5)*3600,
      max_traveltime = 1*3600,
      minimise_transfers = FALSE,
    )
    
    tt <- as.data.table(tt)
    
    tt[, from_transport_zone_id := transport_zone_ids[i]]
    tt[, from_stop_id := stop_ids[i]]
    
    tt[, start_time := as.numeric(start_time)]
    tt[, duration := as.numeric(duration)]
    setnames(tt, "stop_id", "to_stop_id")
    tt <- tt[, list(from_transport_zone_id, from_stop_id, to_stop_id, duration)]
    
    tt <- merge(tt, stops[, list(stop_id, X, Y)], by.x = "from_stop_id", by.y = "stop_id")
    tt <- merge(tt, stops[, list(stop_id, to_transport_zone_id = transport_zone_id, cluster, X, Y)], by.x = "to_stop_id", by.y = "stop_id")
    
    tt[, distance := sqrt((X.x - X.y)^2 + (Y.x - Y.y)^2)]
    tt[, distance := distance*(1.1+0.3*exp(-distance/20))]
    
    tt <- merge(tt, route_modes, by.x = "from_stop_id", by.y = "stop_id", all.x = TRUE)
    tt <- merge(tt, route_modes, by.x = "to_stop_id", by.y = "stop_id", all.x = TRUE, suffixes = c("_from", "_to"))
    tt[, route_type := paste0(route_type_label_from, "+", route_type_label_to)]
    
    tt <- tt[,
             list(
               distance = distance[which.min(abs(median(duration) - duration))][1],
               time = median(duration),
               mode = route_type[which.min(abs(median(duration) - duration))][1]
             ),
             by = list(from = from_transport_zone_id, to = to_transport_zone_id)
    ]
    
    
    return(tt)
  })
  
  travel_costs <- rbindlist(travel_costs)
  
  return(travel_costs)
  
}

# travel_costs <- gtfs_travel_costs_parallel(
#   gtfs = gtfs,
#   transport_zone_ids = stops_cluster$transport_zone_id[1:10],
#   stop_ids = stops_cluster$stop_id[1:10],
#   from_is_id = TRUE,
#   start_time_limits = c(7.5, 8.5)*3600,
#   max_traveltime = 1*3600,
#   minimise_transfers = FALSE,
#   day = "tuesday",
#   stops = stops,
#   route_modes = route_modes,
#   n_parallel = 4
# )

travel_costs <- gtfs_travel_costs(
  gtfs = gtfs,
  transport_zone_ids = stops_cluster$transport_zone_id,
  stop_ids = stops_cluster$stop_id,
  from_is_id = TRUE,
  start_time_limits = c(7.5, 8.5)*3600,
  max_traveltime = 1*3600,
  minimise_transfers = FALSE,
  stops = stops,
  route_modes = route_modes
)

travel_costs[, distance := distance/1000]
travel_costs[,  time := time/3600]

write_parquet(travel_costs, output_file_path)

