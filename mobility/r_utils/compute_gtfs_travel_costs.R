

compute_gtfs_travel_costs <- function(
    gtfs,
    stops,
    start_time_min,
    start_time_max,
    max_traveltime
  ) {
  
  info(
    logger,
    sprintf(
      "Finding fastest routes between accessible stops between %s and %s hours (this can take a while)...",
      as.character(start_time_min),
      as.character(start_time_max)
    )
  )
  
  # Compute the average time between consecutive services (= headway) at each stop and for each service
  services_headways <- gtfs$stop_times[, list(stop_id, trip_id, departure_time)]
  services_headways <- merge(services_headways, gtfs$trips[, list(trip_id, service_id)], by = "trip_id")
  services_headways <- services_headways[order(departure_time)]
  services_headways <- services_headways[departure_time > start_time_min*3600 & departure_time < start_time_max*3600]
  services_headways[, departure_time := departure_time - start_time_min*3600]
  services_headways[, d_departure_time := diff(c(departure_time[.N]-3600, departure_time)), by = list(service_id, stop_id)]
  services_headways <- services_headways[, list(headway_time = mean(d_departure_time)), by = list(stop_id, service_id)]
  
  # Compute the average headway at each stop
  stops_headways <- services_headways[, list(headway_time = mean(headway_time)), by = stop_id]
  stops_headways[, wait_time := pmin(0.5*headway_time, 15.0*60)]
  stops_headways <- stops_headways[, list(stop_id, wait_time)]
  
  # Uses all the available logical cores but 2, to speed us calculations
  plan(multisession, workers = max(parallel::detectCores()-2, 1))
  
  gtfs_travel_costs <- future_lapply(seq(length(stops$stop_id)), future.seed = TRUE, FUN = function(i) {
    
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
      
      # Remove bugs
      # See https://github.com/UrbanAnalyst/gtfsrouter/issues/116
      tt <- tt[start_time != "-1:59:59" & duration != "00:00:00" & !grepl("-", duration)]
      
      tt[, duration := as.numeric(lubridate::hms(duration))]
      
      # Adds transport_zone_id for each origin stop
      tt[, from_transport_zone_id := stops$transport_zone_id[i]]
      tt[, from_stop_id := stops$stop_id[i]]
      
      setnames(tt, "stop_id", "to_stop_id")
      tt <- tt[, list(from_transport_zone_id, ntransfers, from_stop_id, to_stop_id, time = duration)]
      
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
      
      # Remove trips with more than 2 transfers
      tt <- tt[ntransfers < 3]
      
      # Remove trips that are faster than 150 km/h 
      # bug in gtfsrouter or the gtfs data ?
      tt <- tt[distance/time*3.6 < 150.0]
      
      # Add wait time
      tt <- merge(tt, stops_headways, by.x = "from_stop_id", by.y = "stop_id")
      tt[, time := time + wait_time]
      
      tt <- tt[, c("from_stop_id", "to_stop_id", "distance", "time"), with = FALSE]
      
    }
    
    return(tt)
    
  })
  
  
  #Back to single core use
  plan(sequential)
  
  # Eliminating stops that cannot reach any ther stop in the given time window
  gtfs_travel_costs <- Filter(function(x) !is.null(x), gtfs_travel_costs)
  gtfs_travel_costs <- rbindlist(gtfs_travel_costs)
  
  # Create virtual departure and arrival stops
  gtfs_travel_costs[, from_stop_id := paste0("dep-", from_stop_id)]
  gtfs_travel_costs[, to_stop_id := paste0("arr-", to_stop_id)]
  
  return(gtfs_travel_costs)
  
}

get_gtfs_stops <- function(gtfs, transport_zones) {
  
  transport_zones_boundary <- st_union(st_geometry(transport_zones))
  transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)
  
  transport_zones_buffer <- st_union(st_buffer(st_geometry(transport_zones), 20e3))
  transport_zones_buffer <- st_transform(transport_zones_buffer, 4326)
  transport_zones <- st_transform(transport_zones, 4326)
  
  stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
  st_crs(stops) <- 4326
  
  stops <- st_join(stops, transport_zones)
  stops <- st_transform(stops, 3035)
  
  stops <- cbind(as.data.table(stops)[, list(stop_id, transport_zone_id)], st_coordinates(stops))
  stops <- stops[!is.na(transport_zone_id)]
  stops <- stops[!duplicated(stops[, list(X, Y)])]
  
  return(stops)
  
}
