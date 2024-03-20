library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(sfheaders)

args <- commandArgs(trailingOnly = TRUE)

tz_file_path <- args[1]
gtfs_file_paths <- args[2]
output_file_path <- args[3]

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

transport_zones_buffer <- st_union(st_buffer(st_geometry(transport_zones), 20e3))
transport_zones_buffer <- st_transform(transport_zones_buffer, 4326)
transport_zones <- st_transform(transport_zones, 4326)


# Prepare each dataset
info(logger, "Loading and merging GTFS files...")

gtfs_file_paths <- strsplit(gtfs_file_paths, ",")[[1]]

gtfs_file_paths <- lapply(1:length(gtfs_file_paths), function(i) {
  return(
    list(
      name = i,
      file = gtfs_file_paths[i]
    )
  )
})

gtfs_all <- lapply(gtfs_file_paths, function(dataset) {
  
  message(paste0("Loading GTFS file : ", dataset$file))
  
  gtfs <- NULL
  
  # Load the GTFS data
  tryCatch({
    
    gtfs <- extract_gtfs(dataset$file)
    
    # Keep only stops within the region
    stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
    st_crs(stops) <- 4326
    stops <- st_intersection(stops, transport_zones_buffer)
    
    gtfs$stops <- gtfs$stops[stop_id %in% stops$stop_id]
    gtfs$stop_times <- gtfs$stop_times[stop_id %in% stops$stop_id]
    gtfs$stop_times <- gtfs$stop_times[order(trip_id, arrival_time)]
    
    # Make all ids unique
    columns <- c("service_id", "stop_id", "agency_id", "trip_id", "route_id", "from_stop_id", "to_stop_id")
    for (table in names(gtfs)) {
      for (col in columns) {
        if (col %in% colnames(gtfs[[table]])) {
          gtfs[[table]][, (col) := paste0(dataset$name, "-", get(col))]
        }
      }
    }
    
    # Remove calendar data that does not respect the GTFS format
    # (some feed erroneously copy their calendar_dates data in the calendar data)
    calendar_cols <- c(
      "service_id", "monday", "tuesday", "wednesday", "thursday", "friday", 
      "saturday", "sunday", "start_date", "end_date"
    )
    
    if (sum(colnames(gtfs$calendar) %in% calendar_cols) != 10) {
      gtfs$calendar <- NULL  
    }
    
    # Remove stops that are not in any trip 
    gtfs$stops <- gtfs$stops[stop_id %in% gtfs$stop_times$stop_id]
    
    
  }, warning = function(w) {
    info(logger, w$message)
  }, error = function(e) {
    info(logger, "There was an error loading data from the zip file (possibly a corrupted archive).")
  })
  
  return(gtfs)
})

gtfs_all <- Filter(function(x) {!is.null(x)}, gtfs_all)

# Merge all datasets
gtfs <- list()

for (table in c("agency", "calendar", "calendar_dates", "routes", "stops", "stop_times", "transfers", "trips")) {
  df <- rbindlist(
    lapply(gtfs_all, "[[", table),
    fill = TRUE,
    use.names = TRUE
  )
  gtfs[[table]] <- df[!duplicated(df), ]
}

attr(gtfs, "filtered") <- FALSE

info(logger, "Preparing transfers between stops...")

transfer_table <- function(gtfs, d_limit = 200, crs = 2154) {
  
  # Convert the GTFS stops data.table to sf to be able perform spatial operations efficiently
  stops_xy <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
  stops_xy$stop_index <- 1:nrow(stops_xy)
  
  # Project the data according to the local CRS
  st_crs(stops_xy) <- 4326
  stops_xy <- st_transform(stops_xy, crs)
  
  # Find which stops are within d_limit meters of each stop
  stops_xy_buffer <- st_buffer(stops_xy, d_limit)
  
  intersects <- st_intersects(stops_xy, stops_xy_buffer)
  
  intersects <- lapply(seq_along(intersects), function(i) {
    data.table(stop_index = i, neighbor_stop_index = intersects[[i]])
  })
  
  intersects <- rbindlist(intersects)
  intersects <- intersects[stop_index != neighbor_stop_index]
  
  # Compute the crow fly distance and travel times between neighboring stops
  # (travel times formula is based on a linear model fitted on IDFM data)
  stops_xy_coords <- as.data.table(st_coordinates(stops_xy))
  stops_xy_coords[, stop_index := stops_xy$stop_index]
  
  transfers <- merge(intersects, stops_xy_coords, by = "stop_index")
  transfers <- merge(transfers, stops_xy_coords, by.x = "neighbor_stop_index", by.y = "stop_index", suffixes = c("_from", "_to"))
  
  transfers[, distance := sqrt((Y_to - Y_from)^2 + (X_to - X_from)^2)]
  transfers[, min_transfer_time := 31 + 1.125*distance]
  transfers[, transfer_type := 2]
  
  # Format the data as expected for the transfers table
  stops_index_id <- as.data.table(st_drop_geometry(stops_xy[, c("stop_id", "stop_index")]))
  
  transfers <- merge(transfers, stops_index_id, by = "stop_index")
  transfers <- merge(transfers, stops_index_id, by.x = "neighbor_stop_index", by.y = "stop_index", suffixes = c("_from", "_to"))
  
  setnames(transfers, c("stop_id_from", "stop_id_to"), c("from_stop_id", "to_stop_id"))
  
  transfers <- transfers[, list(from_stop_id, to_stop_id, transfer_type = 2, min_transfer_time)]
  
  # Only add new transfers
  new_transfers <- transfers[!(paste(from_stop_id, to_stop_id) %in% gtfs$transfers[, paste(from_stop_id, to_stop_id)])]
  
  gtfs$transfers <- rbindlist(list(gtfs$transfers, new_transfers), fill = TRUE, use.names = TRUE)
  
  return(gtfs)
  
}

gtfs <- transfer_table(gtfs, d_limit = 200, crs = 2154)


info(logger, "Fixing potential issues with stop times...")

fix_stop_times <- function(gtfs) {
  
  # Store the name of the original columns to be able to filter all other 
  # columns at the end of the function
  cols <- colnames(gtfs$stop_times)
  
  # Convert the GTFS stops data.table to sf to be able perform spatial operations efficiently
  stops_xy <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
  st_crs(stops_xy) <- 4326
  stops_xy <- st_transform(stops_xy, 2154)
  stops_xy <- as.data.table(st_coordinates(stops_xy))
  stops_xy$stop_id <- gtfs$stops$stop_id
  
  gtfs$stop_times[, previous_stop_id := shift(stop_id), by = trip_id]
  gtfs$stop_times[, previous_departure_time := shift(departure_time), by = trip_id]
  
  gtfs$stop_times[is.na(previous_stop_id), previous_stop_id := stop_id]
  gtfs$stop_times[is.na(previous_departure_time), previous_departure_time := departure_time]
  
  gtfs$stop_times[, delta_time := arrival_time - previous_departure_time]
  
  gtfs$stop_times <- merge(gtfs$stop_times, stops_xy, by = "stop_id")
  gtfs$stop_times <- merge(gtfs$stop_times, stops_xy, by.x = "previous_stop_id", by.y = "stop_id", suffixes = c("_from", "_to"))
  
  gtfs$stop_times[, distance := sqrt((Y_to - Y_from)^2 + (X_to - X_from)^2)]
  
  gtfs$stop_times <- gtfs$stop_times[order(trip_id, departure_time)]
  
  gtfs$stop_times[, speed := distance/delta_time]
  gtfs$stop_times[distance == 0.0, speed := 1.0]
  
  gtfs$stop_times[, last_speed := shift(speed), by = trip_id]
  gtfs$stop_times[, next_speed := shift(speed, -1), by = trip_id]
  
  gtfs$stop_times[speed > 100 & !is.na(last_speed) & !is.na(next_speed), speed_interp := (last_speed + next_speed)/2]
  gtfs$stop_times[speed > 100 & is.na(speed_interp) & !is.na(last_speed), speed_interp := last_speed]
  gtfs$stop_times[speed > 100 & is.na(speed_interp) & !is.na(next_speed), speed_interp := next_speed]
  
  gtfs$stop_times[, speed_corr := ifelse(speed > 100, speed_interp, speed)]
  gtfs$stop_times[, speed_corr := ifelse(speed > 100, speed_interp, speed)]
  
  gtfs$stop_times[, wait_time := departure_time - arrival_time]
  
  gtfs$stop_times[, delta_time_corr := ceiling(distance/speed_corr), by = trip_id]
  
  gtfs$stop_times[, arrival_time := arrival_time[1] + cumsum(delta_time_corr), by = trip_id]
  gtfs$stop_times[, departure_time := arrival_time[1] + cumsum(delta_time_corr + wait_time), by = trip_id]
  
  gtfs$stop_times <- gtfs$stop_times[, cols, with = FALSE]
  
  return(gtfs)
  
}

gtfs <- fix_stop_times(gtfs)


info(logger, "Preparing the timetable for one day (tuesday)...")

gtfs <- gtfs_timetable(gtfs, day = "tuesday")


saveRDS(gtfs, output_file_path)
