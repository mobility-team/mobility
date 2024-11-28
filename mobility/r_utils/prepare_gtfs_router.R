library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(sfheaders)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_file_path <- args[2]
gtfs_file_paths <- args[3]
output_file_path <- args[4]

package_path <- 'D:/dev/mobility_oss/mobility'
tz_file_path <- 'D:/data/mobility/projects/experiments/6bd940d1b76b6128d0aa3840fc09df07-transport_zones.gpkg'
gtfs_file_paths <- 'D:/data/mobility/data/gtfs/fb4441305cb5d990a82cba49ecdc899c-b70c0b29d3c2d0d262640e04a62869cb_gtfs.zip,D:/data/mobility/data/gtfs/2350ef5d5e3a6e930ba0a59bd4ba1c5e-6434c57678fdd877da53c749c0eea4cf_gtfs_generic_eu.zip,D:/data/mobility/data/gtfs/2b99d1ca977f472ed5397e2939aa789e-c9e2c2b923e6ab5a6a02776f6ee82207_export-ter-gtfs-last.zip,D:/data/mobility/data/gtfs/968305a79fb51e308cf775239e743680-d47d66a3537807ae0e6b9eae45d2a802_export_gtfs_voyages.zip,D:/data/mobility/data/gtfs/7d186f3ce0dbd0ed535b17f5277f2102-105a0eda70e1e7dd1bc81c5505570bef_iledefrance_public.zip,D:/data/mobility/data/gtfs/ec7684622f34a1d84a84f45ffc544e11-a925e164271e4bca93433756d6a340d1_IDFM-gtfs.zip,D:/data/mobility/data/gtfs/a4601d5a90bd843dfb43718ae34f02b4-9adb2aafa0827752a0db8a036b32f0e7_DESTINEO.gtfs.zip,D:/data/mobility/data/gtfs/78c78e206ce4b0754702038c1958c868-7eb92f86cd2571d4b6659470f41c66ce_KORRIGOBRET.gtfs.zip,D:/data/mobility/data/gtfs/a56593ffb468cd4bdc5417bd1c9c7b37-8cdee8d2c78a1f828e0362c15b039c27_BREIZHGO_TER.gtfs.zip,D:/data/mobility/data/gtfs/0224fdb1e537792424013175bf65e9f9-05db816cd25aa105999a4cfe22c25ff3_pt-th-offer-atoumod-gtfs-20240515-708-opendata.zip,D:/data/mobility/data/gtfs/8860d5c99ddc6c9e62c40ed0a32a3c0e-e3e838387844b9d0d9bc349ae2ea5cb6_gtfs.zip,D:/data/mobility/data/gtfs/4acf9dcc8cb93180e062764313c4c74b-188dc42cb4c63c3e534f923ebf8ece38_feed.zip,D:/data/mobility/data/gtfs/27b22eb049345c541d98a8dfadb8013e-f4e7dc87159bb859a63b1e4decf16de1_gtfs-rezobus-rentree-septembre-2022.zip,D:/data/mobility/data/gtfs/dd3c139886b8d0f9f265312ed5fbcff5-97a181234e7d57c2da2ce4f9f367724f_knsbernay-18129489-gtfs-urbain-bernay.zip'
# gtfs_file_paths <- 'D:/data/mobility/data/gtfs/2b99d1ca977f472ed5397e2939aa789e-c9e2c2b923e6ab5a6a02776f6ee82207_export-ter-gtfs-last.zip'
output_file_path <- 'D:/data/mobility/projects/experiments/8a3e4753ed52c0dbbda6d3ff240fab31-gtfs_router.rds'


logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

transport_zones_boundary <- st_union(st_geometry(transport_zones))
transport_zones_boundary <- nngeo::st_remove_holes(transport_zones_boundary)

transport_zones_buffer <- st_union(st_buffer(st_geometry(transport_zones), 10e3))
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
  
  info(logger, paste0("Loading GTFS file : ", dataset$file))

  gtfs <- extract_gtfs(dataset$file, quiet = FALSE)
  
  # Keep only stops within the region
  stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
  st_crs(stops) <- 4326
  stops_in_tz <- lengths(st_intersects(stops, transport_zones_buffer)) > 0
  
  gtfs$stops <- gtfs$stops[stops_in_tz, list(stop_id, stop_name, stop_lat, stop_lon)]
  
  # Keep only stop times at stops that are within the region
  gtfs$stop_times <- gtfs$stop_times[
    stop_id %in% gtfs$stops$stop_id,
    list(trip_id, arrival_time, departure_time, stop_id, stop_sequence)
  ]
  
  if (nrow(gtfs$stop_times) == 0) {
    
    return(NULL)
    
  } else {
    
    # Keep only trips stopping within the region
    stop_ids <- gtfs$stops$stop_id
    trip_ids <- unique(gtfs$stop_times$trip_id)
    
    gtfs$stop_times <- gtfs$stop_times[order(trip_id, arrival_time)]
    gtfs$trips <- gtfs$trips[trip_id %in% trip_ids, list(route_id, service_id, trip_id)]
    
    # Remove trips with fewer than 2 stops
    trip_stop_counts <- gtfs$stop_times[, .N, by=trip_id]
    valid_trips <- trip_stop_counts[N >= 2, trip_id]
    
    if (length(valid_trips) == 0) {
      
      return(NULL)
      
    } else {
      
      gtfs$trips <- gtfs$trips[trip_id %in% valid_trips]
      gtfs$stop_times <- gtfs$stop_times[trip_id %in% valid_trips]
      
      # Keep only routes passing in the region
      route_ids <- unique(gtfs$trips$route_id)
      gtfs$routes <- gtfs$routes[route_id %in% route_ids, list(route_id, agency_id, route_short_name)]
      
      # Keep only agencies that have routes passing in the region
      agency_ids <- unique(gtfs$routes$agency_id)
      gtfs$agency <- gtfs$agency[agency_id %in% agency_ids, list(agency_id, agency_name)]
      
      # Keep only calendar dates for the remaining services
      service_ids <- unique(gtfs$trips$service_id)
      
      if ("calendar" %in% names(gtfs)) {
        gtfs$calendar <- gtfs$calendar[service_id %in% service_ids]
      }
      
      if ("calendar_dates" %in% names(gtfs)) {
        gtfs$calendar_dates <- gtfs$calendar_dates[service_id %in% service_ids]
      }
      
      # Keep only transfers between stops in the region
      if ("transfers" %in% names(gtfs)) {
        gtfs$transfers <- gtfs$transfers[from_stop_id %in% stop_ids & to_stop_id %in% stop_ids, ]
      }
      
      # Make all ids unique by prefixing them with the id of the GTFS dataset
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
      if ("calendar" %in% names(gtfs)) {
        calendar_cols <- c(
          "service_id", "monday", "tuesday", "wednesday", "thursday", "friday", 
          "saturday", "sunday", "start_date", "end_date"
        )
        
        if (!all(calendar_cols %in% colnames(gtfs$calendar))) {
          gtfs$calendar <- NULL  
        }
      }
      
      if ("calendar_dates" %in% names(gtfs)) {
        calendar_dates_cols <- c(
          "service_id", "date", "exception_type"
        )
        
        if (!all(calendar_dates_cols %in% colnames(gtfs$calendar_dates))) {
          gtfs$calendar_dates <- NULL  
        }
      }
      
      return(gtfs)
      
    }
  
  }
  
})

gtfs_all <- Filter(function(x) {!is.null(x)}, gtfs_all)


# Align all GTFS to a common start date
# Helps when feeds were downloaded at different times or when adding files created manually
# Warning : does not take into account "special days" like holidays !
min_dates <- sapply(gtfs_all, function(gtfs) {
  print(gtfs$calendar)
  print(gtfs$calendar_dates)
  min(gtfs$calendar$start_date, gtfs$calendar_dates$date, na.rm = TRUE)
})

max_min_date <- max(ymd(min_dates))

gtfs_all <- lapply(gtfs_all, function(gtfs) {
  
  min_date <- ymd(min(gtfs$calendar$start_date, gtfs$calendar_dates$date, na.rm = TRUE))
  delta_days <- min_date - max_min_date
  
  # Offset by whole weeks to avoid mixing up week days
  delta_days <- 7*(as.numeric(delta_days) %/% 7)
  
  date_to_int <- function(date) {
    int_date <- paste0(
      sprintf("%02d", year(date)),
      sprintf("%02d", month(date)),
      sprintf("%02d", day(date))
    )
    return(as.integer(int_date))
  }
  
  if ("calendar" %in% names(gtfs)) {
    gtfs$calendar[, start_date := ymd(start_date) - delta_days]
    gtfs$calendar[, end_date := ymd(end_date) - delta_days]
    gtfs$calendar[, start_date := date_to_int(start_date)]
    gtfs$calendar[, end_date := date_to_int(end_date)]
  }
  
  if ("calendar_dates" %in% names(gtfs)) {
    gtfs$calendar_dates[, date := ymd(date) - delta_days]
    gtfs$calendar_dates[, date := date_to_int(date)]
  }
  
  return(gtfs)
  
})



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

gtfs <- Filter(function(x) {nrow(x) > 0}, gtfs)
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
  
  if (is.null(transfers)) {
    gtfs$transfers <- new_transfers
  } else {
    gtfs$transfers <- rbindlist(list(gtfs$transfers, new_transfers), fill = TRUE, use.names = TRUE)
  }
  
  gtfs$transfers <- gtfs$transfers[, list(from_stop_id, to_stop_id, min_transfer_time, transfer_type)]
  
  gtfs$transfers <- gtfs$transfers[, list(
    min_transfer_time = min(min_transfer_time),
    transfer_type = transfer_type[which.min(min_transfer_time)]
    ),
    list(from_stop_id, to_stop_id)
  ]
  
  gtfs$transfers <- gtfs$transfers[from_stop_id != to_stop_id]
  gtfs$transfers[is.na(transfer_type), transfer_type := 2]
  
  return(gtfs)
  
}

gtfs <- transfer_table(gtfs, d_limit = 200, crs = 2154)


info(logger, "Preparing the timetable for one day (tuesday with a maximum of running services)...")

# Find the date with the maximum number of services running
# Taking into account both calendar and calendar dates info

# Prepare the data
if ("calendar" %in% names(gtfs)) {
  
  cal <- copy(gtfs$calendar)
  cal[, start_date := ymd(start_date)]
  cal[, end_date := ymd(end_date)]
  cal <- cal[tuesday > 0]
  
} else {
  
  cal <- data.table(
    service_id = "dummy-service",
    start_date = Date(1),
    end_date = Date(1)
  )
  
  gtfs$calendar <- data.table(
    service_id = "dummy-service",
    monday = 1,
    tuesday = 1,
    wednesday = 1,
    thursday = 1,
    friday = 1,
    saturday = 1,
    sunday = 1,
    start_date = 20000101,
    end_date = 21001231
  )
  
}

if ("calendar_dates" %in% names(gtfs)) {
  
  cal_dates <- copy(gtfs$calendar_dates)
  cal_dates[, date := ymd(date)]
  cal_dates <- cal_dates[wday(date) == 2]
  cal_dates[, n_services := ifelse(exception_type == 1, 1, -1)]
  
} else {
  
  cal_dates <- data.table(
    date = Date(),
    n_services = numeric()
  )
  
}



# Create the list of tuesdays covering all dates in the GTFS
end_date <- max(c(cal$end_date, cal_dates$date), na.rm = TRUE)
start_date <- min(c(cal$start_date, cal_dates$date), na.rm = TRUE)

adjust_to_tuesday <- function(date, direction = "past") {
  while(wday(date) != 2) {
    date <- date + ifelse(direction == "past", -1.0, 1.0)*days(1)
  }
  return(date)
}

start_date <- adjust_to_tuesday(start_date, "past")
end_date <- adjust_to_tuesday(end_date, "future")

tuesdays <- data.table(date = seq(from = start_date, to = end_date, by = "1 week"))

# Compute the number of services running at each date in the calendar table
n_services <- tuesdays[cal, list(n = .N), on = list(date >= start_date, date <= end_date), by = date]

# Compute the number of services added or cancelled in the calendar_dates table
delta_n_services <- cal_dates[, list(delta_n = sum(n_services)), by = date]

# Compute the actual number of services for each date
n_services <- merge(n_services, delta_n_services, by = "date", all = TRUE)
n_services[is.na(n), n := 0]
n_services[is.na(delta_n), delta_n := 0]
n_services[, n := n + delta_n]

# Select the month with the most services on average
n_services[, n_month_average := mean(n), by = list(year(date), month(date))]
n_services <- n_services[n_month_average == max(n_month_average)]

# Select the day with the most services
max_services_date <- n_services[n == max(n), date][1]
max_services_date <- as.integer(format(max_services_date, "%Y%m%d"))

# Create the GTFS timetable
gtfs <- gtfs_timetable(gtfs, date = max_services_date)


saveRDS(gtfs, output_file_path)
