library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(sfheaders)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\grand-geneve\\9f060eb2ec610d2a3bdb3bd731e739c6-transport_zones.gpkg',
#   'D:\\mobility-data\\gtfs\\80d9147b5fa8f2a62c1f9492c239b0b7-a9867a67c4aca09a3214ee7de867fbd3_ucexportdownloadid1VXd01yQl2Mb67C9bkrx0P8sqNIL532_o.zip,D:\\mobility-data\\gtfs\\8dba4ccc087960568442cfb2a2b728c9-4a590cb87669fe2bc39328652ef1d2e9_gtfs_generic_eu.zip,D:\\mobility-data\\gtfs\\ad3958a96dacccb44825549e21e84979-7c570637abe59c4c966bdd7323db2746_naq-aggregated-gtfs.zip,D:\\mobility-data\\gtfs\\dccabe1db6bbe10b7457fa5a43069869-ebfa654bbde377deaf34c670d23e9cf6_lioapiKey2b160d626f783808095373766f18714901325e45typegtfs_lio.zip,D:\\mobility-data\\gtfs\\a1e0570cf593e64a24a17e0264183d21-7960742560564aec19bdfc92988923d9_gtfs_global.zip,D:\\mobility-data\\gtfs\\9e748ff3e0f6f3d11c7e7cf702348174-7eb92f86cd2571d4b6659470f41c66ce_KORRIGOBRET.gtfs.zip,D:\\mobility-data\\gtfs\\7dc7fdbf6d0b27516ab576a904ddc290-a2065509a9ecd722ae9bcd89c6a33bf8_pt-th-offer-atoumod-gtfs-20250912-914-opendata.zip,D:\\mobility-data\\gtfs\\45f4b3956a0b9c91f3b042a2d1a4ace4-d059c488bd33c0e0d9ed9d0363d06aa5_gtfs-20240903-154223.zip',
#   'D:\\dev\\mobility_oss\\mobility\\resources\\gtfs\\gtfs_route_types.csv',
#   'D:\\test-09\\0a8bd50eb6f9cc645144a17944c656b6-gtfs_router.rds'
# )
# 
# args[3] <- "D:/downloads/gtfs-manett-20250818-20251219.zip"

package_path <- args[1]
tz_file_path <- args[2]
gtfs_file_paths <- args[3]
route_types_fp <- args[4]
output_file_path <- args[5]

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

route_types <- fread(route_types_fp, sep=";")

  
gtfs_all <- lapply(gtfs_file_paths, function(dataset) {
  
  info(logger, paste0("Loading GTFS file : ", dataset$file))

  gtfs <- extract_gtfs(dataset$file, quiet = FALSE)
  
  # Add the agency_id column if missing
  if (!("agency_id" %in% colnames(gtfs$agency))) {
    gtfs$agency[, agency_id := 1:.N]
  }
  
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
    
    gtfs <- NULL
    
  } else {
    
    # Keep only trips stopping within the region
    stop_ids <- gtfs$stops$stop_id
    trip_ids <- unique(gtfs$stop_times$trip_id)
    
    gtfs$stop_times <- gtfs$stop_times[order(trip_id, arrival_time)]
    gtfs$trips <- gtfs$trips[trip_id %in% trip_ids, list(route_id, service_id, trip_id)]
    
    # Remove trips with fewer than 2 stops
    trip_stop_counts <- gtfs$stop_times[, .N, by=trip_id]
    valid_trips <- trip_stop_counts[N >= 2, trip_id]
    
    if (length(valid_trips) != 0) {
      
      gtfs$trips <- gtfs$trips[trip_id %in% valid_trips]
      gtfs$stop_times <- gtfs$stop_times[trip_id %in% valid_trips]
      
      # Keep only routes passing in the region
      route_ids <- unique(gtfs$trips$route_id)
      
      if (!("agency_id" %in% colnames(gtfs$routes))) {
        if ("agency_id" %in% colnames(gtfs$agency)) {
          gtfs$routes$agency_id <- gtfs$agency$agency_id[1]
        } else {
          gtfs$routes$agency_id <- "default"
        }
      }

      gtfs$routes <- gtfs$routes[route_id %in% route_ids, list(route_id, agency_id, route_short_name, route_type)]
      
      # Add route types (set to bus by default if missing)
      gtfs$routes <- merge(gtfs$routes, route_types[, list(route_type, route_type_label, vehicle_capacity)], by = "route_type", all.x = TRUE)
      gtfs$routes[is.na(route_type_label), route_type_label := "bus"]
      gtfs$routes[is.na(vehicle_capacity), vehicle_capacity := 50]
                           
      # Keep only agencies that have routes passing in the region
      agency_ids <- unique(gtfs$routes$agency_id)
      gtfs$agency <- gtfs$agency[agency_id %in% agency_ids, list(agency_id, agency_name)]
      
      # Keep only calendar dates for the remaining services
      service_ids <- unique(gtfs$trips$service_id)
      
      if ("calendar" %in% names(gtfs)) {
        gtfs$calendar <- gtfs$calendar[service_id %in% service_ids]
      }
      
      if ("calendar_dates" %in% names(gtfs)) {
        
        # Force calendar_dates column names tp avoid bugs whan the GTFS file is malformed
        # (with blank lines below the header)
        # A general and better solution would be to change the parsing of gtfsrouter,
        # see https://github.com/UrbanAnalyst/gtfsrouter/issues/138
        if (("service_id" %in% colnames(gtfs$calendar_dates)) == FALSE) {
          setnames(gtfs$calendar_dates, c("service_id", "date", "exception_type"))
        }
        
        
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
      
    }
  
  }
  
  return(gtfs)
  
})

gtfs_all <- Filter(function(x) {!is.null(x)}, gtfs_all)


# Align all GTFS to a common start date
# Helps when feeds were downloaded at different times or when adding files created manually
# Warning : does not take into account "special days" like holidays !
min_dates <- sapply(gtfs_all, function(gtfs) {
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

# Find the Tuesday with the maximum number of active services after the GTFS
# dates have been aligned. This representative day is then used to keep a
# single timetable snapshot in the router asset.

tuesday_wday <- 3

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
  cal_dates <- cal_dates[wday(date) == tuesday_wday]
  
} else {
  
  cal_dates <- data.table(
    service_id = character(),
    date = as.Date(character()),
    exception_type = integer()
  )
  
}


# Create the list of candidate Tuesdays covering all GTFS service periods.
has_tuesday <- function(s, e) s <= e & (s + ((tuesday_wday - wday(s) + 7) %% 7)) <= e
cal <- cal[has_tuesday(start_date, end_date)]

adjust_to_tuesday <- function(date, direction = "past") {
  while(wday(date) != tuesday_wday) {
    date <- date + ifelse(direction == "past", -1.0, 1.0)*days(1)
  }
  return(date)
}

candidate_tuesdays <- as.Date(character())

if (nrow(cal) > 0) {
  start_date <- adjust_to_tuesday(min(cal$start_date), "past")
  end_date <- adjust_to_tuesday(max(cal$end_date), "future")
  candidate_tuesdays <- seq(from = start_date, to = end_date, by = "1 week")
}

if (nrow(cal_dates) > 0) {
  candidate_tuesdays <- sort(unique(c(candidate_tuesdays, cal_dates$date)))
}

resolve_service_ids_for_date <- function(candidate_date) {
  service_ids <- cal[
    start_date <= candidate_date & end_date >= candidate_date & tuesday == 1,
    service_id
  ]

  if (nrow(cal_dates) > 0) {
    service_ids <- unique(c(
      service_ids,
      cal_dates[date == candidate_date & exception_type == 1, service_id]
    ))
    service_ids <- setdiff(
      service_ids,
      cal_dates[date == candidate_date & exception_type == 2, service_id]
    )
  }

  unique(service_ids)
}

if (length(candidate_tuesdays) == 0) {
  # Synthetic or malformed feeds may not yield any candidate Tuesday at all.
  # In that case keep all declared services instead of failing during selection.
  service_ids <- unique(gtfs$trips$service_id)
} else {
  n_services <- data.table(date = candidate_tuesdays)
  n_services[, service_ids := lapply(date, resolve_service_ids_for_date)]
  n_services[, n := lengths(service_ids)]

  if (all(n_services$n == 0)) {
    # If every candidate Tuesday resolves to zero active services, keep all trips
    # instead of writing an empty router.
    service_ids <- unique(gtfs$trips$service_id)
  } else {
    max_services_date <- n_services[n == max(n), date][1]
    service_ids <- n_services[date == max_services_date, service_ids][[1]]
  }
}

trip_ids <- gtfs$trips[service_id %in% service_ids, trip_id]
gtfs$stop_times <- gtfs$stop_times[trip_id %in% trip_ids]

saveRDS(gtfs, output_file_path)
