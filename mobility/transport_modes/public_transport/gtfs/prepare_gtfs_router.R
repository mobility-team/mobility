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
route_types_fp <- args[4]
output_file_path <- args[5]

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg'
# gtfs_file_paths <- 'D:\\data\\mobility\\data\\gtfs\\8f079203e31e5691d6d5f477289a49c0-a161391d60620240d7ae4ee37235fbc3_gtfs_complete.zip,D:\\data\\mobility\\data\\gtfs\\05640a5657d2c01dfa31724c21766f01-b70c0b29d3c2d0d262640e04a62869cb_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\20c21f94f8253a28d75284d5fbfe1066-6434c57678fdd877da53c749c0eea4cf_gtfs_generic_eu.zip,D:\\data\\mobility\\data\\gtfs\\320285583e204d9870286c46c261efa1-c9e2c2b923e6ab5a6a02776f6ee82207_export-ter-gtfs-last.zip,D:\\data\\mobility\\data\\gtfs\\4f2639f26eb94e07a18aa7ace94b286c-d47d66a3537807ae0e6b9eae45d2a802_export_gtfs_voyages.zip,D:\\data\\mobility\\data\\gtfs\\bc5dbe98b9daa96fe316d03fc5df3c6b-184253675901bfb025145a45593a95d0_DAT_AURA_GTFS_ExportAOM.zip,D:\\data\\mobility\\data\\gtfs\\1b3e497042c10298355caebe9c2dd0d2-f47807b294f863252c2da5e52a82a0da_GTFS_haute_savoie.zip,D:\\data\\mobility\\data\\gtfs\\ded173997cbb2ae769981abfa256b88d-9adb2aafa0827752a0db8a036b32f0e7_DESTINEO.gtfs.zip,D:\\data\\mobility\\data\\gtfs\\96db7cde4b24f24cf5c61b89923b89e6-7eb92f86cd2571d4b6659470f41c66ce_KORRIGOBRET.gtfs.zip,D:\\data\\mobility\\data\\gtfs\\53364a5913538d1294ca2493c35ec5ba-05db816cd25aa105999a4cfe22c25ff3_pt-th-offer-atoumod-gtfs-20240515-708-opendata.zip,D:\\data\\mobility\\data\\gtfs\\91ac42fa84d13e722a293ec9e2973723-9a6ec87a6e13b2162aae4c8f1e48a13c_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\af81d29e11025429121f73f53a40fa17-3099d69b1640aa362433a2b01a5adc4d_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\094e45c18e15942893a32a5533d05837-2a8453724d9c657011b2640ab935f1ad_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\9624bfdce445e8068df3e1b43ce473bc-3cb37cbf51263dcec94874cc10aa8913_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\e030ba9af42cb993315b4994aa113656-ba0fc9ebd797ef95c9ae8c794b02d65a_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\eff2be32796c9810706eda64e0cf481b-e844419b3e84ae96125e7b87ce222526_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\1f874a9da730832b72907ad1d0e1c65d-70b9a19cf4e00c1988a5b8cb1a9a9e5e_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\5e3a1dd644c6443309b5ddc50dffa804-0387e1ecfb203ff88a4fa83e76e91953_gtfs-ginko.zip,D:\\data\\mobility\\data\\gtfs\\a20e7e28599b5757ff8c55447d165ec6-42f0655e9e41033f28eb9d26b17cb80f_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\9d5159c9392f474fe214d3ba433f24ec-36b41f205c5ab27575d18b06caa34ee0_capg-2024.zip,D:\\data\\mobility\\data\\gtfs\\60635a2b3ee6beec568271a865902f7e-8f6e40321ca12be15dfea9f8d150e8dd_2024.03.27-gtfs-start-2024-03-28-au-2024-06-30-orchestra-sans-scolaires.zip,D:\\data\\mobility\\data\\gtfs\\6e03163fbad50c58ecf80b853ef61f56-ba2cb3a0cacf24a315c0c8e5db3c8e27_gtfs-dole.zip,D:\\data\\mobility\\data\\gtfs\\a415c76765906b4cc1e53d4ea6e39098-1745561814d0d3b6bb148b516e09d468_gtfs-evian-10122023-26052024-v3.zip,D:/data/mobility/projects/haut-doubs/gtfs-vallorbe-pontarlier.zip,D:/data/mobility/projects/haut-doubs/export-ter-gtfs-2024-06-12-edited-haut-doubs.zip'
# # gtfs_file_paths <- 'D:/data/mobility/data/gtfs/a415c76765906b4cc1e53d4ea6e39098-1745561814d0d3b6bb148b516e09d468_gtfs-evian-10122023-26052024-v3.zip'
# route_types_fp <- 'D:/dev/mobility_oss/mobility/data/gtfs/gtfs_route_types.csv'
# output_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\0c828f5d2dc7774ee06699b2812eb93a-gtfs_router.rds'

# stop()


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
      
      if (!"agency_id" %in% colnames(gtfs$routes)) {
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

# Filter trips
service_ids <- gtfs$calendar[start_date < max_services_date & end_date > max_services_date & tuesday == 1, service_id]
service_ids <- unique(c(service_ids, gtfs$calendar_dates[date == max_services_date & exception_type == 1, service_id]))
service_ids <- setdiff(service_ids, gtfs$calendar_dates[date == max_services_date & exception_type == 2, service_id])

trip_ids <- gtfs$trips[service_id %in% service_ids, trip_id]
gtfs$stop_times <- gtfs$stop_times[trip_id %in% trip_ids]

saveRDS(gtfs, output_file_path)
