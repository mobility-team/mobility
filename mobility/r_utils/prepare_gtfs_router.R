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
tz_file_path <- 'D:\\data\\mobility\\projects\\grand-geneve\\2e3f146ec4314657eda8c102d316cb49-transport_zones.gpkg'
gtfs_file_paths <- 'D:\\data\\mobility\\data\\gtfs\\aa01d021a36d977442017bcbf81f1f06-a161391d60620240d7ae4ee37235fbc3_gtfs_complete.zip,D:\\data\\mobility\\data\\gtfs\\a4b0683e98db619fd0f1059412a048ce-2f009fbd51ed54c18dcf9b2cdc7f0364_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\a3f11398509cab5d6479ed1bd4c927c9-b70c0b29d3c2d0d262640e04a62869cb_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\6b19b87ec8cd0c7621d11e3f00fa64e6-8b448f968541a814cd23152c77f54797_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\844d0f40ab04b8e940587d49219d0c36-6434c57678fdd877da53c749c0eea4cf_gtfs_generic_eu.zip,D:\\data\\mobility\\data\\gtfs\\853ab300e3f25133c760fafc1b020f17-328d14646a81a0cdd25e39a12191b806_gtfs_static.zip,D:\\data\\mobility\\data\\gtfs\\af0bdb788a9327ca029558d94ba1129c-c9e2c2b923e6ab5a6a02776f6ee82207_export-ter-gtfs-last.zip,D:\\data\\mobility\\data\\gtfs\\6ed723ba6bcedaf33afe0a279fcd90eb-d47d66a3537807ae0e6b9eae45d2a802_export_gtfs_voyages.zip,D:\\data\\mobility\\data\\gtfs\\06101eb02fd2cb92513c5a9146258fef-184253675901bfb025145a45593a95d0_DAT_AURA_GTFS_ExportAOM.zip,D:\\data\\mobility\\data\\gtfs\\118e8c7ccc21dab6ef7c4ae00aeb2829-f47807b294f863252c2da5e52a82a0da_GTFS_haute_savoie.zip,D:\\data\\mobility\\data\\gtfs\\4b15be20d8f43c174e5518dffe382b9c-e852de9f6ce818cc03277ac94926620a_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\d258e974735db72d69d53635f49f8e7a-859952002fa3c071703717753892eab8_DownloadfileNameCG38.GTFS.zip,D:\\data\\mobility\\data\\gtfs\\ad3958a96dacccb44825549e21e84979-7c570637abe59c4c966bdd7323db2746_naq-aggregated-gtfs.zip,D:\\data\\mobility\\data\\gtfs\\76ae84ec77435f8027de4beb735aebb7-9adb2aafa0827752a0db8a036b32f0e7_DESTINEO.gtfs.zip,D:\\data\\mobility\\data\\gtfs\\9e748ff3e0f6f3d11c7e7cf702348174-7eb92f86cd2571d4b6659470f41c66ce_KORRIGOBRET.gtfs.zip,D:\\data\\mobility\\data\\gtfs\\2e7eba6649c71cad0be8babd91b52ef8-05db816cd25aa105999a4cfe22c25ff3_pt-th-offer-atoumod-gtfs-20240515-708-opendata.zip,D:\\data\\mobility\\data\\gtfs\\4c0ab6406f889725882c14d580c5125f-9a6ec87a6e13b2162aae4c8f1e48a13c_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\bbcd21254f084dce9842c6ab472e89db-3099d69b1640aa362433a2b01a5adc4d_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\7a2f14da880080e2c63fb77abaac8b8e-2a8453724d9c657011b2640ab935f1ad_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\b4818db36776c0fb9d913e87abb49204-3cb37cbf51263dcec94874cc10aa8913_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\bc783b0a8511eebc0336a1a47e7ec6a7-ba0fc9ebd797ef95c9ae8c794b02d65a_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\b17491e099321ffe59121158cc03df2e-e844419b3e84ae96125e7b87ce222526_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\004500fc0adb8c8ae9f55831a6979eb7-70b9a19cf4e00c1988a5b8cb1a9a9e5e_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\6224dcc8471990db2ce2d52e4b3d1837-3b1b9a8a5bedc5c63b0b2cf11c6ba185_gtfs-sibra.zip,D:\\data\\mobility\\data\\gtfs\\d941bf10e2cbff9cfe93789801ff6e78-01939c605ab107a671c35ddb2859774b_chamberyapiKey223f2f102c1242570d3f0231326a271940774f72typegtfs_urbain.zip,D:\\data\\mobility\\data\\gtfs\\7233442004315843e003a063b0b226db-42f0655e9e41033f28eb9d26b17cb80f_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\60b323006158fc93308008b7626ad243-88f516ac120d0a40b50a35638a182350_DownloadfileNameCAPI.GTFS.zip,D:\\data\\mobility\\data\\gtfs\\bc4d950d015d8a1427d6748c283b85e2-36b41f205c5ab27575d18b06caa34ee0_capg-2024.zip,D:\\data\\mobility\\data\\gtfs\\1b011f6b7ee75af5951714b3622b9399-d00b7b823644abd7a018b69af98f5f96_medias.zip,D:\\data\\mobility\\data\\gtfs\\05dd0d50d9b519ed2c2f31474b0c448f-6343c6699bbcbdab5f285629f0c7ed27_gtfs.zip,D:\\data\\mobility\\data\\gtfs\\495875e4327e0246c28afdb7a245260c-c70e52d90d19c7067176a1382459e454_medias.zip,D:\\data\\mobility\\data\\gtfs\\ce4c2ed581224b29eb57036f6bff0dbc-1745561814d0d3b6bb148b516e09d468_gtfs-evian-10122023-26052024-v3.zip'


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
  
  gtfs <- NULL
  
  # Load the GTFS data
  tryCatch({
    
    gtfs <- extract_gtfs(dataset$file, quiet = TRUE)
    
    # Keep only stops within the region
    stops <- sfheaders::sf_point(gtfs$stops, x = "stop_lon", y = "stop_lat", keep = TRUE)
    st_crs(stops) <- 4326
    stops <- st_intersection(stops, transport_zones_buffer)
    
    gtfs$stops <- gtfs$stops[stop_id %in% stops$stop_id]
    gtfs$stop_times <- gtfs$stop_times[stop_id %in% stops$stop_id]
    gtfs$stop_times <- gtfs$stop_times[order(trip_id, arrival_time)]
    
    gtfs$trips <- gtfs$trips[trip_id %in% gtfs$stop_times$trip_id]
    gtfs$routes <- gtfs$routes[route_id %in% gtfs$trips$route_id]
    gtfs$calendar <- gtfs$calendar[service_id %in% gtfs$trips$service_id]
    gtfs$calendar_dates <- gtfs$calendar_dates[service_id %in% gtfs$trips$service_id]
    
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
    if ("calendar" %in% names(gtfs)) {
      calendar_cols <- c(
        "service_id", "monday", "tuesday", "wednesday", "thursday", "friday", 
        "saturday", "sunday", "start_date", "end_date"
      )
      
      if (sum(colnames(gtfs$calendar) %in% calendar_cols) != 10) {
        gtfs$calendar <- NULL  
      }
    }
    
    if ("calendar_dates" %in% names(gtfs)) {
      calendar_dates_cols <- c(
        "service_id", "date", "exception_type"
      )
      
      if (sum(colnames(gtfs$calendar_dates) %in% calendar_dates_cols) != 3) {
        gtfs$calendar_dates <- NULL  
      }
    }

    # Remove stops that are not in any trip 
    gtfs$stops <- gtfs$stops[stop_id %in% gtfs$stop_times$stop_id]
    
  }, error = function(e) {
    info(logger, "There was an error loading data from the zip file (possibly a corrupted archive).")
  }, warning = function(w) {
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
  
  gtfs$transfers <- rbindlist(list(gtfs$transfers, new_transfers), fill = TRUE, use.names = TRUE)
  
  return(gtfs)
  
}

gtfs <- transfer_table(gtfs, d_limit = 200, crs = 2154)
gtfs$transfers <- gtfs$transfers[from_stop_id != to_stop_id]

# Transfers are NA in some feeds
# TO DO : investigate why ?
gtfs$transfers[is.na(transfer_type), transfer_type := 2]


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
  
  gtfs$stop_times[speed > 200 & !is.na(last_speed) & !is.na(next_speed), speed_interp := (last_speed + next_speed)/2]
  gtfs$stop_times[speed > 200 & is.na(speed_interp) & !is.na(last_speed), speed_interp := last_speed]
  gtfs$stop_times[speed > 200 & is.na(speed_interp) & !is.na(next_speed), speed_interp := next_speed]
  
  gtfs$stop_times[, speed_corr := ifelse(speed > 200, speed_interp, speed)]
  gtfs$stop_times[, speed_corr := ifelse(speed > 200, speed_interp, speed)]
  
  gtfs$stop_times[, wait_time := departure_time - arrival_time]
  
  gtfs$stop_times[, delta_time_corr := ceiling(distance/speed_corr), by = trip_id]
  
  gtfs$stop_times[, arrival_time := arrival_time[1] + cumsum(delta_time_corr), by = trip_id]
  gtfs$stop_times[, departure_time := arrival_time[1] + cumsum(delta_time_corr + wait_time), by = trip_id]
  
  gtfs$stop_times <- gtfs$stop_times[, cols, with = FALSE]
  
  return(gtfs)
  
}

gtfs <- fix_stop_times(gtfs)


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

adjust_to_tuesday <- function(date) {
  while(wday(date) != 2) {
    date <- date + days(1)
  }
  return(date)
}

start_date <- adjust_to_tuesday(start_date)

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

# Remove stops that do not appear in any trips
# stop_ids <- unique(gtfs$stop_times$stop_id)
# 
# gtfs$stops <- gtfs$stops[stop_id %in% stop_ids]
# 
# gtfs$timetable <- gtfs$timetable[departure_station]
# gtfs$stop_ids <- gtfs$stop_ids[gtfs$stop_id %in% stop_ids]


saveRDS(gtfs, output_file_path)
