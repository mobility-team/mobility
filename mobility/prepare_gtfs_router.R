library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)

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
info(logger, "Preparing possible transfers between routes...")

gtfs$transfers <- gtfs_transfer_table(gtfs, d_limit = 200)


saveRDS(gtfs, output_file_path)