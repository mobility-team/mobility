library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(future.apply)
library(lubridate)
library(FNN)
library(cppRouting)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]

tz_file_path <- args[2]

gtfs_file_path <- args[3]
gtfs_route_types_path <- args[4]

start_time_min <- as.numeric(args[5])
start_time_max <- as.numeric(args[6])
max_traveltime <- as.numeric(args[7])

output_file_path <- args[8]


# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\study_area\\d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg'
# gtfs_file_path <- 'D:/data/mobility/projects/study_area/3b724506861960add825434760d69b05-gtfs_router.rds'
# gtfs_route_types_path <- 'D:\\dev\\mobility_oss\\mobility\\data\\gtfs\\gtfs_route_types.xlsx'
# start_time_min <- as.numeric('6.5')
# start_time_max <- as.numeric('7.5')
# max_traveltime <- as.numeric('1.0')
# max_walking_time <- as.numeric('0.167')
# max_walking_speed <- as.numeric('5')
# output_file_path <- 'D:\\data\\mobility\\projects\\study_area\\92d64787200e7ed83bc8eadf88d3acc4-public_transport_travel_costs.parquet'

source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

# Compute the travel costs between all stops in the GTFS data
gtfs <- readRDS(gtfs_file_path)
stops <- get_gtfs_stops(gtfs, transport_zones)

travel_costs <- compute_gtfs_travel_costs(
  gtfs,
  stops,
  start_time_min,
  start_time_max,
  max_traveltime,
  gtfs_route_types_path
)

travel_costs[, distance := distance/1000]
travel_costs[, time := time/3600]

travel_costs <- travel_costs[time < max_traveltime]
travel_costs <- travel_costs[, list(from_stop_id, to_stop_id, time, distance)]

write_parquet(travel_costs, output_file_path)
