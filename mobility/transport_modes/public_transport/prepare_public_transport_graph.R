library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(readxl)
library(future.apply)
library(lubridate)
library(FNN)
library(cppRouting)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_file_path <- args[2]
gtfs_file_path <-args[3]
parameters <- args[4]
output_file_path <- args[5]


# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\study_area\\d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg'
# gtfs_file_path <- 'D:/data/mobility/projects/study_area/3b724506861960add825434760d69b05-gtfs_router.rds'
# gtfs_route_types_path <- 'D:\\dev\\mobility_oss\\mobility\\data\\gtfs\\gtfs_route_types.xlsx'
# start_time_min <- as.numeric('6.5')
# start_time_max <- as.numeric('7.5')
# max_traveltime <- as.numeric('1.0')
# output_file_path <- 'D:\\data\\mobility\\projects\\study_area\\public_transport_graph\\simplified\\done'


parameters <- fromJSON(parameters)


buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "cpprouting_io.R"))
source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))
source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))

logger <- logger(appenders = console_appender())

# Compute the travel costs between all stops in the GTFS data
transport_zones <- st_read(tz_file_path)
gtfs <- readRDS(gtfs_file_path)
stops <- get_gtfs_stops(gtfs, transport_zones)

gtfs_travel_costs <- compute_gtfs_travel_costs(
  gtfs,
  stops,
  parameters$start_time_min,
  parameters$start_time_max,
  parameters$max_traveltime
)

graph <- create_graph_from_travel_costs(gtfs_travel_costs)

verts <- rbindlist(
  list(
    stops[, list(vertex_id = paste0("dep-", stop_id), x = X, y = Y)],
    stops[, list(vertex_id = paste0("arr-", stop_id), x = X, y = Y)]
  )
)
  

info(logger, "Saving cppRouting graph and vertices coordinates...")

save_cppr_graph(graph, dirname(output_file_path))
write_parquet(verts, file.path(dirname(dirname(output_file_path)), "vertices.parquet"))

file.create(output_file_path)
