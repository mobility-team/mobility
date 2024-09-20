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

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]

tz_file_path <- args[2]

gtfs_file_path <- args[3]
gtfs_route_types_path <- args[4]

start_time_min <- as.numeric(args[5])
start_time_max <- as.numeric(args[6])
max_traveltime <- as.numeric(args[7])

walk_graph_fp <- args[8]

output_file_path <- args[9]


# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\study_area\\d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg'
# gtfs_file_path <- 'D:/data/mobility/projects/study_area/3b724506861960add825434760d69b05-gtfs_router.rds'
# gtfs_route_types_path <- 'D:\\dev\\mobility_oss\\mobility\\data\\gtfs\\gtfs_route_types.xlsx'
# start_time_min <- as.numeric('6.5')
# start_time_max <- as.numeric('7.5')
# max_traveltime <- as.numeric('1.0')
# max_walking_time <- as.numeric('0.167')
# max_walking_speed <- as.numeric('5')
# walk_graph_fp <- "D:/data/mobility/projects/study_area/path_graph_walk/simplified"
# output_file_path <- 'D:\\data\\mobility\\projects\\study_area\\92d64787200e7ed83bc8eadf88d3acc4-public_transport_travel_costs.parquet'
# 


buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))
source(file.path(package_path, "r_utils", "duplicate_cpprouting_graph.R"))
source(file.path(package_path, "r_utils", "initialize_travel_costs.R"))
source(file.path(package_path, "r_utils", "concatenate_graphs.R"))
source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))
source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Create a three layer routing graph
# Layer 1 : original walk graph
# Layer 2 : destination walk graph (duplicate of the layer 1)
# Layer 3 : public transport "shortcuts" from the layer 1 to the layer 2

# This setup prevents the router from jumping back and forth between the walk
# graph and the public transport shortcuts (because the router can only go from
# layer 1 to 2 through layer 3, but cannot go back)

# Load walk cpprouting graph and vertices
graph_1 <- read_cppr_graph(walk_graph_fp)
verts_1 <- read_parquet(file.path(dirname(walk_graph_fp), "vertices.parquet"))

graph_2 <- duplicate_cpprouting_graph(graph_1, verts_1)
verts_2 <- graph_2[["vertices"]]
graph_2 <- graph_2[["graph"]]

# Compute the travel costs between all stops in the GTFS data
gtfs <- readRDS(gtfs_file_path)
stops <- get_gtfs_stops(gtfs, transport_zones)

gtfs_travel_costs <- compute_gtfs_travel_costs(
  gtfs,
  stops,
  start_time_min,
  start_time_max,
  max_traveltime,
  gtfs_route_types_path
)

graph_3 <- create_graph_from_travel_costs(
  gtfs_travel_costs,
  stops,
  graph_1,
  verts_1,
  graph_2,
  verts_2
)

graph <- concatenate_graphs(graph_1, graph_2, graph_3)


travel_costs <- initialize_travel_costs(
  transport_zones,
  buildings_sample,
  verts_1,
  verts_2
)

# Compute the travel time between clusters
info(logger, "Computing travel times...")

travel_costs$time <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = FALSE
)

# Compute the distances between clusters
info(logger, "Computing travel distances...")

travel_costs$distance <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = TRUE
)


# Aggregate the result by transport zone
travel_costs[, prob := weight_from*weight_to]
travel_costs[, prob := prob/sum(prob), list(from, to)]

travel_costs <- travel_costs[, list(
    distance = weighted.mean(distance, prob),
    time = weighted.mean(time, prob)
  ),
  by = list(from, to)
]

travel_costs[, distance := distance/1000]
travel_costs[, time := time/3600]
travel_costs <- travel_costs[time < max_traveltime]

write_parquet(travel_costs, output_file_path)