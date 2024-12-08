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

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility', # package_path
#   'D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg', # tz_file_path
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\simplified\\9a6f4500ffbf148bfe6aa215a322e045-done', # graph_fp
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\simplified\\9a6f4500ffbf148bfe6aa215a322e045-done', # modal_shift
#   '{"max_travel_time": 0.3333333333333333, "average_speed": 50.0, "shift_time": 10.0, "shortcuts_shift_time": null, "shortcuts_locations": null}', # congestion
#   'True',
#   'D:\\data\\mobility\\projects\\haut-doubs\\7418f0652ae728684d6e6acc42da802d-carpool_travel_costs.parquet' # output_file_path
# )

package_path <- args[1]
tz_file_path <- args[2]
first_leg_graph_fp <- args[3]
last_leg_graph_fp <- args[4]
modal_shift <- args[5]
congestion <- args[6]
output_file_path <- args[7]

buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

modal_shift <- fromJSON(modal_shift)
congestion <- as.logical(congestion)

source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))
source(file.path(package_path, "r_utils", "duplicate_cpprouting_graph.R"))
source(file.path(package_path, "r_utils", "initialize_travel_costs.R"))
source(file.path(package_path, "r_utils", "concatenate_graphs.R"))
source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))
source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

# Load transport zones and selected buildings in each transport zone
transport_zones <- st_read(tz_file_path)

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Load cpprouting graphs and vertices
hash <- strsplit(basename(first_leg_graph_fp), "-")[[1]][1]
start_graph <- read_cppr_graph(dirname(first_leg_graph_fp), hash)
start_verts <- read_parquet(file.path(dirname(dirname(first_leg_graph_fp)), paste0(hash, "-vertices.parquet")))

hash <- strsplit(basename(last_leg_graph_fp), "-")[[1]][1]
last_graph <- read_cppr_graph(dirname(last_leg_graph_fp), hash)
last_verts <- read_parquet(file.path(dirname(dirname(last_leg_graph_fp)), paste0(hash, "-vertices.parquet")))


# Update the travel times based on congestion if needed
if (congestion == TRUE) {
  
  hash <- strsplit(basename(first_leg_graph_fp), "-")[[1]][1]
  updated_times <- read_parquet(file.path(dirname(first_leg_graph_fp), paste0(hash, "-updated-times.parquet")))
  
  start_graph$data <- as.data.table(updated_times)
  last_graph$data <- as.data.table(updated_times)
  
}


info(logger, "Concatenating graphs...")

graph <- concatenate_graphs_carpool(
  start_graph,
  last_graph,
  start_verts,
  last_verts,
  modal_shift,
  transport_zones,
  last_graph_speed_coeff = 0.999
)


travel_costs <- initialize_travel_costs(
  transport_zones,
  buildings_sample,
  start_verts,
  NULL,
  last_verts,
  NULL,
  NULL
)

travel_costs[, vertex_id_from := paste0("s", vertex_id_from)]
travel_costs[, vertex_id_to := paste0("l", vertex_id_to)]

# Remove trips that are very long
travel_costs <- travel_costs[distance < 80e3]

# Compute the travel time between clusters
info(logger, "Computing travel times...")

travel_costs$total_time <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = FALSE
)

# Remove trips that are very long
travel_costs <- travel_costs[total_time < 3600]


graph$original$attrib$aux <- graph$original$attrib$carpooling_time

travel_costs$carpooling_time <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = TRUE
)

travel_costs$car_time <- travel_costs$total_time - travel_costs$carpooling_time


# Remove trips that are 100 % solo driving
travel_costs <- travel_costs[car_time/total_time < 1.0]


# Compute the distances between clusters
info(logger, "Computing travel distances...")

graph$original$attrib$aux <- graph$original$attrib$distance

travel_costs$total_distance <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = TRUE
)


graph$original$attrib$aux <- graph$original$attrib$carpooling_distance

travel_costs$carpooling_distance <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = TRUE
)

travel_costs$car_distance <- travel_costs$total_distance - travel_costs$carpooling_distance




# Aggregate the result by transport zone
travel_costs[, prob := weight_from*weight_to]
travel_costs[, prob := prob/sum(prob), list(from, to)]

travel_costs <- travel_costs[, list(
    car_distance = sum(car_distance*prob)/1000,
    carpooling_distance = sum(carpooling_distance*prob)/1000,
    car_time = sum(car_time*prob)/3600,
    carpooling_time = sum(carpooling_time*prob)/3600
  ),
  by = list(from, to)
]


write_parquet(travel_costs, output_file_path)