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
pt_graph_fp <- args[3]
first_leg_graph_fp <- args[4]
last_leg_graph_fp <- args[5]
first_modal_shift <- args[6]
last_modal_shift <- args[7]
output_file_path <- args[8]

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\study_area\\d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg'
# pt_graph_fp <- "D:\\data\\mobility\\projects\\study_area\\public_transport_graph\\simplified"
# first_leg_graph_fp <- "D:/data/mobility/projects/study_area/path_graph_car/simplified"
# last_leg_graph_fp <- "D:/data/mobility/projects/study_area/path_graph_walk/simplified"
# first_modal_shift <- '{"max_travel_time": 0.33, "average_speed": 5.0, "shift_time": 10.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
# last_modal_shift <- '{"max_travel_time": 0.33, "average_speed": 5.0, "shift_time": 2.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
# output_file_path <- 'D:\\data\\mobility\\projects\\study_area\\92d64787200e7ed83bc8eadf88d3acc4-public_transport_travel_costs.parquet'

first_modal_shift <- fromJSON(first_modal_shift)
last_modal_shift <- fromJSON(last_modal_shift)

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
# Layer 1 : original graph
# Layer 2 : destination graph
# Layer 3 : public transport "shortcuts" from the layer 1 to the layer 2

# This setup prevents the router from jumping back and forth between the first and last
# graphs and the public transport shortcuts (because the router can only go from
# layer 1 to 2 through layer 3, but cannot go back)

# Load cpprouting graphs and vertices
hash <- strsplit(basename(first_leg_graph_fp), "-")[[1]][1]
start_graph <- read_cppr_graph(dirname(first_leg_graph_fp), hash)
start_verts <- read_parquet(file.path(dirname(dirname(first_leg_graph_fp)), paste0(hash, "-vertices.parquet")))

hash <- strsplit(basename(last_leg_graph_fp), "-")[[1]][1]
last_graph <- read_cppr_graph(dirname(last_leg_graph_fp), hash)
last_verts <- read_parquet(file.path(dirname(dirname(last_leg_graph_fp)), paste0(hash, "-vertices.parquet")))

hash <- strsplit(basename(pt_graph_fp), "-")[[1]][1]
mid_graph <- read_cppr_graph(dirname(pt_graph_fp), hash)
mid_verts <- read_parquet(file.path(dirname(dirname(pt_graph_fp)), paste0(hash, "-vertices.parquet")))

# Make sure all vertices are in the graph
# (this should not be needed because vertices are now filtered at graph creation
# still have this issue)
start_verts <- start_verts[vertex_id %in% start_graph$dict$ref]
last_verts <- last_verts[vertex_id %in% last_graph$dict$ref]

# Concatenate all graphs
# (warning : modifies verts in place !)
info(logger, "Concatenating graphs...")

graph <- concatenate_graphs(
  start_graph,
  mid_graph,
  last_graph,
  start_verts,
  mid_verts,
  last_verts,
  first_modal_shift,
  last_modal_shift
)


# Compute the travel time between clusters
info(logger, "Computing travel times and distances...")

travel_costs <- initialize_travel_costs(
  transport_zones,
  buildings_sample,
  start_verts,
  mid_verts,
  last_verts,
  first_modal_shift,
  last_modal_shift,
  graph
)

# x <- travel_costs[from == 1226]
# 
# x$time <- get_distance_pair(
#   graph,
#   from = x$vertex_id_from,
#   to = x$vertex_id_to,
#   aggregate_aux = FALSE
# )
# 
# x[to == 1014]
# 
# x[!(x$vertex_id_to %in% graph$dict$ref)]
# 
# 
# library(ggplot2)
# p <- ggplot(x)
# p <- p + geom_point(aes(x = x_to, y = y_to, color = time/3600))
# p <- p + coord_equal()
# p


# Times
# Total timme
travel_costs$total_time <- get_distance_pair(
  graph,
  from = travel_costs$vertex_id_from,
  to = travel_costs$vertex_id_to,
  aggregate_aux = FALSE
)

travel_costs <- travel_costs[!is.na(total_time)]



get_distance_pair_aux <- function(graph, from, to, aux_name) {
  
  graph$original$attrib$aux <- graph$original$attrib[[aux_name]]
  
  value <- get_distance_pair(
    graph,
    from = travel_costs$vertex_id_from,
    to = travel_costs$vertex_id_to,
    aggregate_aux = TRUE
  )
  
  return(value)
}


for (aux_name in c("start_time", "last_time", "start_distance", "mid_distance", "last_distance")) {
  
  info(logger, paste0("Computing auxiliary variable : ", aux_name))
  
  travel_costs[[aux_name]] <- get_distance_pair_aux(
    graph,
    from = travel_costs$vertex_id_from,
    to = travel_costs$vertex_id_to,
    aux_name = aux_name
  )
  
}

travel_costs$mid_time <- travel_costs$total_time - travel_costs$start_time - travel_costs$last_time


# Debugging code
# start_verts[, vertex_id := paste0("s", vertex_id)]
# last_verts[, vertex_id := paste0("l", vertex_id)]
# 
# tc <- copy(travel_costs)
# tc <- tc[!is.na(time)]
# 
# tc <- merge(tc, buildings_sample[, list(building_id, x_building = x, y_building = y)], by.x = "building_id_from", by.y = "building_id")
# tc <- merge(tc, buildings_sample[, list(building_id, x_building = x, y_building = y)], by.x = "building_id_to", by.y = "building_id", suffixes = c("_from", "_to"))
# 
# 
# 
# x <- tc[from == 1092 & to == 1093]
# 
# path <- get_path_pair(graph, from = x$vertex_id_from, x$vertex_id_to, long = TRUE)
# path <- as.data.table(path)
# 
# 
# 
# path <- merge(path, start_verts, by.x = "node", by.y = "vertex_id", all.x = TRUE, sort = FALSE)
# path <- merge(path, last_verts, by.x = "node", by.y = "vertex_id", all.x = TRUE, sort = FALSE)
# 
# path[, x := ifelse(is.na(x.x), x.y, x.x)]
# path[, y := ifelse(is.na(y.x), y.y, y.x)]
# path[, index := 1:.N, by = list(from, to)]
# 
# 
# 
# pt <- merge(as.data.table(mid_graph$data), mid_graph$dict, by.x = "from", by.y = "id")
# pt <- merge(pt, mid_graph$dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
# pt <- merge(pt, mid_verts, by.x = "ref_from", by.y = "vertex_id")
# pt <- merge(pt, mid_verts, by.x = "ref_to", by.y = "vertex_id", suffixes = c("_from" ,"_to"))
# 
# 
# 
# library(ggplot2)
# p <- ggplot(path)
# # p <- p + geom_point(aes(x = x_to, y = y_to, color = time/3600))
# p <- p + geom_point(aes(x = x, y = y, color = index))
# # p <- p + geom_point(data = tc[from == 1092 & to == 1093], aes(x = x_building_from, y = y_building_from), color = "blue", size = 2)
# # p <- p + geom_point(data = travel_costs[from == 1092], aes(x = x_from, y = y_from), color = "green", size = 2)
# # p <- p + geom_segment(data = pt[abs(x_from - 4019924) < 1000 & abs(y_from - 2688633) < 1000 & dist/60 < 10], aes(x = x_from, y = y_from, xend = x_to, yend = y_to), color = "red")
# p <- p + scale_color_gradientn(colors = viridis::magma(9))
# p <- p + coord_equal()
# p <- p + facet_wrap(~paste(from, to))
# # p <- p + coord_equal(xlim = c(4010000, 4030000), ylim = c(2680000, 2700000))
# p
# 
# p <- ggplot(tc[time/3600 < 0.5, .N, by = list(x_from, y_from)])
# p <- p + geom_point(aes(x = x_from, y = y_from, size = N, color = N), alpha = 0.5)
# p <- p + scale_size_area()
# p <- p + scale_color_gradientn(colors = rev(viridis::viridis(9)))
# p <- p + coord_equal()
# p <- p + theme_minimal()
# p

# tt_map <- merge(transport_zones, travel_costs[from == 902, list(transport_zone_id = to, time)], by = "transport_zone_id", all.x = TRUE)
# plot(tt_map[, "time"])

  

# Aggregate the result by transport zone
travel_costs[, prob := weight_from*weight_to]
travel_costs[, prob := prob/sum(prob), list(from, to)]

travel_costs <- travel_costs[,
  list(
    start_distance = weighted.mean(start_distance, prob)/1000,
    start_time = weighted.mean(start_time, prob)/3600,
    mid_distance = weighted.mean(mid_distance, prob)/1000,
    mid_time = weighted.mean(mid_time, prob)/3600,
    last_distance = weighted.mean(last_distance, prob)/1000,
    last_time = weighted.mean(last_time, prob)/3600
  ),
  by = list(from, to)
]


write_parquet(travel_costs, output_file_path)