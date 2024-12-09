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
intermodal_graph_fp <- args[3]
first_modal_shift <- args[4]
last_modal_shift <- args[5]
output_file_path <- args[6]

# package_path <- 'D:/dev/mobility_oss/mobility'
# tz_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg'
# pt_graph_fp <- "D:/data/mobility/projects/haut-doubs/public_transport_graph/simplified/3d807c3ccc9ee020cbabb3e281f5635a-done"
# intermodal_graph_fp <- "D:/data/mobility/projects/haut-doubs/walk_public_transport_walk_intermodal_transport_graph/simplified/dad2d53274998829d5a595ee27df908a-done"
# first_leg_graph_fp <- "D:/data/mobility/projects/haut-doubs/path_graph_car/contracted/60c7d5b22e428d3fbea41ac0ffadd067-done"
# last_leg_graph_fp <- "D:/data/mobility/projects/haut-doubs/path_graph_walk/contracted/3320481ff138926f18a6f45ced9d511e-done"
# first_modal_shift <- '{"max_travel_time": 0.3333333333333333, "average_speed": 50.0, "shift_time": 5.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
# last_modal_shift <- '{"max_travel_time": 0.3333333333333333, "average_speed": 5.0, "shift_time": 1.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'
# output_file_path <- 'D:/data/mobility/projects/experiments/f65f378d4dc11f0929cf7f2aeaa2aaf5-public_transport_travel_costs.parquet'


first_modal_shift <- fromJSON(first_modal_shift)
last_modal_shift <- fromJSON(last_modal_shift)

buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

# source(file.path(package_path, "r_utils", "compute_gtfs_travel_costs.R"))
# source(file.path(package_path, "r_utils", "duplicate_cpprouting_graph.R"))
# source(file.path(package_path, "r_utils", "initialize_travel_costs.R"))
# source(file.path(package_path, "r_utils", "concatenate_graphs.R"))
# source(file.path(package_path, "r_utils", "create_graph_from_travel_costs.R"))
source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path)
transport_zones <- as.data.table(st_drop_geometry(transport_zones))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

hash <- strsplit(basename(intermodal_graph_fp), "-")[[1]][1]
intermodal_graph <- read_cppr_graph(dirname(intermodal_graph_fp), hash)
intermodal_verts <- as.data.table(read_parquet(file.path(dirname(dirname(intermodal_graph_fp)), paste0(hash, "-vertices.parquet"))))

# Compute crowfly distances between transport zones to compute the number of 
# points within the origin and destination zones that should be used
# (between 5 for )
travel_costs <- CJ(
  from = transport_zones$transport_zone_id,
  to = transport_zones$transport_zone_id
)

travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "from", by.y = "transport_zone_id")
travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))

travel_costs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
travel_costs <- travel_costs[distance < 80e3]

travel_costs[, n_clusters := round(1 + 4*exp(-distance/1000/2))]


travel_costs <- merge(
  travel_costs,
  buildings_sample[, list(transport_zone_id, n_clusters, building_id, x, y, weight)],
  by.x = c("from", "n_clusters"),
  by.y = c("transport_zone_id", "n_clusters"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

travel_costs <- merge(
  travel_costs,
  buildings_sample[, list(transport_zone_id, n_clusters, building_id, x, y, weight)],
  by.x = c("to", "n_clusters"),
  by.y = c("transport_zone_id", "n_clusters"),
  all.x = TRUE,
  suffixes = c("_from_cluster", "_to_cluster"),
  allow.cartesian = TRUE
)

travel_costs <- travel_costs[building_id_from_cluster != building_id_to_cluster]


# Match transport zones centroids with graph vertices
start_verts <- intermodal_verts[grepl("s1-", vertex_id), list(vertex_id, x, y)]

knn_start <- get.knnx(
  start_verts[, list(x, y)],
  buildings_sample[, list(x, y)],
  k = 1
)

buildings_sample[, vertex_id_from := start_verts$vertex_id[knn_start$nn.index]]


last_verts <- intermodal_verts[grepl("l2-", vertex_id), list(vertex_id, x, y)]

knn_last <- get.knnx(
  last_verts[, list(x, y)],
  buildings_sample[, list(x, y)],
  k = 1
)

buildings_sample[, vertex_id_to := last_verts$vertex_id[knn_last$nn.index]]


travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, weight_from = weight, vertex_id_from)], by.x = "building_id_from_cluster", by.y = "building_id")
travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, weight_to = weight, vertex_id_to)], by.x = "building_id_to_cluster", by.y = "building_id")


# xy <- travel_costs[
#   from %in% transport_zones[local_admin_unit_id == "fr-25462", transport_zone_id] & 
#     to %in% transport_zones[local_admin_unit_id == "ch-5764", transport_zone_id] 
# ]
# 
# p <- ggplot(xy)
# p <- p + geom_point(aes(x = x_from, y = y_from))
# p
# 
# get_distance_pair(
#   intermodal_graph,
#   xy$vertex_id_from,
#   xy$vertex_id_to
# )
# 
# paths <- get_multi_paths(
#   intermodal_graph,
#   xy$vertex_id_from,
#   xy$vertex_id_to,
#   long = TRUE
# )
# 
# paths <- paths[!duplicated(paths), ]
# 
# paths <- as.data.table(paths)
# paths <- merge(paths, intermodal_verts, by.x = "node", by.y = "vertex_id", sort = FALSE)
# paths[, index := 1:.N, by = list(from, to)]
# paths[, od := .GRP, by = list(from, to)]
# paths[, leg := substr(node, 1, 1)]
# 
# paths <- merge(paths, intermodal_graph$dict, by.x = "node", by.y = "ref", sort = FALSE)
# paths[, previous_id := shift(id, type = "lag", n = 1), by = list(from, to)]
# 
# paths <- merge(paths, intermodal_graph$data, by.x = c("previous_id", "id"), by.y = c("from", "to"), all.x = TRUE, sort = FALSE)
# 
# p <- ggplot(paths)
# p <- p + geom_path(aes(x = x, y = y, color = leg))
# p <- p + geom_point(aes(x = x, y = y, color = leg))
# # p <- p + geom_text_repel(aes(x = x, y = y, label = node))
# p <- p + facet_wrap(~od)
# p <- p + coord_equal()
# p
# 
# fwrite(paths, "paths.csv")
# 
# 
# lines <- as.data.table(intermodal_graph$data)
# lines <- merge(lines, intermodal_graph$dict, by.x = "from", by.y = "id")
# lines <- merge(lines, intermodal_graph$dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
# 
# lines <- merge(lines, intermodal_verts, by.x = "ref_from", by.y = "vertex_id")
# lines <- merge(lines, intermodal_verts, by.x = "ref_to", by.y = "vertex_id", suffixes = c("_from", "_to"))
# 
# lines[, d := sqrt((x_to-x_from)^2 + (y_to-y_from)^2)]
# lines <- lines[d > 0 & dist > 0.0]
# 
# lines <- lines[, list(edge_id = paste(ref_from, ref_to), x_from, y_from, x_to, y_to, dist)]
# 
# lines_long <- rbind(
#   lines[, .(edge_id, x = x_from, y = y_from, dist, point_id = 1)],
#   lines[, .(edge_id, x = x_to, y = y_to, dist, point_id = 2)]
# )[order(edge_id, point_id)]
# 
# linestrings <- sfheaders::sf_linestring(
#   obj = lines_long,
#   x = "x",
#   y = "y",
#   linestring_id = "edge_id",
#   keep = TRUE
# )
# 
# st_crs(linestrings) <- 3035
# 
# st_write(linestrings, "intermodal.gpkg", delete_dsn = TRUE)
# 
# graph <- as.data.table(intermodal_graph$data)
# dict <- as.data.table(intermodal_graph$dict)
# 
# graph <- merge(graph, dict, by.x = "from", by.y = "id")
# graph <- merge(graph, dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
# 
# graph[ref_from == "s1-7431598429"]



# Compute the travel costs
info(logger, "Computing travel costs...")

graph_c <- cpp_contract(intermodal_graph)

travel_costs$total_time <- get_distance_pair(
  graph_c,
  travel_costs$vertex_id_from,
  travel_costs$vertex_id_to
)

travel_costs <- travel_costs[!is.na(total_time)]

modal_shift_time <- 60*(first_modal_shift$shift_time + last_modal_shift$shift_time)
travel_costs[, total_time := total_time + modal_shift_time]

travel_costs <- travel_costs[total_time < 3600]

get_distance_pair_aux <- function(graph, from, to, aux_name) {
  
  graph$attrib$aux <- graph$attrib[[aux_name]]
  
  value <- get_distance_pair(
    graph,
    from = from,
    to = to,
    algorithm = "NBA",
    constant = 0.1,
    aggregate_aux = TRUE
  )
  
  return(value)
}


for (aux_name in c("start_time", "last_time", "start_distance", "mid_distance", "last_distance")) {
  
  info(logger, paste0("Computing auxiliary variable : ", aux_name))
  
  travel_costs[[aux_name]] <- get_distance_pair_aux(
    intermodal_graph,
    from = travel_costs$vertex_id_from,
    to = travel_costs$vertex_id_to,
    aux_name = aux_name
  )
  
  # Remove trips that don't rely enough on public transport
  if (aux_name == "start_time") {
    travel_costs <- travel_costs[start_time/total_time < 0.5]
  }
  
  if (aux_name == "last_time") {
    travel_costs <- travel_costs[last_time/total_time < 0.5]
  }
  
}


travel_costs[, mid_time := total_time - start_time - last_time]

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