library(dodgr)
library(osmdata)
library(log4r)
library(sf)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)
library(FNN)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
cppr_graph_fp <- args[2]
od_flows_fp <- args[3]
traffic_fp <- args[4]
output_fp <- args[5]

package_path <- "D:/dev/mobility_oss/mobility"
cppr_graph_fp <- "D:/data/mobility/projects/experiments/path_graph_car/simplified/62b22e6f35a51af629cb515adbc5234e-done"
od_flows_fp <- "D:/data/mobility/projects/experiments/path_graph_car/simplified/62b22e6f35a51af629cb515adbc5234e-od_flows.parquet"
output_fp <- "D:/data/mobility/projects/experiments/path_graph_car/simplified/62b22e6f35a51af629cb515adbc5234e-traffic.parquet"

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

hash <- strsplit(basename(cppr_graph_path), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_path), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))


od_flows <- as.data.table(read_parquet(od_flows_fp))

transport_zones <- st_read("D:/data/mobility/projects/experiments/90c5d8004cebc13d3c4fdfa89d17d8ce-transport_zones.gpkg")
buildings <- read_parquet("D:/data/mobility/projects/experiments/90c5d8004cebc13d3c4fdfa89d17d8ce-transport_zones_buildings.parquet")

buildings[, building_id := 1:.N]
buildings[, vertex_id := get_buildings_nearest_vertex_id(buildings, vertices)]
buildings <- merge(buildings[, list(building_id, transport_zone_id, n_clusters, weight, vertex_id)], vertices, by = "vertex_id")

vertex_pairs <- tz_pairs_to_vertex_pairs(
  tz_id_from = od_flows$from,
  tz_id_to = od_flows$to,
  transport_zones = as.data.table(transport_zones),
  buildings = buildings,
  max_crowfly_time = 10.0
)

od_flows <- merge(od_flows, vertex_pairs, by.x = c("from", "to"), by.y = c("tz_id_from", "tz_id_to"))
od_flows[, flow_volume := flow_volume*weight]


traffic <- assign_traffic(
  cppr_graph,
  from = od_flows$vertex_id_from,
  to = od_flows$vertex_id_to,
  demand = od_flows$flow_volume*4,
  aon_method = "cbi"
)

write_parquet(as.data.table(traffic), output_fp)

