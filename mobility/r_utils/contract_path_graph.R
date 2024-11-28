library(dodgr)
library(osmdata)
library(log4r)
library(sf)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)

args <- commandArgs(trailingOnly = TRUE)

package_fp <- args[1]
cppr_graph_fp <- args[2]
transport_zones_fp <- args[3]
flows_fp <- args[4]
output_fp <- args[5]

# package_fp <- "D:/dev/mobility_oss/mobility"
# cppr_graph_fp <- "D:/data/mobility/projects/experiments/path_graph_car/simplified/62b22e6f35a51af629cb515adbc5234e-done"
# transport_zones_fp <- "D:/data/mobility/projects/experiments/90c5d8004cebc13d3c4fdfa89d17d8ce-transport_zones.gpkg"
# flows_fp <- "D:/data/mobility/projects/experiments/path_graph_car/simplified/flows.parquet"
# output_fp <- "D:/data/mobility/projects/experiments/path_graph_car/contracted/5ca9ce47abdf6f3f5977f01ea64a785e-done"


source(file.path(package_fp, "r_utils", "cpprouting_io.R"))
source(file.path(package_fp, "r_utils", "tz_pairs_to_vertex_pairs.R"))
source(file.path(package_fp, "r_utils", "get_buildings_nearest_vertex_id.R"))

logger <- logger(appenders = console_appender())

# Load the cpprouting graph
hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))

# If OD flows were provided, estimate the congestion on each link and update
# the travel times 
if (file.exists(flows_fp)) {
  
  # Load OD flows, transport zones and representative buildings and disaggregate
  # each flow between transport zones into flows between network vertices
  od_flows <- as.data.table(read_parquet(flows_fp))
  
  transport_zones <- st_read(transport_zones_fp)
  transport_zones <- as.data.table(st_drop_geometry(transport_zones))
  
  buildings_fp <- file.path(
    dirname(transport_zones_fp),
    paste0(
      gsub("-transport_zones.gpkg", "", basename(transport_zones_fp)),
      "-transport_zones_buildings.parquet"
    )
  )
  
  buildings <- as.data.table(read_parquet(buildings_fp))
  buildings[, building_id := 1:.N]
  buildings[, vertex_id := get_buildings_nearest_vertex_id(buildings, vertices)]
  buildings <- merge(buildings[, list(building_id, transport_zone_id, n_clusters, weight, vertex_id)], vertices, by = "vertex_id")
  
  vertex_pairs <- tz_pairs_to_vertex_pairs(
    tz_id_from = od_flows$from,
    tz_id_to = od_flows$to,
    transport_zones = as.data.table(transport_zones),
    buildings = buildings
  )
  
  od_flows <- merge(od_flows, vertex_pairs, by.x = c("from", "to"), by.y = c("tz_id_from", "tz_id_to"))
  od_flows[, flow_volume := flow_volume*weight]
  
  # Assign traffic 
  traffic <- assign_traffic(
    cppr_graph,
    from = od_flows$vertex_id_from,
    to = od_flows$vertex_id_to,
    demand = od_flows$flow_volume,
    aon_method = "cbi",
    max_gap = 0.01,
    max_it = 20
  )
  
  # Update travel times
  cppr_graph$data$dist <- traffic$data$cost
  
}

# Contract the graph and save it
cppr_graph <- cpp_contract(cppr_graph)

hash <- strsplit(basename(output_fp), "-")[[1]][1]
save_cppr_contracted_graph(cppr_graph, dirname(output_fp), hash)
write_parquet(vertices, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))

file.create(output_fp)
