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

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\modified\\7e93e3fe005c640299f4a3c23d2c8ab0-car-modified-path-graph',
#   'D:\\data\\mobility\\projects\\haut-doubs\\9da6c9b51734ddd0278a650c3b00fe30-transport_zones.gpkg',
#   'True',
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\simplified\\flows.parquet',
#   '1.0',
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\congested\\6666b03c21c085dfa46927e0a578b0aa-car-congested-path-graph'
# )

package_fp <- args[1]
cppr_graph_fp <- args[2]
transport_zones_fp <- args[3]
congestion <- args[4]
flows_fp <- args[5]
congestion_flows_scaling_factor <- args[6]
output_fp <- args[7]


source(file.path(package_fp, "r_utils", "cpprouting_io.R"))
source(file.path(package_fp, "r_utils", "tz_pairs_to_vertex_pairs.R"))
source(file.path(package_fp, "r_utils", "get_buildings_nearest_vertex_id.R"))

logger <- logger(appenders = console_appender())

congestion <- as.logical(congestion)
congestion_flows_scaling_factor <- as.numeric(congestion_flows_scaling_factor)

# Load the cpprouting graph
info(logger, "Loading simplified/modified graph...")
hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))

# If OD flows were provided, estimate the congestion on each link and update
# the travel times 
if (congestion == TRUE & file.exists(flows_fp)) {
  
  info(logger, "Loading OD flows...")
  
  # Load OD flows, transport zones and representative buildings and disagregate
  # each flow between transport zones into flows between network vertices
  od_flows <- as.data.table(read_parquet(flows_fp))
  
  if (any(is.na(od_flows$vehicle_volume))) {
    stop("Cannot assign traffic, some OD flows volumes are NA.")
  }
  
  transport_zones <- st_read(transport_zones_fp, quiet = TRUE)
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
  od_flows[, vehicle_volume := vehicle_volume*weight]
  
  # Retain only the largest flows accounting for 95 % of the total volume
  # and upscale them to match the total volume
  od_flows <- od_flows[order(-vehicle_volume)]
  od_flows[, cum_share := cumsum(vehicle_volume)/sum(vehicle_volume)]
  od_flows <- od_flows[cum_share < 0.95]
  od_flows[, vehicle_volume := vehicle_volume/0.95*congestion_flows_scaling_factor]
  
  # Assign traffic 
  info(logger, "Assigning traffic...")
  
  print(summary(cppr_graph$data))
  print(summary(cppr_graph$dict))
  print(summary(cppr_graph$attrib$cap))
  
  print(summary(od_flows$vertex_id_from))
  print(summary(od_flows$vertex_id_to))
  print(summary(od_flows$vehicle_volume))
  
  
  traffic <- assign_traffic(
    cppr_graph,
    from = od_flows$vertex_id_from,
    to = od_flows$vertex_id_to,
    demand = od_flows$vehicle_volume,
    algorithm = "cfw",
    aon_method = "cbi",
    max_gap = 0.05,
    max_it = 10,
    verbose = TRUE
  )
  
  # Update travel times
  cppr_graph$data$dist <- traffic$data$cost
  
  # Save the updated travel times
  hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
  write_parquet(cppr_graph$data, file.path(dirname(cppr_graph_fp), paste0(hash, "-updated-times.parquet")))
  
}

# Save the graph
info(logger, "Saving congested graph...")
hash <- strsplit(basename(output_fp), "-")[[1]][1]
save_cppr_graph(cppr_graph, dirname(output_fp), hash)
write_parquet(vertices, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))

file.create(output_fp)
