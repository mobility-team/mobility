library(dodgr)
library(osmdata)
library(log4r)
library(cppRoutingCCH)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\modified\\a4d8d9065e1217e8f5ef1ca48cca4789-car-modified-path-graph',
#   'D:\\data\\mobility\\projects\\grand-geneve\\07506b75cdf292b33559375be3029b0c-transport_zones.gpkg',
#   'True',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\simplified\\flows.parquet',
#   '0.1',
#   '1000.0',
#   '10',
#   '0.05',
#   '0.95',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\congested\\7e5144cf3db620565c9a9797be8a6df0-car-congested-path-graph'
# )

package_fp <- args[1]
cppr_graph_fp <- args[2]
transport_zones_fp <- args[3]
congestion <- args[4]
flows_fp <- args[5]
cch_graph_fp <- args[6]
congestion_flows_scaling_factor <- args[7]
target_max_vehicles_per_od_endpoint <- args[8]
congestion_assignment_max_iterations <- args[9]
congestion_assignment_max_gap <- args[10]
congestion_assignment_retained_volume_share <- args[11]
output_fp <- args[12]


source(file.path(package_fp, "transport", "graphs", "core", "cpprouting_io.R"))
source(file.path(package_fp, "transport", "graphs", "congested", "tz_pairs_to_vertex_pairs.R"))

logger <- logger(appenders = console_appender())

congestion <- as.logical(congestion)
congestion_flows_scaling_factor <- as.numeric(congestion_flows_scaling_factor)
target_max_vehicles_per_od_endpoint <- as.numeric(target_max_vehicles_per_od_endpoint)
congestion_assignment_max_iterations <- as.integer(congestion_assignment_max_iterations)
congestion_assignment_max_gap <- as.numeric(congestion_assignment_max_gap)
congestion_assignment_retained_volume_share <- as.numeric(congestion_assignment_retained_volume_share)

if (is.na(congestion_assignment_max_iterations) || congestion_assignment_max_iterations < 1) {
  stop("congestion_assignment_max_iterations must be an integer greater than or equal to 1.")
}
if (is.na(congestion_assignment_max_gap) || congestion_assignment_max_gap <= 0) {
  stop("congestion_assignment_max_gap must be greater than 0.")
}
if (
  is.na(congestion_assignment_retained_volume_share) ||
  congestion_assignment_retained_volume_share <= 0 ||
  congestion_assignment_retained_volume_share > 1
) {
  stop("congestion_assignment_retained_volume_share must be greater than 0 and less than or equal to 1.")
}

# Load the cpprouting graph
info(logger, "Loading simplified/modified graph...")
hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))
od_vertex_map_fp <- file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-od-vertex-map.parquet"))
if (!file.exists(od_vertex_map_fp)) {
  stop("Missing OD vertex map for the input graph.")
}
od_vertex_map <- as.data.table(read_parquet(od_vertex_map_fp))

# If OD flows were provided, estimate the congestion on each link and update
# the travel times 
if (congestion == TRUE & file.exists(flows_fp)) {
  
  info(logger, "Loading OD flows...")
  
  # Load OD flows, transport zones and representative buildings and disagregate
  # each flow between transport zones into flows between network vertices
  od_flows <- as.data.table(read_parquet(flows_fp))
  info(logger, paste0("Loaded ", format(nrow(od_flows), big.mark = ","), " OD flow rows."))
  
  if (any(is.na(od_flows$vehicle_volume))) {
    stop("Cannot assign traffic, some OD flows volumes are NA.")
  }
  
  buildings_fp <- file.path(
    dirname(transport_zones_fp),
    paste0(
      gsub("-transport_zones.gpkg", "", basename(transport_zones_fp)),
      "-transport_zones_buildings.parquet"
    )
  )
  
  buildings <- as.data.table(read_parquet(buildings_fp))
  info(logger, paste0("Loaded ", format(nrow(buildings), big.mark = ","), " representative building rows."))
  buildings[, building_id := 1:.N]
  buildings <- merge(
    buildings,
    od_vertex_map[, list(building_id, vertex_id)],
    by = "building_id",
    all.x = TRUE,
    sort = FALSE
  )
  info(logger, paste0("Matched buildings to OD vertex map, ", format(nrow(buildings), big.mark = ","), " rows."))

  if (any(is.na(buildings$vertex_id))) {
    stop("OD vertex map is incomplete for the current buildings sample.")
  }

  buildings <- merge(buildings[, list(building_id, transport_zone_id, n_clusters, weight, vertex_id)], vertices, by = "vertex_id")
  info(logger, paste0("Attached graph coordinates to buildings, ", format(nrow(buildings), big.mark = ","), " rows."))

  info(logger, "Disaggregating OD flows to graph vertex pairs...")
  vertex_pairs <- tz_pairs_to_vertex_pairs(
    tz_id_from = od_flows$from,
    tz_id_to = od_flows$to,
    buildings = buildings,
    vehicle_volume = od_flows$vehicle_volume,
    congestion_flows_scaling_factor = congestion_flows_scaling_factor,
    target_max_vehicles_per_od_endpoint = target_max_vehicles_per_od_endpoint
  )
  info(logger, paste0("Generated ", format(nrow(vertex_pairs), big.mark = ","), " vertex-pair rows."))
  
  od_flows <- merge(od_flows, vertex_pairs, by.x = c("from", "to"), by.y = c("tz_id_from", "tz_id_to"))
  od_flows[, vehicle_volume := vehicle_volume*weight]
  info(logger, paste0("Expanded OD flows to ", format(nrow(od_flows), big.mark = ","), " weighted vertex-flow rows."))
  
  # Retain only the largest flows accounting for the requested total-volume share
  # and upscale them to match the total volume
  info(logger, "Sorting vertex-flow rows by decreasing vehicle volume...")
  od_flows <- od_flows[order(-vehicle_volume)]
  info(logger, "Computing cumulative vehicle volume shares...")
  od_flows[, cum_share := cumsum(vehicle_volume)/sum(vehicle_volume)]
  info(logger, paste0("Filtering vertex-flow rows to retain the top ", congestion_assignment_retained_volume_share * 100, "% of total volume..."))
  od_flows[, flow_rank := .I]
  od_flows <- od_flows[cum_share <= congestion_assignment_retained_volume_share | flow_rank == 1]
  info(logger, "Rescaling retained vehicle volumes after volume-share filtering...")
  od_flows[, vehicle_volume := vehicle_volume/congestion_assignment_retained_volume_share*congestion_flows_scaling_factor]
  od_flows[, flow_rank := NULL]
  info(logger, paste0("Retained ", format(nrow(od_flows), big.mark = ","), " vertex-flow rows after volume-share filtering."))
  
  info(logger, "Loading CCH topology for traffic assignment...")
  cch_hash <- strsplit(basename(cch_graph_fp), "-")[[1]][1]
  cch <- read_cppr_cch(dirname(cch_graph_fp), cch_hash, graph = cppr_graph)

  # Assign traffic
  info(logger, "Assigning traffic with CCH queries...")
  
  traffic <- assign_traffic(
    cppr_graph,
    from = od_flows$vertex_id_from,
    to = od_flows$vertex_id_to,
    demand = od_flows$vehicle_volume,
    algorithm = "cfw",
    aon_method = "cch",
    cch = cch,
    max_gap = congestion_assignment_max_gap,
    max_it = congestion_assignment_max_iterations,
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
write_parquet(od_vertex_map, file.path(dirname(dirname(output_fp)), paste0(hash, "-od-vertex-map.parquet")))

file.create(output_fp)
