library(dodgr)
library(osmdata)
library(log4r)
library(cppRoutingCCH)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)
library(sf)
library(FNN)

args <- commandArgs(trailingOnly = TRUE)

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

# Shared inputs are loaded once. All assignments start from the same road graph.
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

cch_hash <- strsplit(basename(cch_graph_fp), "-")[[1]][1]
cch <- read_cppr_cch(dirname(cch_graph_fp), cch_hash, graph = cppr_graph)
# Prepare the travel-time OD requests once for every scenario.
max_beeline_distance <- as.numeric(args[13])
buildings_sample_fp <- buildings_fp
transport_zones <- st_read(transport_zones_fp)
transport_zones <- as.data.table(st_drop_geometry(transport_zones))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Compute crowfly distances between transport zones to choose how many
# representative buildings to use inside each origin and destination zone.
travel_costs <- CJ(
  from = transport_zones$transport_zone_id,
  to = transport_zones$transport_zone_id
)

travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "from", by.y = "transport_zone_id")
travel_costs <- merge(travel_costs, transport_zones[, list(transport_zone_id, x, y)], by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))

travel_costs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
travel_costs <- travel_costs[distance < max_beeline_distance * 1000]

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

# Match buildings with graph vertices. The OD vertex map is the normal path;
# nearest-neighbour fallback is kept for older graph caches without that map.
if (!is.null(od_vertex_map)) {
  buildings_sample <- merge(
    buildings_sample,
    od_vertex_map[, list(building_id, vertex_id)],
    by = "building_id",
    all.x = TRUE,
    sort = FALSE
  )

  if (any(is.na(buildings_sample$vertex_id))) {
      stop("OD vertex map is incomplete for travel-cost preparation.")
  }
} else {
  knn <- get.knnx(
    vertices[, list(x, y)],
    buildings_sample[, list(x, y)],
    k = 1
  )

  buildings_sample[, vertex_id := vertices$vertex_id[knn$nn.index]]
}

graph_refs <- as.character(cppr_graph$dict$ref)
buildings_sample[, vertex_id := as.character(vertex_id)]
if (any(!buildings_sample$vertex_id %in% graph_refs)) {
  stop("Some building vertex ids are missing from the graph dictionary.")
}

travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, vertex_id)], by.x = "building_id_from_cluster", by.y = "building_id")
travel_costs <- merge(travel_costs, buildings_sample[, list(building_id, vertex_id)], by.x = "building_id_to_cluster", by.y = "building_id", suffixes = c("_from", "_to"))

flow_paths <- unlist(fromJSON(flows_fp))
wide <- NULL
flow_paths <- unlist(fromJSON(flows_fp))
for (i in seq_along(flow_paths)) {
  info(logger, paste0("Assigning scenario ", i, " of ", length(flow_paths)))
  od_flows <- as.data.table(read_parquet(flow_paths[[i]]))
  if (any(is.na(od_flows$vehicle_volume))) stop("OD flow volumes must not be NA")
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
  
  # R objects can be modified by native code; give each scenario a clean graph.
  scenario_graph <- unserialize(serialize(cppr_graph, NULL))
  # Assign traffic
  info(logger, "Assigning traffic with CCH queries...")
  
  traffic <- assign_traffic(
    scenario_graph,
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
  scenario_graph$data$dist <- traffic$data$cost
  
  # Route this assignment immediately without saving a congested graph.
  metric <- cpp_cch_customize(cch, scenario_graph$data$dist)
  path_values <- get_path_values_pair(
    metric, from = travel_costs$vertex_id_from, to = travel_costs$vertex_id_to,
    values = data.frame(distance = scenario_graph$attrib$aux)
  )
  travel_costs[, time := path_values$cost]
  travel_costs[, distance := path_values$distance]
  reachable <- travel_costs[!is.na(time) & !is.na(distance)]
  reachable[, prob := weight_from_cluster * weight_to_cluster]
  reachable[, prob := prob / sum(prob), list(from, to)]
  scenario_costs <- reachable[, list(
    distance = weighted.mean(distance, prob),
    time = weighted.mean(time, prob)
  ), by = list(from, to)]
  scenario_costs[, distance := distance / 1000]
  scenario_costs[, time := time / 3600]
  names_for_scenario <- paste0(c("distance_", "time_"), i - 1L)
  setnames(scenario_costs, c("distance", "time"), names_for_scenario)
  if (is.null(wide)) {
    wide <- scenario_costs
  } else if (identical(wide$from, scenario_costs$from) && identical(wide$to, scenario_costs$to)) {
    # The usual case: identical OD order needs no join or duplicate keys.
    for (column in names_for_scenario) set(wide, j = column, value = scenario_costs[[column]])
  } else {
    # A missing route can change OD coverage; preserve the union and mark gaps NA.
    wide <- merge(wide, scenario_costs, by = c("from", "to"), all = TRUE, sort = FALSE)
  }
  rm(metric, path_values, reachable, scenario_costs, scenario_graph, traffic)
}

write_parquet(wide, output_fp)
