

tz_pairs_to_vertex_pairs <- function(
    tz_id_from,
    tz_id_to,
    transport_zones,
    buildings,
    max_crowfly_speed = 110.0,
    max_crowfly_time = 1.0
) {
  
  # Compute the crowfly distance and travel time between transport zones centers, 
  # the number of representative buildings to use for each transport zone pair,
  # and filter out pairs that are too far away
  pairs <- data.table(
    tz_id_from = tz_id_from,
    tz_id_to = tz_id_to
  ) 
  
  pairs <- merge(pairs, transport_zones[, list(transport_zone_id, x, y)], by.x = "tz_id_from", by.y = "transport_zone_id")
  pairs <- merge(pairs, transport_zones[, list(transport_zone_id, x, y)], by.x = "tz_id_to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))
  
  pairs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
  pairs[, time := distance/1000/max_crowfly_speed]
  pairs[, n_clusters := round(1 + 4*exp(-distance/1000/2))]
  
  pairs <- pairs[time < max_crowfly_time, list(tz_id_from, tz_id_to, n_clusters)]
  
  # Merge the tz pairs with the representative buildings locations and weights
  # Compute the crowfly distance between these buildings, agregating weights
  # when there are duplicates (which can happen when buildings are mapped 
  # to the same network vertex)
  pairs <- merge(
    pairs,
    buildings[, list(transport_zone_id, n_clusters, vertex_id, x, y, weight)],
    by.x = c("tz_id_from", "n_clusters"),
    by.y = c("transport_zone_id", "n_clusters"),
    all.x = TRUE,
    allow.cartesian = TRUE
  )
  
  pairs <- merge(
    pairs,
    buildings[, list(transport_zone_id, n_clusters, vertex_id, x, y, weight)],
    by.x = c("tz_id_to", "n_clusters"),
    by.y = c("transport_zone_id", "n_clusters"),
    all.x = TRUE,
    suffixes = c("_from", "_to"),
    allow.cartesian = TRUE
  )
  
  pairs <- pairs[vertex_id_from != vertex_id_to]
  pairs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
  
  pairs[, weight := weight_from*weight_to]
  pairs[, weight := weight/sum(weight), list(tz_id_from, tz_id_to)]
  
  pairs <- pairs[, list(weight = sum(weight), distance = mean(distance)), by = list(tz_id_from, tz_id_to, vertex_id_from, vertex_id_to)]
  
  return(pairs)
  
}

get_buildings_nearest_vertex_id <- function(buildings, vertices) {
  knn <- get.knnx(
    vertices[, list(x, y)],
    buildings[, list(x, y)],
    k = 1
  )
  return(vertices$vertex_id[knn$nn.index])
}


get_cost_pair <- function(graph, vertex_id_from, vertex_id_to, approx_speed, variable = "time") {
  
  aggregate_aux <- variable != "time"
  
  get_distance_pair(
    graph,
    vertex_id_from,
    vertex_id_to,
    aggregate_aux = aggregate_aux
  )
  
}


# get_updated_travel_times <- function(graph, tz_id_from, tz_id_to, flow, flow_weight, approx_speed, time_step = 15.0) {
#   
# 
#   
#   f <- flows[sample(1:.N, 10000, replace = TRUE, prob = flow_weight)]
#   f <- f[!duplicated(f[, list(vertex_id_from, vertex_id_to)])]
#   vertex_id_from <- f$vertex_id_from
#   vertex_id_to <- f$vertex_id_to
#   flow <- f$flow
#   
#   # Get the sequence of nodes of the shortest paths for each vertex pair
#   paths <- get_path_pair(graph, vertex_id_from, vertex_id_to, algorithm = "NBA", constant = 1/(approx_speed/3.6), long = TRUE)
#   paths <- as.data.table(paths)
#   paths[, previous_node := shift(node, 1, type = "lag"), by = list(from, to)]
#   
#   # Add the flows
#   flows <- data.table(
#     from = vertex_id_from,
#     to = vertex_id_to,
#     flow = flow
#   )
#   
#   paths <- merge(paths, flows, by = c("from", "to"))
#   
#   # Add the time take to travel each edge
#   times <- as.data.table(graph$data)
#   setnames(times, c("from", "to", "time"))
#   
#   times <- merge(times, graph$dict, by.x = "from", by.y = "id")
#   times <- merge(times, graph$dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
#   times <- times[, list(previous_node = ref_from, node = ref_to, time)]
#   
#   paths <- merge(paths, times, by = c("previous_node", "node"), sort = FALSE)
#   paths[, cum_time := cumsum(time), by = list(from, to)]
#   
#   # Offset all times to make all trips arrive at the same time
#   paths[, cum_time := cum_time - max(cum_time), by = list(from, to)]
#   paths[, cum_time := cum_time - min(cum_time)]
#   
#   # Create a data.table of events recording when the flow enters or leaves each edge
#   paths[, `:=`(
#     edge = paste(previous_node, node, sep = "_"),
#     start_time = cum_time - time,
#     end_time = cum_time
#   )]
#   
#   events <- rbind(
#     paths[, .(edge, time = start_time, flow_change = flow)],
#     paths[, .(edge, time = end_time, flow_change = -flow)]
#   )
#   
#   setorder(events, edge, time)
#   
#   # Cumulate the events to get the actual flow at each time step
#   events[, cum_flow := cumsum(flow_change), by = edge]
#   events[, next_time := shift(time, type = "lead"), by = edge]
#   
#   # Align the flows on regular time intervals
#   delta_time <- time_step*60
#   min_time <- round(min(events$time)/60)*60
#   max_time <- round(max(events$time)/60)*60
#   
#   intervals <- data.table(
#     start_time = seq(min_time, max_time, delta_time),
#     end_time = seq(min_time + delta_time, max_time + delta_time, delta_time)
#   )
#   
#   intervals$start_time_cp <- intervals$start_time
#   intervals$end_time_cp <- intervals$end_time
#   
#   events[, next_time_cp := next_time]
#   events[, time_cp := time]
#   
#   events <- events[intervals, on = .(next_time_cp >= start_time_cp, time_cp < end_time_cp), list(edge, start_time, end_time, time, next_time, cum_flow), nomatch = 0L]
#   events[, interval_overlap := (pmin(end_time, next_time) - pmax(start_time, time))/delta_time]
#   events <- events[, list(cum_flow = sum(cum_flow*3600/delta_time*interval_overlap)), by = list(edge, start_time, end_time)]
#   
#   # Smooth the result to simulate the spread in departure / arrival times
#   offsets <- seq(-1, 0, 1)
#   weights <- rep(1/length(offsets), length(offsets))
#   cols <- paste0("offset", offsets)
#   
#   events[, c(cols) := shift(cum_flow, n = offsets, fill = 0.0), by = edge]
#   events[, cum_flow_smooth := rowSums(.SD[, cols, with = FALSE]*weights)]
#   
#   # Compute the maximum flow on each edge
#   edges_max_flow <- events[, list(max_flow = max(cum_flow_smooth)), by = edge]
#   
#   edges_max_flow[, node := unlist(lapply(strsplit(edge, "_"), "[[", 1))]
#   edges_max_flow[, previous_node := unlist(lapply(strsplit(edge, "_"), "[[", 2))]
#   
#   # Compute the updated speed based on the volume decay function of each edge and its traffic
#   edges_max_flow <- merge(edges_max_flow, graph$dict, by.x = "node", by.y = "ref")
#   edges_max_flow <- merge(edges_max_flow, graph$dict, by.x = "previous_node", by.y = "ref", suffixes = c("_from", "_to"))
#   edges_max_flow <- merge(as.data.table(graph$data), edges_max_flow[, c("id_from", "id_to", "max_flow")], by.x = c("from", "to"), by.y = c("id_from", "id_to"), all.x = TRUE, sort = FALSE)
#   
#   edges_max_flow[is.na(max_flow), max_flow := 0.0]
#   edges_max_flow[, k_speed := 1/(1 + graph$attrib$alpha*(max_flow/graph$attrib$cap)^graph$attrib$beta)]
#   edges_max_flow[, dist := dist*k_speed]
#   
#   return(edges_max_flow$dist)
#   
# }

get_updated_travel_times <- function(graph, vertex_id_from, vertex_id_to, flow, min_flow = 1.0) {
  
  total_flow <- sum(flow)
  index <- flow > min_flow
  
  vertex_id_from <- vertex_id_from[index]
  vertex_id_to <- vertex_id_to[index]
  flow <- flow[index]
  flow <- flow*total_flow/sum(flow)
  
  # Get the all or nothing flow assignment on the network
  contr_graph <- cpp_contract(graph, silent = TRUE)
  aon <- get_aon(contr_graph, vertex_id_from, vertex_id_to, demand = flow)
  aon <- as.data.table(aon)
  
  # Compute the updated speed based on the volume decay function of each edge and its traffic
  aon <- merge(aon, as.data.table(contr_graph$dict), by.x = "from", by.y = "ref")
  aon <- merge(aon, as.data.table(contr_graph$dict), by.x = "to", by.y = "ref", suffixes = c("_from", "_to"))
  aon <- merge(as.data.table(graph$data), aon[, c("id_from", "id_to", "flow")], by.x = c("from", "to"), by.y = c("id_from", "id_to"), all.x = TRUE, sort = FALSE)
  
  return(aon$flow)

}


get_travel_costs <- function(
    tz_id_from,
    tz_id_to,
    od_flows = NULL,
    edge_flows = NULL,
    mode,
    max_crowfly_speed = 110.0,
    max_crowfly_time = 1.0,
    approx = TRUE,
    approx_routing_speed = 40.0
) {
  
  # Load transport zones
  transport_zones <- st_read(tz_fp, quiet = TRUE)
  transport_zones <- as.data.table(st_drop_geometry(transport_zones))
  
  # Load cpprouting graph
  hash <- strsplit(basename(graph_fp), "-")[[1]][1]
  vertices <- read_parquet(file.path(dirname(dirname(graph_fp)), paste0(hash, "-vertices.parquet")))
  graph <- read_cppr_graph(dirname(graph_fp), hash)
  graph$coords <- vertices
  
  # Load representative buildings
  buildings <- as.data.table(read_parquet(buildings_sample_fp))
  buildings[, building_id := 1:.N]
  buildings[, vertex_id := get_buildings_nearest_vertex_id(buildings, vertices)]
  buildings <- merge(buildings[, list(building_id, transport_zone_id, n_clusters, weight, vertex_id)], vertices, by = "vertex_id")

  # Transform transport zones pairs into representative buildings pairs
  vertex_pairs <- tz_pairs_to_vertex_pairs(
    tz_id_from,
    tz_id_to,
    transport_zones,
    buildings,
    max_crowfly_speed,
    max_crowfly_time
  )
  
  # Modify speeds given the current flows on the network
  if (!is.null(edge_flows)) {
    
    info(logger, "Estimating congestion...")
    
    k_speed <- 1 + graph$attrib$alpha*(edge_flows/graph$attrib$cap)^graph$attrib$beta
    graph$data$dist <- graph$data$dist*k_speed
    
  }
  
  # Use crowfly distances and a constant speed for the approximate calculation,
  # or real distances and times on the routing graph
  info(logger, "Computing travel times and distances...")
  
  if (approx == TRUE) {
    
    vertex_pairs[, time := distance/1000/approx_routing_speed*3600]
    edge_flows <- edge_flows
    
  } else {
    
    contr_graph <- cpp_contract(graph, silent = TRUE)
    
    vertex_pairs[, time := get_cost_pair(contr_graph, vertex_id_from, vertex_id_to, approx_routing_speed, "time")]
    vertex_pairs[, distance := get_cost_pair(contr_graph, vertex_id_from, vertex_id_to, approx_routing_speed, "distance")]
    
    # Compute updated edge flows
    od_flows <- data.table(
      tz_id_from = tz_id_from,
      tz_id_to = tz_id_to,
      flow = od_flows
    )
    
    od_flows <- merge(vertex_pairs, od_flows, by = c("tz_id_from", "tz_id_to"))
    od_flows[, flow := flow*weight]
    
    edge_flows <- get_updated_edge_flows(contr_graph, od_flows$vertex_id_from, od_flows$vertex_id_to, od_flows$flow, min_flow = 1.0)
    
  }
  
  costs <- vertex_pairs[,
    list(
      distance = sum(distance*weight)/1000,
      time = sum(time*weight)/3600
    ),
    by = list(tz_id_from, tz_id_to)
  ]
  
  return(list(
    costs = costs,
    edge_flows = edge_flows
  ))
  
  
}
# 
# 
# all_tz_pairs <- CJ(
#   tz_id_from = transport_zones$transport_zone_id,
#   tz_id_to = transport_zones$transport_zone_id
# )
# # tz_id_from <- all_tz_pairs$tz_id_from[1:100]
# # tz_id_to <- all_tz_pairs$tz_id_to[200:299]
# 
# tz_id_from <- all_tz_pairs$tz_id_from
# tz_id_to <- all_tz_pairs$tz_id_to
# max_crowfly_speed <- 110.0
# max_crowfly_time <- 1.0
# 
# 
# costs_0 <- get_travel_costs(
#   tz_id_from = all_tz_pairs$tz_id_from,
#   tz_id_to = all_tz_pairs$tz_id_to,
#   mode = "car",
#   approx = TRUE
# )
# 
# sources <- data.table(
#   transport_zone_id = transport_zones$transport_zone_id,
#   source_volume = runif(nrow(transport_zones), 1000, 10000)
# )
# 
# sinks <- data.table(
#   transport_zone_id = transport_zones$transport_zone_id,
#   sink_volume = runif(nrow(transport_zones), 1000, 10000)
# )
# 
# flows_0 <- merge(costs_0, sources, by.x = "tz_id_from", by.y = "transport_zone_id")
# flows_0 <- merge(flows_0, sinks, by.x = "tz_id_to", by.y = "transport_zone_id")
# 
# flows_0 <- flows_0[order(time)]
# 
# flows_0[, p := source_volume*sink_volume/(source_volume + cumsum(source_volume))/(source_volume + cumsum(source_volume) + sink_volume), by = tz_id_from]
# flows_0[, p := p/sum(p), by = tz_id_from]
# flows_0[, flow := source_volume*p]
# 
# tz_id_from <- flows_0$tz_id_from
# tz_id_to <- flows_0$tz_id_to
# flow <- flows_0$flow
# flow_weight <- flows_0[, distance*flow]
# 
# 
# costs_1 <- get_travel_costs(
#   tz_id_from = flows_0$tz_id_from,
#   tz_id_to = flows_0$tz_id_to,
#   od_flows = flows_0$flow,
#   mode = "car",
#   approx = FALSE
# )
# 
# 
# flows_1 <- merge(costs_1, sources, by.x = "tz_id_from", by.y = "transport_zone_id")
# flows_1 <- merge(flows_1, sinks, by.x = "tz_id_to", by.y = "transport_zone_id")
# 
# flows_1 <- flows_1[order(time)]
# 
# flows_1[, p := source_volume*sink_volume/(source_volume + cumsum(source_volume))/(source_volume + cumsum(source_volume) + sink_volume), by = tz_id_from]
# flows_1[, p := p/sum(p), by = tz_id_from]
# flows_1[, flow := source_volume*p]
# 
# tz_id_from <- flows_1$tz_id_from
# tz_id_to <- flows_1$tz_id_to
# flow <- flows_1$flow
# flow_weight <- flows_1[, distance*flow]
# 
# 
# 
# comp[order(-flow_0)]
# 
# 
# 
# 
# costs_2 <- get_travel_costs(
#   tz_id_from = flows_1$tz_id_from,
#   tz_id_to = flows_1$tz_id_to,
#   flow = flows_1$flow,
#   mode = "car",
#   approx = FALSE
# )
# 
# 
# flows_2 <- merge(costs_2, sources, by.x = "tz_id_from", by.y = "transport_zone_id")
# flows_2 <- merge(flows_2, sinks, by.x = "tz_id_to", by.y = "transport_zone_id")
# 
# flows_2 <- flows_2[order(time)]
# 
# flows_2[, p := source_volume*sink_volume/(source_volume + cumsum(source_volume))/(source_volume + cumsum(source_volume) + sink_volume), by = tz_id_from]
# flows_2[, p := p/sum(p), by = tz_id_from]
# flows_2[, flow := source_volume*p]
# 
# 
# 
# 
# 
# 
# comp <- merge(flows_0[, list(tz_id_from, tz_id_to, flow_0 = flow)], flows_1[, list(tz_id_from, tz_id_to, flow_1 = flow)], by = c("tz_id_from", "tz_id_to"))
# comp <- merge(comp, flows_2[, list(tz_id_from, tz_id_to, flow_2 = flow)], by = c("tz_id_from", "tz_id_to"))
# 
# comp[order(-flow_2)]
