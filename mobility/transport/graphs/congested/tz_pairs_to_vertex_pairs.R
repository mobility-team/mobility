tz_pairs_to_vertex_pairs <- function(
    tz_id_from,
    tz_id_to,
    buildings,
    vehicle_volume,
    congestion_flows_scaling_factor,
    target_max_vehicles_per_od_endpoint
) {
  
  # Pick the number of representative buildings to use for each transport zone
  # pair based on the target maximum traffic allowed on one OD endpoint.
  pairs <- data.table(
    tz_id_from = tz_id_from,
    tz_id_to = tz_id_to,
    vehicle_volume = vehicle_volume
  )
  
  flows_from <- pairs[, list(veh_vol_from = sum(vehicle_volume*congestion_flows_scaling_factor)), by = tz_id_from]
  flows_to <- pairs[, list(veh_vol_to = sum(vehicle_volume*congestion_flows_scaling_factor)), by = tz_id_to]
  
  veh_col_from_to <- merge(flows_from, flows_to, by.x = "tz_id_from", by.y = "tz_id_to")
  veh_col_from_to[, veh_vol := pmax(veh_vol_from, veh_vol_to)]
  setnames(veh_col_from_to, "tz_id_from", "tz_id")
  
  # Split enough so the busiest OD endpoint stays below the target when the
  # available clustering levels make it possible.
  n_clusters_max <- max(buildings$n_clusters)
  
  veh_col_from_to[, n_clusters_target := ceiling(veh_vol/target_max_vehicles_per_od_endpoint)]
  veh_col_from_to[, n_clusters := pmin(n_clusters_max, n_clusters_target)]
  
  # Merge the tz pairs with the representative buildings locations and weights
  # Compute the crowfly distance between these buildings, agregating weights
  # when there are duplicates (which can happen when buildings are mapped 
  # to the same network vertex)
  buildings_from <- merge(
    buildings[, list(transport_zone_id, n_clusters, vertex_id, x, y, weight)],
    veh_col_from_to[, list(tz_id, n_clusters)],
    by.x = c("transport_zone_id", "n_clusters"),
    by.y = c("tz_id", "n_clusters"),
  )
  
  buildings_to <- merge(
    buildings[, list(transport_zone_id, n_clusters, vertex_id, x, y, weight)],
    veh_col_from_to[, list(tz_id, n_clusters)],
    by.x = c("transport_zone_id", "n_clusters"),
    by.y = c("tz_id", "n_clusters"),
  )
  
  pairs <- merge(
    pairs,
    buildings_from[, list(transport_zone_id, vertex_id, x, y, weight)],
    by.x = "tz_id_from",
    by.y = "transport_zone_id",
    allow.cartesian = TRUE
  )
  
  pairs <- merge(
    pairs,
    buildings_to[, list(transport_zone_id, vertex_id, x, y, weight)],
    by.x = "tz_id_to",
    by.y = "transport_zone_id",
    allow.cartesian = TRUE,
    suffixes = c("_from", "_to")
  )
  
  pairs <- pairs[vertex_id_from != vertex_id_to]
  pairs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
  
  pairs[, weight := weight_from*weight_to]
  pairs[, weight := weight/sum(weight), list(tz_id_from, tz_id_to)]
  
  pairs <- pairs[, list(weight = sum(weight), distance = mean(distance)), by = list(tz_id_from, tz_id_to, vertex_id_from, vertex_id_to)]
  
  return(pairs)
  
}
