

initialize_travel_costs <- function(
    transport_zones,
    buildings_sample,
    orig_verts,
    mid_verts,
    dest_verts,
    first_modal_shift,
    last_modal_shift
  ) {
  
  buildings_sample <- copy(buildings_sample)
  transport_zones_dt <- as.data.table(st_drop_geometry(transport_zones))
  
  # Compute crowfly distances between transport zones to compute the number of 
  # points within the origin and destination zones that should be used
  # (between 5 for )
  travel_costs <- CJ(
    from = transport_zones_dt$transport_zone_id,
    to = transport_zones_dt$transport_zone_id
  )
  
  travel_costs <- merge(travel_costs, transport_zones_dt[, list(transport_zone_id, x, y)], by.x = "from", by.y = "transport_zone_id")
  travel_costs <- merge(travel_costs, transport_zones_dt[, list(transport_zone_id, x, y)], by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))
  
  travel_costs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
  travel_costs[, n_clusters := round(1 + 4*exp(-distance/1000/2))]
  
  # Associate each transport zones with its representative buildings
  travel_costs <- merge(
    travel_costs,
    buildings_sample[, list(transport_zone_id, n_clusters, building_id, weight)],
    by.x = c("from", "n_clusters"),
    by.y = c("transport_zone_id", "n_clusters"),
    all.x = TRUE,
    allow.cartesian = TRUE
  )
  
  travel_costs <- merge(
    travel_costs,
    buildings_sample[, list(transport_zone_id, n_clusters, building_id, weight)],
    by.x = c("to", "n_clusters"),
    by.y = c("transport_zone_id", "n_clusters"),
    all.x = TRUE,
    suffixes = c("_from", "_to"),
    allow.cartesian = TRUE
  )
  
  travel_costs <- travel_costs[building_id_from != building_id_to]
  
  # Associate each building with its nearest vertex in the start graph
  orig_knn <- get.knnx(
    orig_verts[, list(x, y)],
    buildings_sample[, list(x, y)],
    k = 1
  )
  
  buildings_sample[, vertex_id_from := orig_verts[orig_knn$nn.index, vertex_id]]
  
  # Associate each building with its nearest vertex in the last graph
  dest_knn <- get.knnx(
    dest_verts[, list(x, y)],
    buildings_sample[, list(x, y)],
    k = 1
  )
  
  buildings_sample[, vertex_id_to := dest_verts[dest_knn$nn.index, vertex_id]]
  
  # Find the distance of each building to its nearest connection vertex
  if (!is.null(mid_verts)) {
    
    con_knn <- get.knnx(
      unique(mid_verts[vertex_type == "access", list(x, y)]),
      buildings_sample[, list(x, y)],
      k = 1
    )
    
    buildings_sample[, dist_con := con_knn$nn.dist]
    
  }

  
  # Associate graph vertices for all origin destinations
  # by using the association transport_zones -> buildings -> graph vertices
  # and buildings -> connection distance
  travel_costs <- merge(
    travel_costs,
    buildings_sample[, list(building_id, vertex_id_from)],
    by.x = "building_id_from",
    by.y = "building_id"
  )
  
  travel_costs <- merge(
    travel_costs,
    buildings_sample[, list(building_id, vertex_id_to)],
    by.x = "building_id_to",
    by.y = "building_id",
    suffixes = c("_from", "_to")
  )

  # Filter out origin - destinations pairs for which there is no public transport 
  # stop within travel times to access the first stop and to get to the final destination
  if (!is.null(mid_verts)) {
    
    travel_costs <- merge(
      travel_costs,
      buildings_sample[, list(building_id, dist_con)],
      by.x = "building_id_from",
      by.y = "building_id"
    )
    
    travel_costs <- merge(
      travel_costs,
      buildings_sample[, list(building_id, dist_con)],
      by.x = "building_id_to",
      by.y = "building_id",
      suffixes = c("_from", "_to")
    )
  
    travel_costs <- travel_costs[dist_con_from/1000/first_modal_shift$average_speed < first_modal_shift$max_travel_time]
    travel_costs <- travel_costs[dist_con_to/1000/last_modal_shift$average_speed < last_modal_shift$max_travel_time]
  
  }
  
  # travel_costs[, vertex_id_from := paste0("s", vertex_id_from)]
  # travel_costs[, vertex_id_to := paste0("l", vertex_id_to)]
  
  # travel_costs <- travel_costs[vertex_id_from %in% graph$dict$ref]
  # travel_costs <- travel_costs[vertex_id_to %in% graph$dict$ref]
  
  return(travel_costs)
  
}