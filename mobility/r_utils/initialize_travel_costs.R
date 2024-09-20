

initialize_travel_costs <- function(transport_zones, buildings_sample, orig_verts, dest_verts) {
  
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
  
  
  orig_knn <- get.knnx(
    orig_verts[, list(x, y)],
    buildings_sample[, list(x, y)],
    k = 1
  )
  
  buildings_sample[, vertex_id_from := orig_verts[orig_knn$nn.index, vertex_id]]
  
  dest_knn <- get.knnx(
    dest_verts[, list(x, y)],
    buildings_sample[, list(x, y)],
    k = 1
  )
  
  buildings_sample[, vertex_id_to := dest_verts[dest_knn$nn.index, vertex_id]]
  
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
  
  return(travel_costs)
  
  
}