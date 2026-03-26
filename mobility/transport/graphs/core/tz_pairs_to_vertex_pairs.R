

tz_pairs_to_vertex_pairs <- function(
    tz_id_from,
    tz_id_to,
    transport_zones,
    buildings
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
  pairs[, n_clusters := round(1 + 4*exp(-distance/1000/2))]
  
  pairs <- pairs[, list(tz_id_from, tz_id_to, n_clusters)]
  
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