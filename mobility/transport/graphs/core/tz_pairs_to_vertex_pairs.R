

tz_pairs_to_vertex_pairs <- function(
    tz_id_from,
    tz_id_to,
    transport_zones,
    buildings,
    vehicle_volume = NULL
) {
  
  # Compute the crowfly distance between transport zones centers and the number
  # of representative buildings to use for each transport zone pair.
  pairs <- data.table(
    tz_id_from = tz_id_from,
    tz_id_to = tz_id_to
  ) 
  if (!is.null(vehicle_volume)) {
    pairs[, vehicle_volume := vehicle_volume]
  }
  
  pairs <- merge(pairs, transport_zones[, list(transport_zone_id, x, y)], by.x = "tz_id_from", by.y = "transport_zone_id")
  pairs <- merge(pairs, transport_zones[, list(transport_zone_id, x, y)], by.x = "tz_id_to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))
  
  pairs[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
  pairs[, n_clusters_distance := round(1 + 4*exp(-distance/1000/2))]
  
  if (!is.null(vehicle_volume)) {
    positive_vehicle_volume <- pairs[vehicle_volume > 0, vehicle_volume]
    if (length(positive_vehicle_volume) > 0) {
      volume_reference <- as.numeric(stats::quantile(positive_vehicle_volume, probs = 0.75, na.rm = TRUE))
      volume_reference <- max(volume_reference, 1)
      pairs[, n_clusters_volume := 1 + floor(log1p(vehicle_volume/volume_reference)/log(2))]
      pairs[, n_clusters_volume := pmin(5, pmax(1, n_clusters_volume))]
      pairs[, n_clusters := pmax(n_clusters_distance, n_clusters_volume)]
    } else {
      pairs[, n_clusters := n_clusters_distance]
    }
  } else {
    pairs[, n_clusters := n_clusters_distance]
  }
  
  zones_max_n_clusters <- buildings[
    ,
    list(max_n_clusters = max(n_clusters)),
    by = "transport_zone_id"
  ]
  pairs <- merge(pairs, zones_max_n_clusters, by.x = "tz_id_from", by.y = "transport_zone_id")
  setnames(pairs, "max_n_clusters", "max_n_clusters_from")
  pairs <- merge(pairs, zones_max_n_clusters, by.x = "tz_id_to", by.y = "transport_zone_id")
  setnames(pairs, "max_n_clusters", "max_n_clusters_to")
  pairs[, n_clusters := pmin(n_clusters, max_n_clusters_from, max_n_clusters_to)]
  
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
