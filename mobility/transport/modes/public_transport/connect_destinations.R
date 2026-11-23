# Add the final straight-line connection before applying the arrival deadline.
connect_destinations <- function(last_leg, last_verts, buildings, transfer, target_time) {
  vertices <- last_verts[vertex_id %in% unique(last_leg$to)]
  snap <- FNN::get.knnx(vertices[, list(x, y)], buildings[, list(x, y)], k = 1)
  destinations <- buildings[, list(building_id, x, y)]
  destinations[, road_vertex := vertices$vertex_id[snap$nn.index[, 1]]]
  destinations[, snap_distance := snap$nn.dist[, 1]]
  destinations <- destinations[snap_distance / 1000 / transfer$average_speed < transfer$max_travel_time]
  destinations[, vertex_id := paste0("l2-b", building_id)]

  legs <- merge(last_leg, destinations[, list(road_vertex, building_id, snap_distance)],
                by.x = "to", by.y = "road_vertex", allow.cartesian = TRUE)
  legs[, `:=`(time = time + snap_distance * 3.6 / transfer$average_speed,
              distance = distance + snap_distance)]
  legs <- legs[time < transfer$max_travel_time * 3600]
  legs[, from := paste0("l1-", from)]
  legs[, to := paste0("l2-b", building_id)]
  # All stop events already use a one-minute resolution.
  legs[, max_time_at_station := round((target_time * 3600 - time) / 60) * 60]
  legs[, from_with_time := paste0(from, "-", max_time_at_station)]
  list(legs = legs, vertices = destinations[, list(vertex_id, x, y)])
}
