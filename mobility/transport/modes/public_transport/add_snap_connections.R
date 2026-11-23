# Add the straight-line connections between buildings and their graph vertices.
# Distances are in metres and graph times in seconds; transfer speeds are km/h.
add_snap_connections <- function(costs, first_transfer, last_transfer, parameters, modal_transfer_time) {
  first_time <- costs$vertex_id_from_dist * 3.6 / first_transfer$average_speed
  last_time <- costs$vertex_id_to_dist * 3.6 / last_transfer$average_speed

  costs[, `:=`(
    start_distance = start_distance + vertex_id_from_dist,
    last_distance = last_distance + vertex_id_to_dist,
    start_real_time = start_real_time + first_time,
    last_real_time = last_real_time + last_time,
    start_perceived_time = start_perceived_time + first_time,
    last_perceived_time = last_perceived_time + last_time,
    perceived_time = perceived_time + first_time + last_time
  )]

  # The graph was routed once per vertex pair. If the selected path exceeds
  # the complete leg limit, a different path is not searched here.
  costs[start_real_time < first_transfer$max_travel_time * 3600 &
        last_real_time < last_transfer$max_travel_time * 3600 &
        perceived_time - modal_transfer_time < parameters$max_traveltime * 3600 &
        perceived_time < parameters$max_perceived_time * 3600]
}
