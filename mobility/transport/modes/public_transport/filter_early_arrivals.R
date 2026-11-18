# The allowance is expressed in hours; graph event times are in seconds.
filter_early_arrivals <- function(events, max_wait_hours) {
  events[, min_out_time := min(time[edge_type == "out"]), by = v_id]
  events[time > min_out_time - max_wait_hours * 3600]
}
