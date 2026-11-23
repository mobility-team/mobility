# Give the same ID to visits on the same route with the same remaining stops.
# Work backwards so each key consists only of the current stop and the ID of
# the remaining journey, rather than repeatedly copying long stop sequences.
stop_pattern_suffixes <- function(visits) {
  stops <- unique(visits[, list(pattern_id, route_id, position, stop_id)])
  setorder(stops, -position, pattern_id)
  next_suffix <- integer(max(stops$pattern_id))
  suffix_ids <- integer(nrow(stops))
  known <- new.env(hash = TRUE, parent = emptyenv())
  n_suffixes <- 0L
  for (i in seq_len(nrow(stops))) {
    route <- stops$route_id[i]
    stop <- stops$stop_id[i]
    pattern <- stops$pattern_id[i]
    key <- paste0(nchar(route), ":", route, nchar(stop), ":", stop, ":", next_suffix[pattern])
    suffix <- known[[key]]
    if (is.null(suffix)) {
      n_suffixes <- n_suffixes + 1L
      suffix <- n_suffixes
      known[[key]] <- suffix
    }
    suffix_ids[i] <- suffix
    next_suffix[pattern] <- suffix
  }
  stops[, suffix_id := suffix_ids]
  stops[, list(pattern_id, position, suffix_id)]
}
