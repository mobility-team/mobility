#' Check whether each edge is usable by cars.
#'
#' Generic access restrictions are combined with more specific vehicle tags.
#' A specific motor-vehicle allowance can make an access=no road usable, while
#' access=private remains blocked.
#'
#' @param edges Data frame or data table containing OSM access columns.
#' @return Logical vector with one value per edge.
car_access_is_allowed <- function(edges) {
  # A few roads are tagged `access=no` but still allow motor vehicles through a
  # more specific tag such as `motor_vehicle=yes`. Keep those roads for the car
  # graph when the specific vehicle tag makes the generic access tag less strict.
  allowed_motor_values <- c("yes", "designated", "permissive", "destination", "official")

  if (!("access" %in% names(edges))) {
    return(rep(TRUE, nrow(edges)))
  }

  blocked_by_private <- ifelse(is.na(edges$access), FALSE, edges$access == "private")
  blocked_by_no <- ifelse(is.na(edges$access), FALSE, edges$access == "no")

  allowed_motor_vehicle <- rep(FALSE, nrow(edges))
  if ("motor_vehicle" %in% names(edges)) {
    allowed_motor_vehicle <- allowed_motor_vehicle | edges$motor_vehicle %in% allowed_motor_values
  }
  if ("motorcar" %in% names(edges)) {
    allowed_motor_vehicle <- allowed_motor_vehicle | edges$motorcar %in% allowed_motor_values
  }
  if ("vehicle" %in% names(edges)) {
    allowed_motor_vehicle <- allowed_motor_vehicle | edges$vehicle %in% allowed_motor_values
  }

  blocked_by_no <- blocked_by_no & !allowed_motor_vehicle

  !(blocked_by_private | blocked_by_no)
}

#' Filter a dodgr street graph to roads usable by cars.
#'
#' The function removes HGV-only and ridesharing-only roads, computes the
#' reusable car_access_allowed flag, and drops edges that cars cannot use.
#'
#' @param graph dodgr street graph.
#' @return Filtered dodgr street graph with a car_access_allowed column.
filter_car_streetnet_graph <- function(graph) {
  # Drop roads that are not usable for cars. The access decision is saved before
  # we keep only the graph columns needed by the next processing steps.
  if ("hgv" %in% colnames(graph)) {
    graph <- graph[graph$hgv != "designated" | is.na(graph$hgv), ]
  }
  if ("hov" %in% colnames(graph)) {
    graph <- graph[graph$hov != "designated" | is.na(graph$hov), ]
  }

  graph$car_access_allowed <- car_access_is_allowed(graph)
  graph[graph$car_access_allowed, ]
}

#' Parse an OSM lane count.
#'
#' @param x Character vector of OSM lane count values.
#' @return Numeric vector with the first lane count found in each value.
parse_lane_count <- function(x) {
  # Keep the first numeric value when a tag contains separators such as ";"
  # or "|" and return NA when no number is present.
  x <- as.character(x)
  x[is.na(x) | x == ""] <- NA_character_
  has_number <- grepl("^[[:space:]]*[0-9]+(?:\\.[0-9]+)?", x, perl = TRUE)
  x[has_number] <- sub(
    "^[[:space:]]*([0-9]+(?:\\.[0-9]+)?).*$",
    "\\1",
    x[has_number],
    perl = TRUE
  )
  x[!has_number] <- NA_character_

  as.numeric(x)
}

#' Count designated public-service-vehicle lanes in lane strings.
#'
#' @param x Character vector of psv:lanes:* values.
#' @return Numeric vector with the number of designated PSV lanes, or NA when unknown.
count_designated_psv_lanes <- function(x) {
  # Some OSM exports provide psv:lanes:* strings such as
  # "yes|yes|designated" instead of lanes:psv:* counts.
  x <- as.character(x)
  x[is.na(x) | x == ""] <- NA_character_

  counts <- rep(NA_real_, length(x))
  valid <- !is.na(x)
  if (any(valid)) {
    matches <- gregexpr("(^|\\|)[[:space:]]*designated[[:space:]]*(\\||$)", x[valid], perl = TRUE)
    counts[valid] <- lengths(matches)
  }

  counts
}

#' Build car edges with capacity attributes for the cppRouting graph.
#'
#' The function keeps dodgr edge identifiers, computes directional car lanes,
#' subtracts reserved public-service-vehicle lanes, and attaches congestion
#' parameters from the highway capacity table.
#'
#' @param graph Filtered dodgr car street graph.
#' @param osm_data OSM data returned by osmdata_sc().
#' @param osm_capacity_parameters Table of highway classes and capacity parameters.
#' @return data.table of directed car edges ready for cppRouting graph creation.
build_car_edges <- function(graph, osm_data, osm_capacity_parameters) {
  edge_columns <- c(
    "edge_",
    ".vx0", ".vx1",
    "time_weighted", "d",
    "highway",
    "access",
    "car_access_allowed",
    "lanes",
    "lanes:forward", "lanes:backward",
    "lanes:psv:forward", "lanes:psv:backward",
    "psv:lanes:forward", "psv:lanes:backward"
  )

  graph_dt <- as.data.table(graph)

  missing_edge_columns <- setdiff(edge_columns, colnames(graph_dt))
  for (col in missing_edge_columns) {
    graph_dt[, (col) := NA_character_]
  }

  edges <- graph_dt[, ..edge_columns]

  # Original dodgr edges keep the edge_ identifiers from osmdata_sc.
  # Reversed duplicates created by dodgr receive new edge_ hashes, so this flag
  # tells us whether the row follows the original OSM way direction.
  edges[, is_original_direction := edge_ %in% osm_data$object_link_edge$edge_]

  # Detect whether the edge has an opposite-direction companion. When only a
  # total `lanes` tag is available on a bidirectional road, we split that total
  # between the two directed edges instead of giving the full total to both.
  edges[, pair_v0 := pmin(.vx0, .vx1)]
  edges[, pair_v1 := pmax(.vx0, .vx1)]
  edges[, pair_n_edges := .N, by = .(pair_v0, pair_v1)]

  edges[, lanes_total := parse_lane_count(lanes)]
  edges[, lanes_forward := parse_lane_count(`lanes:forward`)]
  edges[, lanes_backward := parse_lane_count(`lanes:backward`)]

  edges[, psv_lanes_forward := parse_lane_count(`lanes:psv:forward`)]
  edges[, psv_lanes_backward := parse_lane_count(`lanes:psv:backward`)]

  missing_forward_psv <- is.na(edges$psv_lanes_forward)
  edges[missing_forward_psv, psv_lanes_forward := count_designated_psv_lanes(`psv:lanes:forward`[missing_forward_psv])]

  missing_backward_psv <- is.na(edges$psv_lanes_backward)
  edges[missing_backward_psv, psv_lanes_backward := count_designated_psv_lanes(`psv:lanes:backward`[missing_backward_psv])]

  edges[is.na(psv_lanes_forward), psv_lanes_forward := 0]
  edges[is.na(psv_lanes_backward), psv_lanes_backward := 0]

  # Infer one directional lane count from the opposite direction plus the total
  # lane count when possible.
  edges[
    is.na(lanes_forward) & !is.na(lanes_total) & !is.na(lanes_backward),
    lanes_forward := pmax(lanes_total - lanes_backward, 0)
  ]
  edges[
    is.na(lanes_backward) & !is.na(lanes_total) & !is.na(lanes_forward),
    lanes_backward := pmax(lanes_total - lanes_forward, 0)
  ]

  # When only one directional lane tag is available and the total lane count is
  # missing, reuse the known direction as a pragmatic fallback.
  edges[
    is.na(lanes_forward) & is.na(lanes_total) & !is.na(lanes_backward),
    lanes_forward := lanes_backward
  ]
  edges[
    is.na(lanes_backward) & is.na(lanes_total) & !is.na(lanes_forward),
    lanes_backward := lanes_forward
  ]

  # When only the total lane count is known on a bidirectional road, split it
  # between the two directed edges. On one-way roads, keep the full total on
  # the original direction.
  edges[
    is.na(lanes_forward) & is.na(lanes_backward) & !is.na(lanes_total) & pair_n_edges >= 2,
    `:=`(
      lanes_forward = ceiling(lanes_total / 2),
      lanes_backward = floor(lanes_total / 2)
    )
  ]
  edges[
    is.na(lanes_forward) & is.na(lanes_backward) & !is.na(lanes_total) & pair_n_edges == 1,
    `:=`(
      lanes_forward = lanes_total,
      lanes_backward = 0
    )
  ]

  edges[
    is.na(lanes_forward) & is.na(lanes_backward),
    `:=`(
      lanes_forward = 1,
      lanes_backward = 1
    )
  ]

  edges[
    is_original_direction == TRUE,
    car_lanes := pmax(lanes_forward - psv_lanes_forward, 0)
  ]
  edges[
    is_original_direction == FALSE,
    car_lanes := pmax(lanes_backward - psv_lanes_backward, 0)
  ]

  # Keep at least one usable car lane as a conservative fallback when OSM tags
  # are incomplete or inconsistent.
  edges[is.na(car_lanes) | car_lanes <= 0, car_lanes := 1]

  edges <- merge(edges, osm_capacity_parameters, by = "highway", sort = FALSE)
  edges[, capacity := capacity * car_lanes]

  edges[, c(
    "pair_v0", "pair_v1", "pair_n_edges",
    "lanes_total", "lanes_forward", "lanes_backward",
    "psv_lanes_forward", "psv_lanes_backward", "car_lanes"
  ) := NULL]

  edges
}
