compute_od_edge_projections <- function(
    cppr_graph,
    vertices,
    od_points,
    mode,
    projection_bin_size = 10.0,
    endpoint_tolerance = 1.0
  ) {

  # Copy the input graph tables and materialize edge-aligned attributes next to
  # the edge rows.
  vertices <- copy(as.data.table(vertices))
  vertices[, vertex_id := as.character(vertex_id)]

  od_points <- copy(as.data.table(od_points))
  od_points[, building_id := 1:.N]

  dict <- copy(as.data.table(cppr_graph[["dict"]]))
  dict[, ref := as.character(ref)]

  edges <- copy(as.data.table(cppr_graph[["data"]]))

  for (attrib_name in names(cppr_graph[["attrib"]])) {
    attrib <- cppr_graph[["attrib"]][[attrib_name]]
    if (!is.null(attrib) && length(attrib) == nrow(edges)) {
      edges[, (attrib_name) := attrib]
    }
  }

  # Group opposite directed edges under one undirected host edge.
  edges[, from0 := pmin(from, to)]
  edges[, to0 := pmax(from, to)]

  edge_groups <- unique(edges[, list(from0, to0)])
  edge_groups[, group_id := .I]
  edges <- merge(edges, edge_groups, by = c("from0", "to0"), all.x = TRUE, sort = FALSE)

  # Attach endpoint vertex ids and coordinates to each host edge group.
  edge_groups <- merge(
    edge_groups,
    dict[, list(id, ref)],
    by.x = "from0",
    by.y = "id",
    all.x = TRUE,
    sort = FALSE
  )
  setnames(edge_groups, "ref", "ref_from")

  edge_groups <- merge(
    edge_groups,
    dict[, list(id, ref)],
    by.x = "to0",
    by.y = "id",
    all.x = TRUE,
    sort = FALSE
  )
  setnames(edge_groups, "ref", "ref_to")

  edge_groups <- merge(
    edge_groups,
    vertices[, list(vertex_id, x, y)],
    by.x = "ref_from",
    by.y = "vertex_id",
    all.x = TRUE,
    sort = FALSE
  )
  setnames(edge_groups, c("x", "y"), c("x_from", "y_from"))

  edge_groups <- merge(
    edge_groups,
    vertices[, list(vertex_id, x, y)],
    by.x = "ref_to",
    by.y = "vertex_id",
    all.x = TRUE,
    sort = FALSE
  )
  setnames(edge_groups, c("x", "y"), c("x_to", "y_to"))

  # Mark which host edges are eligible projection targets for the current mode.
  if (mode == "car" && "direct_access" %in% names(edges)) {
    access_groups <- edges[, list(candidate = any(direct_access > 0)), by = group_id]
    edge_groups <- merge(edge_groups, access_groups, by = "group_id", all.x = TRUE, sort = FALSE)
    edge_groups[is.na(candidate), candidate := FALSE]
  } else {
    edge_groups[, candidate := TRUE]
  }

  edge_groups[, edge_length := sqrt((x_to - x_from)^2 + (y_to - y_from)^2)]
  candidate_edges <- edge_groups[candidate == TRUE & is.finite(edge_length) & edge_length > 0]

  if (nrow(candidate_edges) == 0) {
    stop("No eligible edges available to project OD points onto.")
  }

  # Build LINESTRING geometries for eligible host edges and POINT geometries for
  # OD points, then find the nearest edge for each point.
  edge_line_points <- rbindlist(
    list(
      candidate_edges[, list(group_id, x = x_from, y = y_from)],
      candidate_edges[, list(group_id, x = x_to, y = y_to)]
    )
  )

  edges_sf <- sfheaders::sf_linestring(
    edge_line_points[order(group_id)],
    x = "x",
    y = "y",
    linestring_id = "group_id",
    keep = TRUE
  )
  st_crs(edges_sf) <- 3035

  od_points_sf <- sfheaders::sf_point(od_points, x = "x", y = "y", keep = TRUE)
  st_crs(od_points_sf) <- 3035

  nearest_idx <- st_nearest_feature(od_points_sf, edges_sf)
  matched_edges <- candidate_edges[nearest_idx, list(
    group_id,
    from0,
    to0,
    ref_from,
    ref_to,
    x_from,
    y_from,
    x_to,
    y_to,
    edge_length
  )]

  # Convert the nearest-edge results back to a table and compute each
  # projection's position along its host segment.
  attachments <- cbind(
    od_points[, list(building_id, transport_zone_id, n_clusters, weight, x, y)],
    matched_edges
  )

  attachments[, dx := x_to - x_from]
  attachments[, dy := y_to - y_from]
  attachments[, t := ((x - x_from) * dx + (y - y_from) * dy) / (edge_length^2)]
  attachments[, t := pmin(1.0, pmax(0.0, t))]
  attachments[, proj_x := x_from + t * dx]
  attachments[, proj_y := y_from + t * dy]
  attachments[, proj_pos_m := t * edge_length]
  attachments[, dist_to_edge := sqrt((x - proj_x)^2 + (y - proj_y)^2)]

  # Bin projected positions along each host edge so nearby OD points share one
  # inserted node and the split step does not create zero-length segments.
  attachments[, proj_pos_bin := round(proj_pos_m / projection_bin_size) * projection_bin_size]
  attachments[, proj_pos_bin := pmin(edge_length, pmax(0.0, proj_pos_bin))]
  attachments[, t_bin := fifelse(edge_length > 0, proj_pos_bin / edge_length, 0.0)]
  attachments[, proj_x_bin := x_from + t_bin * dx]
  attachments[, proj_y_bin := y_from + t_bin * dy]

  unique_projections <- unique(attachments[, list(
    group_id,
    from0,
    to0,
    ref_from,
    ref_to,
    x_from,
    y_from,
    x_to,
    y_to,
    edge_length,
    proj_pos_m = proj_pos_bin,
    t = t_bin,
    proj_x = proj_x_bin,
    proj_y = proj_y_bin
  )], by = c("group_id", "proj_pos_m"))

  # Reuse existing endpoint vertices when the projection falls close to an edge
  # endpoint; otherwise allocate a new projected node id.
  unique_projections[, vertex_id := NA_character_]
  unique_projections[, new_id := NA_integer_]

  unique_projections[proj_pos_m <= endpoint_tolerance, `:=`(
    vertex_id = ref_from,
    proj_x = x_from,
    proj_y = y_from,
    t = 0.0
  )]

  unique_projections[(edge_length - proj_pos_m) <= endpoint_tolerance, `:=`(
    vertex_id = ref_to,
    proj_x = x_to,
    proj_y = y_to,
    t = 1.0
  )]

  interior_projections <- unique_projections[is.na(vertex_id)]

  if (nrow(interior_projections) > 0) {
    interior_projections[, new_id := cppr_graph[["nbnode"]] + 0:(.N - 1L)]
    interior_projections[, vertex_id := paste0("proj_", seq_len(.N))]
    unique_projections[interior_projections, `:=`(
      vertex_id = i.vertex_id,
      new_id = i.new_id
    ), on = .(group_id, t)]
  }

  unique_projections[
    dict[, list(vertex_id = ref, existing_id = id)],
    new_id := fcoalesce(new_id, i.existing_id),
    on = "vertex_id"
  ]

  attachments[
    unique_projections[, list(group_id, proj_pos_m, vertex_id, projected_x = proj_x, projected_y = proj_y)],
    `:=`(
      vertex_id = i.vertex_id,
      projected_x = i.projected_x,
      projected_y = i.projected_y
    ),
    on = .(group_id, proj_pos_bin = proj_pos_m)
  ]

  list(
    edges = edges,
    edge_groups = edge_groups,
    attachments = attachments,
    interior_projections = interior_projections
  )

}


split_cppr_graph_at_projections <- function(cppr_graph, vertices, projection_data) {

  # Start from the attributed edge table prepared during the projection stage.
  edges <- copy(projection_data$edges)
  edge_groups <- copy(projection_data$edge_groups)
  interior_projections <- copy(projection_data$interior_projections)

  dict <- copy(as.data.table(cppr_graph[["dict"]]))
  dict[, ref := as.character(ref)]

  vertices <- copy(as.data.table(vertices))
  vertices[, vertex_id := as.character(vertex_id)]
  vertices <- unique(vertices[, list(vertex_id, x, y)], by = "vertex_id")

  if (nrow(interior_projections) == 0) {
    vertices <- merge(
      dict[, list(vertex_id = ref)],
      vertices[, list(vertex_id, x, y)],
      by = "vertex_id",
      all.x = TRUE,
      sort = FALSE
    )

    if (anyNA(vertices$x) || anyNA(vertices$y)) {
      stop("Some graph dictionary refs are missing from the vertex table after OD insertion.")
    }

    return(list(
      graph = cppr_graph,
      vertices = vertices
    ))
  }

  # Build split boundaries on each affected host edge and derive the
  # resulting subsegments.
  interior_lookup <- interior_projections[, list(group_id, new_id, t, vertex_id, proj_x, proj_y)]
  edge_attr_names <- setdiff(names(edges), c("from", "to", "from0", "to0", "group_id"))
  additive_attr_names <- intersect(c("dist", "aux"), edge_attr_names)

  split_group_meta <- unique(edge_groups[group_id %in% interior_lookup$group_id, list(group_id, from0, to0)])

  segment_boundaries <- rbindlist(
    list(
      split_group_meta[, list(group_id, t = 0.0, node_id = from0)],
      interior_lookup[, list(group_id, t, node_id = new_id)],
      split_group_meta[, list(group_id, t = 1.0, node_id = to0)]
    ),
    use.names = TRUE
  )

  setorder(segment_boundaries, group_id, t, node_id)
  segment_boundaries[, boundary_rank := seq_len(.N), by = group_id]

  split_segments <- segment_boundaries[, list(
    seg_from = node_id[-.N],
    seg_to = node_id[-1],
    seg_frac = diff(t)
  ), by = group_id]
  split_segments <- split_segments[seg_frac > 0]

  # Expand each directed graph edge across the subsegments of its host edge
  # and orient the generated rows to match the original direction.
  split_edges <- merge(
    edges[group_id %in% interior_lookup$group_id],
    split_segments,
    by = "group_id",
    allow.cartesian = TRUE,
    sort = FALSE
  )

  split_edges[, is_forward := from == from0 & to == to0]
  split_edges[, `:=`(
    from = fifelse(is_forward, seg_from, seg_to),
    to = fifelse(is_forward, seg_to, seg_from)
  )]

  # Split additive attributes proportionally to segment length and copy the
  # remaining edge attributes to each generated row.
  for (attrib_name in additive_attr_names) {
    split_edges[, (attrib_name) := get(attrib_name) * seg_frac]
  }

  split_edges <- split_edges[, c("from", "to", edge_attr_names), with = FALSE]

  retained_edges <- edges[!group_id %in% interior_lookup$group_id]
  edges <- rbindlist(
    list(
      retained_edges[, c("from", "to", edge_attr_names), with = FALSE],
      split_edges[, c("from", "to", edge_attr_names), with = FALSE]
    ),
    use.names = TRUE
  )

  # Register the new projected nodes in the graph dictionary and vertex table.
  dict <- rbind(
    dict,
    interior_projections[, list(ref = vertex_id, id = new_id)],
    fill = TRUE
  )

  vertices <- rbind(
    vertices,
    interior_projections[, list(vertex_id, x = proj_x, y = proj_y)],
    fill = TRUE
  )
  vertices <- unique(vertices[, list(vertex_id, x, y)], by = "vertex_id")

  # Rebuild the cppRouting graph object and keep only the exported vertex
  # fields.
  used_node_ids <- sort(unique(c(edges$from, edges$to)))
  id_map <- data.table(id = used_node_ids, new_id = 0:(length(used_node_ids) - 1L))

  edges <- merge(edges, id_map, by.x = "from", by.y = "id", all.x = TRUE, sort = FALSE)
  setnames(edges, "new_id", "from_new")
  edges <- merge(edges, id_map, by.x = "to", by.y = "id", all.x = TRUE, sort = FALSE)
  setnames(edges, "new_id", "to_new")
  edges[, `:=`(from = from_new, to = to_new)]
  edges[, c("from_new", "to_new") := NULL]

  dict <- merge(dict, id_map, by = "id", all = FALSE, sort = FALSE)
  dict[, id := new_id]
  dict[, new_id := NULL]
  setorder(dict, id)

  # Rebuild the vertex table from the dictionary so the exported coordinates use
  # exactly the same external ids as the graph dictionary.
  vertices <- merge(
    dict[, list(vertex_id = ref)],
    vertices[, list(vertex_id, x, y)],
    by = "vertex_id",
    all.x = TRUE,
    sort = FALSE
  )

  if (anyNA(vertices$x) || anyNA(vertices$y)) {
    stop("Some graph dictionary refs are missing from the vertex table after OD insertion.")
  }

  cppr_graph[["data"]] <- as.data.frame(edges[, list(from, to, dist)])
  cppr_graph[["dict"]] <- dict
  cppr_graph[["nbnode"]] <- nrow(dict)
  cppr_graph_attrib <- cppr_graph[["attrib"]]

  if ("aux" %in% colnames(edges)) {
    cppr_graph_attrib[["aux"]] <- edges$aux
  }
  if ("cap" %in% colnames(edges)) {
    cppr_graph_attrib[["cap"]] <- edges$cap
  }
  if ("alpha" %in% colnames(edges)) {
    cppr_graph_attrib[["alpha"]] <- edges$alpha
  }
  if ("beta" %in% colnames(edges)) {
    cppr_graph_attrib[["beta"]] <- edges$beta
  }

  for (attrib_name in setdiff(colnames(edges), c("from", "to", "dist", "aux", "cap", "alpha", "beta"))) {
    cppr_graph_attrib[[attrib_name]] <- edges[[attrib_name]]
  }

  cppr_graph[["attrib"]] <- cppr_graph_attrib

  list(
    graph = cppr_graph,
    vertices = vertices
  )

}


insert_projected_od_nodes <- function(
    cppr_graph,
    vertices,
    od_points,
    mode,
    projection_bin_size = 10.0,
    endpoint_tolerance = 1.0
  ) {

  # Compute geometric projections, then rewrite the graph topology at those
  # projected locations.
  od_points <- as.data.table(od_points)

  if (nrow(od_points) == 0) {
    empty_od_vertex_map <- copy(od_points)
    empty_od_vertex_map[, building_id := integer()]
    return(list(
      graph = cppr_graph,
      vertices = vertices,
      od_vertex_map = empty_od_vertex_map[, list(building_id, vertex_id = character())]
    ))
  }

  projection_data <- compute_od_edge_projections(
    cppr_graph = cppr_graph,
    vertices = vertices,
    od_points = od_points,
    mode = mode,
    projection_bin_size = projection_bin_size,
    endpoint_tolerance = endpoint_tolerance
  )

  split_result <- split_cppr_graph_at_projections(
    cppr_graph = cppr_graph,
    vertices = vertices,
    projection_data = projection_data
  )

  # Build the OD-to-node attachment table from the computed projections.
  od_vertex_map <- projection_data$attachments[, list(
    building_id,
    vertex_id
  )]

  list(
    graph = split_result$graph,
    vertices = split_result$vertices,
    od_vertex_map = od_vertex_map
  )

}
