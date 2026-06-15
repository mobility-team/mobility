#' Build the edge table used to create the path graph.
#'
#' @param graph Weighted dodgr street graph.
#' @param mode Mobility mode name.
#' @param osm_data OSM data returned by osmdata_sc().
#' @param osm_capacity_parameters Table of highway classes and capacity parameters.
#' @return data.table of directed edges.
build_path_edges <- function(graph, mode, osm_data, osm_capacity_parameters) {
  if (mode == "car") {
    return(build_car_edges(graph, osm_data, osm_capacity_parameters))
  }

  as.data.table(graph[, c(".vx0", ".vx1", "time_weighted", "d")])
}

#' Build a vertex coordinate table from a dodgr street graph.
#'
#' @param graph Weighted dodgr street graph.
#' @return data.table with vertex_id, x, and y columns in WGS84 coordinates.
build_graph_vertices <- function(graph) {
  vertices <- as.data.table(graph[, c(".vx0", ".vx0_x", ".vx0_y", ".vx1", ".vx1_x", ".vx1_y")])
  vertices <- rbind(
    vertices[, list(vertex_id = .vx0, x = .vx0_x, y = .vx0_y)][!duplicated(vertex_id)],
    vertices[, list(vertex_id = .vx1, x = .vx1_x, y = .vx1_y)][!duplicated(vertex_id)]
  )
  vertices[!duplicated(vertex_id)]
}

#' Project graph vertex coordinates to LAEA Europe.
#'
#' @param vertices data.table with vertex_id, x, and y columns in WGS84 coordinates.
#' @return data.table with vertex_id, x, and y columns in EPSG:3035 coordinates.
project_graph_vertices <- function(vertices) {
  vertices_3035 <- sfheaders::sf_point(vertices, x = "x", y = "y", keep = TRUE)
  st_crs(vertices_3035) <- 4326
  vertices_3035 <- st_transform(vertices_3035, 3035)
  vertices_3035 <- as.data.table(cbind(st_drop_geometry(vertices_3035), st_coordinates(vertices_3035)))
  setnames(vertices_3035, c("vertex_id", "x", "y"))

  vertices_3035
}

#' Add the car direct-access attribute to a cppRouting graph.
#'
#' The direct-access flag marks road edges that OD points can attach to.
#'
#' @param cppr_graph cppRouting graph.
#' @param edges data.table of directed car edges.
#' @return cppRouting graph with a direct_access edge attribute.
add_car_direct_access_attribute <- function(cppr_graph, edges) {
  direct_access_highways <- c(
    "primary",
    "secondary",
    "tertiary",
    "unclassified",
    "residential",
    "living_street",
    "service",
    "road"
  )

  direct_access <- edges$highway %in% direct_access_highways
  direct_access <- direct_access & edges$car_access_allowed

  cppr_graph[["attrib"]][["direct_access"]] <- as.numeric(direct_access)
  cppr_graph
}

#' Build a cppRouting graph from path edges.
#'
#' @param edges data.table of directed path edges.
#' @param mode Mobility mode name.
#' @return cppRouting graph with distance, time, and optional car congestion attributes.
build_cppr_path_graph <- function(edges, mode) {
  if (mode == "car") {
    cppr_graph <- makegraph(
      df = edges[, list(.vx0, .vx1, time_weighted)],
      directed = TRUE,
      aux = edges$d,
      capacity = edges$capacity,
      alpha = edges$alpha,
      beta = edges$beta
    )
  } else {
    cppr_graph <- makegraph(
      df = edges[, list(.vx0, .vx1, time_weighted)],
      directed = TRUE,
      aux = edges$d
    )
  }

  cppr_graph[["attrib"]][["n_edges"]] <- rep(1.0, nrow(cppr_graph[["data"]]))

  if (mode == "car") {
    cppr_graph <- add_car_direct_access_attribute(cppr_graph, edges)
  }

  cppr_graph
}

#' Remove dead-end edges from a simplified graph edge table.
#'
#' @param edges data.table with from and to graph node indexes.
#' @return data.table with recursively removed dead-end edges.
remove_dead_end_edges <- function(edges) {
  nodes <- c(0)

  while (length(nodes) > 0) {
    n_incoming <- edges[, list(n_incoming = .N), by = list(index = to)]
    n_outgoing <- edges[, list(n_outgoing = .N), by = list(index = from)]

    degree <- merge(n_incoming, n_outgoing, by = "index", all = TRUE)
    degree[is.na(n_incoming), n_incoming := 0]
    degree[is.na(n_outgoing), n_outgoing := 0]
    degree[, deg := n_incoming + n_outgoing]

    one_way_dead_end_nodes <- degree[deg == 1, index]

    two_way_nodes <- degree[deg == 2, list(index)]
    two_way_nodes <- merge(two_way_nodes, edges[, list(from, to)], by.x = "index", by.y = "from")
    two_way_nodes <- merge(two_way_nodes, edges[, list(from, to)], by.x = "index", by.y = "to")
    two_way_dead_end_nodes <- two_way_nodes[from == to, index]

    nodes <- unique(c(one_way_dead_end_nodes, two_way_dead_end_nodes))

    edges <- edges[!(from %in% nodes | to %in% nodes)]
  }

  edges
}

#' Rebuild a cppRouting graph after dead-end pruning.
#'
#' @param cppr_graph_simple Simplified cppRouting graph before pruning.
#' @param edges Pruned edge table using the original node indexes.
#' @return cppRouting graph with compacted node indexes and aligned attributes.
rebuild_cppr_graph_after_pruning <- function(cppr_graph_simple, edges) {
  remaining_node_index <- unique(c(edges$from, edges$to))

  data <- as.data.table(cppr_graph_simple[["data"]])
  data[, keep_from := from %in% remaining_node_index]
  data[, keep_to := to %in% remaining_node_index]

  keep_index <- data[, keep_from & keep_to]

  dict <- as.data.table(cppr_graph_simple[["dict"]])
  dict <- dict[id %in% remaining_node_index]
  dict[, new_index := 0:(.N - 1)]

  data <- merge(data, dict[, list(id, new_index)], by.x = "from", by.y = "id", sort = FALSE)
  data <- merge(data, dict[, list(id, new_index)], by.x = "to", by.y = "id", sort = FALSE)
  data <- data[, list(from = new_index.x, to = new_index.y, dist)]

  dict <- dict[, list(ref, id = new_index)]

  cppr_graph_simple[["attrib"]][["aux"]] <- cppr_graph_simple[["attrib"]][["aux"]][keep_index]
  cppr_graph_simple[["attrib"]][["n_edges"]] <- cppr_graph_simple[["attrib"]][["n_edges"]][keep_index]
  if ("direct_access" %in% names(cppr_graph_simple[["attrib"]])) {
    cppr_graph_simple[["attrib"]][["direct_access"]] <- cppr_graph_simple[["attrib"]][["direct_access"]][keep_index]
  }
  cppr_graph_simple[["attrib"]][["alpha"]] <- cppr_graph_simple[["attrib"]][["alpha"]][keep_index]
  cppr_graph_simple[["attrib"]][["beta"]] <- cppr_graph_simple[["attrib"]][["beta"]][keep_index]
  cppr_graph_simple[["attrib"]][["cap"]] <- cppr_graph_simple[["attrib"]][["cap"]][keep_index]

  cppr_graph_simple[["data"]] <- data
  cppr_graph_simple[["dict"]] <- dict
  cppr_graph_simple[["nbnode"]] <- nrow(dict)

  cppr_graph_simple
}

#' Simplify a cppRouting graph, prune dead ends, then simplify again.
#'
#' @param cppr_graph cppRouting graph before simplification.
#' @param mode Mobility mode name.
#' @return Simplified and pruned cppRouting graph.
simplify_and_prune_cppr_graph <- function(cppr_graph, mode) {
  cppr_graph_simple <- simplify_cppr_graph(cppr_graph, mode = mode)

  edges <- remove_dead_end_edges(as.data.table(cppr_graph_simple[["data"]]))
  cppr_graph_simple <- rebuild_cppr_graph_after_pruning(cppr_graph_simple, edges)

  simplify_cppr_graph(cppr_graph_simple, mode = mode)
}
