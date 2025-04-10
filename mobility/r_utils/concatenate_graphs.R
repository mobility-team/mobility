library(FNN)

concatenate_graphs_carpool <- function(
    start_graph, last_graph,
    start_verts, last_verts,
    modal_shift,
    transport_zones,
    last_graph_speed_coeff
) {
  
  start_verts <- start_verts[vertex_id %in% start_graph$dict$ref]
  last_verts <- last_verts[vertex_id %in% last_graph$dict$ref]
  
  # Make all vertex indices and names unique
  start_verts[, vertex_id := paste0("s", vertex_id)]
  last_verts[, vertex_id := paste0("l", vertex_id)]
  
  start_graph <- rename_vertices(start_graph, "s", 0, contracted_graph = FALSE)
  last_graph <- rename_vertices(last_graph, "l", max(start_graph$dict$id) + 1, contracted_graph = FALSE)
  
  # Make the last carpooling edges slightly faster to make the router want to
  # switch from solo drive to carpooling
  last_graph$data[, dist := dist*last_graph_speed_coeff]
  
  # Add modal shift times
  start_graph <- add_carpooling_edges(
    start_graph,
    last_graph,
    start_verts,
    last_verts,
    transport_zones,
    modal_shift
  )
  
  # Combine the two graphs
  data <- rbind(start_graph$data, last_graph$data)
  dict <- rbind(start_graph$dict, last_graph$dict)
  
  graph <- list(
    data = data,
    coords = NULL,
    nbnodes = nrow(data),
    dict = dict,
    attrib = list(
      aux = NULL,
      alpha = NULL,
      beta = NULL,
      cap = NULL
    )
  )
  
  # Contract the graph for faster routing
  graph <- cpp_contract(graph)
  
  # Add the aux weights
  graph$original$attrib$aux <- c(
    start_graph$attrib$aux,
    last_graph$attrib$aux
  )
  
  graph$original$attrib$distance <- graph$original$attrib$aux
  
  graph$original$attrib$carpooling_distance <- c(
    rep(0.0, nrow(start_graph$data)),
    last_graph$attrib$aux
  )
  
  graph$original$attrib$carpooling_time <- c(
    rep(0.0, nrow(start_graph$data)),
    last_graph$data$dist
  )
  
  return(graph)
  
}


rename_vertices <- function(graph, prefix, n_start, contracted_graph) {
  
  data <- as.data.table(graph$data)
  dict <- as.data.table(graph$dict)
  
  # Make all refs and ids unique
  dict[, ref := paste0(prefix, ref)]
  dict[, new_id := n_start:(n_start + nrow(graph$dict) - 1)]
  
  data <- merge(data, dict[, list(id, new_id)], by.x = "from", by.y = "id", sort = FALSE)
  data <- merge(data, dict[, list(id, new_id)], by.x = "to", by.y = "id", suffixes = c("_from", "_to"), sort = FALSE)
  
  graph$data <- data[, list(from = new_id_from, to = new_id_to, dist)]
  graph$dict <- dict[, list(ref, id = new_id)]
  
  return(graph)
  
}


map_vertices <- function(graph_1, graph_2, verts_1, verts_2, vertex_type) {
  
  knn <- get.knnx(
    verts_2[, list(x, y)],
    verts_1[, list(x, y)],
    k = 1
  )
  
  verts_1[, new_vertex_id := verts_2$vertex_id[knn$nn.index]]
  
  dict <- as.data.table(graph_1$dict)
  dict <- merge(dict, verts_1[, list(vertex_id, new_vertex_id)], by.x = "ref", by.y = "vertex_id")
  dict <- merge(dict, graph_2$dict, by.x = "new_vertex_id", by.y = "ref", suffixes = c("", "_new"))
  
  if (vertex_type == "from") {
    
    data <- merge(graph_1$data, dict[, list(id, id_new)], by.x = "from", by.y = "id", sort = FALSE)
    data <- data[, list(from = id_new, to, dist)]
    
    original_data <- merge(graph_1$original$data, dict[, list(id, id_new)], by.x = "from", by.y = "id", sort = FALSE)
    original_data <- original_data[, list(from = id_new, to, dist)]
    
  } else {
    
    data <- merge(graph_1$data, dict[, list(id, id_new)], by.x = "to", by.y = "id", sort = FALSE)
    data <- data[, list(from, to = id_new, dist)]
    
    original_data <- merge(graph_1$original$data, dict[, list(id, id_new)], by.x = "to", by.y = "id", sort = FALSE)
    original_data <- original_data[, list(from, to = id_new, dist)]
    
  }
  
  graph_1$data <- data
  graph_1$original$data <- original_data
  
  return(graph_1)
  
}


add_carpooling_edges <- function(
    start_graph, last_graph,
    start_verts, last_verts,
    transport_zones,
    modal_shift
  ) {
  
  # Prepare the vertices of the generic modal shift : one vertex per transport
  # zone, which will be accessible in a given time from all vertices that are close
  # (400 m radius)
  tz_verts <- as.data.table(st_drop_geometry(transport_zones))
  tz_verts <- tz_verts[, list(
    vertex_id = paste0("i", transport_zone_id),
    x = x,
    y = y,
    internal_distance
  )]
  
  # Find which vertices are close to the generic modal shift vertex
  start_knn <- get.knnx(
    start_verts[, list(x, y)],
    tz_verts[, list(x, y)],
    k = 1
  )
  
  last_knn <- get.knnx(
    last_verts[, list(x, y)],
    tz_verts[, list(x, y)],
    k = 1
  )
  
  gen_data <- data.table(
    from_vertex_id = start_verts[start_knn$nn.index, vertex_id],
    to_vertex_id = last_verts[last_knn$nn.index, vertex_id],
    internal_distance = tz_verts$internal_distance
  )
  
  gen_data <- merge(gen_data, start_graph$dict, by.x = "from_vertex_id", by.y = "ref")
  gen_data <- merge(gen_data, last_graph$dict, by.x = "to_vertex_id", by.y = "ref", suffixes = c("_from", "_to"))
  gen_data <- gen_data[, list(from = id_from, to = id_to, dist = 60*modal_shift$transfer_time + 3600*internal_distance/2/1000/modal_shift$average_speed)]
  
  if (!is.null(modal_shift$shortcuts_locations)) {
    
    shortcuts_locations <- as.data.frame(modal_shift$shortcuts_locations)
    colnames(shortcuts_locations) <- c("lon", "lat")
    
    sc_locations <- sfheaders::sf_point(shortcuts_locations, x = "lon", y = "lat")
    st_crs(sc_locations) <- 4326
    sc_locations <- st_transform(sc_locations, 3035)
    sc_locations <- as.data.table(st_coordinates(sc_locations))
    
    start_knn <- get.knnx(
      start_verts[, list(x, y)],
      sc_locations[, list(X, Y)],
      k = 1
    )
    
    last_knn <- get.knnx(
      last_verts[, list(x, y)],
      sc_locations[, list(X, Y)],
      k = 1
    )
    
    sc_data <- data.table(
      from_vertex_id = start_verts[start_knn$nn.index, vertex_id],
      to_vertex_id = last_verts[last_knn$nn.index, vertex_id]
    )
    
    sc_data <- merge(sc_data, start_graph$dict, by.x = "from_vertex_id", by.y = "ref")
    sc_data <- merge(sc_data, last_graph$dict, by.x = "to_vertex_id", by.y = "ref", suffixes = c("_from", "_to"))
    sc_data <- sc_data[, list(from = id_from, to = id_to, dist = 60*modal_shift$shortcuts_shift_time)]
    
    
  } else {
    
    sc_data <- data.table(from = integer(), to = integer(), dist = integer())
    
  }
  
  data <- rbindlist(list(gen_data, sc_data))
  distance <- rep(0.0, nrow(data))
  
  start_graph$data <- rbindlist(list(start_graph$data, data))
  
  start_graph$attrib$aux <- c(start_graph$attrib$aux, distance)
  start_graph$attrib$distance <- start_graph$attrib$aux
  
  return(start_graph)
  
}
