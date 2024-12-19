library(FNN)


concatenate_graphs <- function(
    start_graph, mid_graph, last_graph,
    start_verts, mid_verts, last_verts,
    first_modal_shift,
    last_modal_shift
) {
  
  
  start_graph <- filter_vertices(
    start_graph,
    start_verts,
    mid_verts,
    first_modal_shift$average_speed,
    first_modal_shift$max_travel_time
  )

  start_verts <- start_verts[vertex_id %in% start_graph$dict$ref]

  last_graph <- filter_vertices(
    last_graph,
    last_verts,
    mid_verts,
    last_modal_shift$average_speed,
    last_modal_shift$max_travel_time
  )

  last_verts <- last_verts[vertex_id %in% last_graph$dict$ref]
  
  # Store original travel times before contraction to be able to add them back later
  start_time <- start_graph$data$dist
  mid_time <- mid_graph$data$dist
  last_time <- last_graph$data$dist
  
  # Apply the contraction hierarchies to the graphs
  start_graph <- cpp_contract(start_graph)
  last_graph <- cpp_contract(last_graph)
  mid_graph <- cpp_contract(mid_graph)
  
  # Make all vertex indices and names unique
  start_verts[, vertex_id := paste0("s", vertex_id)]
  mid_verts[, vertex_id := paste0("m", vertex_id)]
  last_verts[, vertex_id := paste0("l", vertex_id)]
  
  start_graph <- rename_vertices(start_graph, "s", 0, contracted_graph = TRUE)
  mid_graph <- rename_vertices(mid_graph, "m", max(start_graph$dict$id) + 1, contracted_graph = TRUE)
  last_graph <- rename_vertices(last_graph, "l", max(mid_graph$dict$id) + 1, contracted_graph = TRUE)
  
  # Add modal shift times
  mid_graph <- add_modal_shift_times(
    mid_graph,
    mid_verts,
    first_modal_shift,
    last_modal_shift
  )
  
  # Map the vertices of the start graph to the ones of the middle one
  # and then the vertices of the middle graph to the ones of the last one
  mid_graph <- map_vertices(mid_graph, start_graph, mid_verts[grepl("mdep-", vertex_id)], start_verts, "from")
  mid_graph <- map_vertices(mid_graph, last_graph, mid_verts[grepl("marr-", vertex_id)], last_verts, "to")
  
  first_con_verts <- mid_graph$data[, unique(from)]
  last_con_verts <- mid_graph$data[, unique(to)]
  
  # Shift the ranks of the connection nodes after the non connection nodes
  # of the first graph
  start_dict <- copy(start_graph$dict)
  start_dict[, old_rank := start_graph$rank]
  start_dict[, is_con_vertex := id %in% first_con_verts]
  start_dict[, new_rank := rank(old_rank), by = is_con_vertex]
  
  max_non_con_rank <- max(start_dict[is_con_vertex == FALSE, new_rank])
  start_dict[is_con_vertex == TRUE, new_rank := new_rank + max_non_con_rank]
  
  max_rank <- nrow(start_dict)
  
  # Shift the ranks of the connection nodes before the non connection nodes
  # of the last graph
  last_dict <- copy(last_graph$dict)
  last_dict[, old_rank := last_graph$rank]
  last_dict[, is_con_vertex := id %in% last_con_verts]
  last_dict[, new_rank := rank(old_rank), by = is_con_vertex]
  
  max_con_rank <- max(last_dict[is_con_vertex == TRUE, new_rank])
  last_dict[is_con_vertex == TRUE, new_rank := new_rank + max_rank]
  last_dict[is_con_vertex == FALSE, new_rank := new_rank + max_rank + max_con_rank]
  
  # Shift all vertices ids of the last graph to account for the fact that 
  # mid graph nodes were mapped to first and last graphs (so should not appear
  # in the graph anymore)
  offset <- nrow(mid_graph$dict)
  
  last_graph$dict[, id := id - offset]
  last_graph$data[, from := from - offset]
  last_graph$data[, to := to - offset]
  last_graph$shortcuts[, shortf := shortf - offset]
  last_graph$shortcuts[, shortt := shortt - offset]
  last_graph$shortcuts[, shortc := shortc - offset]
  last_graph$original$data[, from := from - offset]
  last_graph$original$data[, to := to - offset]
  
  mid_graph$data[, to := to - offset]
  mid_graph$original$data[, to := to - offset]
  
  # Combine the three graphs
  data <- rbind(
    start_graph$data,
    mid_graph$data,
    last_graph$data
  )
  
  shortcuts <- rbind(
    start_graph$shortcuts,
    last_graph$shortcuts
  )
  
  dict <- rbind(
    start_graph$dict,
    last_graph$dict
  )
  
  rank <- c(
    start_dict$new_rank,
    last_dict$new_rank
  )
  
  original_data <- rbind(
    start_graph$original$data,
    mid_graph$original$data,
    last_graph$original$data
  )
  
  aux <- c(
    start_graph$original$attrib$aux,
    mid_graph$original$attrib$aux,
    last_graph$original$attrib$aux
  )
  
  start_dist <- c(
    start_graph$original$attrib$aux,
    rep(0.0, length(mid_graph$original$attrib$aux)),
    rep(0.0, length(last_graph$original$attrib$aux))
  )
  
  mid_dist <- c(
    rep(0.0, length(start_graph$original$attrib$aux)),
    mid_graph$original$attrib$aux,
    rep(0.0, length(last_graph$original$attrib$aux))
  )
  
  last_dist <- c(
    rep(0.0, length(start_graph$original$attrib$aux)),
    rep(0.0, length(mid_graph$original$attrib$aux)),
    last_graph$original$attrib$aux
  )
  
  start_time <- c(
    start_time,
    rep(0.0, length(mid_graph$original$attrib$aux)),
    rep(0.0, length(last_graph$original$attrib$aux))
  )
  
  last_time <- c(
    rep(0.0, length(start_graph$original$attrib$aux)),
    rep(0.0, length(mid_graph$original$attrib$aux)),
    last_time
  )
  
  graph <- list(
    data = data,
    rank = rank,
    shortcuts = shortcuts,
    nbnode = nrow(dict),
    dict = dict,
    original = list(
      data = original_data,
      attrib = list(
        aux = aux,
        start_distance = start_dist,
        mid_distance = mid_dist,
        last_distance = last_dist,
        start_time = start_time,
        last_time = last_time,
        alpha = NULL,
        beta = NULL,
        cap = NULL
      )
    )
  )
  
  return(graph)
  
}



filter_vertices <- function(graph, verts, con_verts, average_speed, max_travel_time) {
  
  data <- as.data.table(graph$data)
  dict <- as.data.table(graph$dict)
  
  # Keep only vertices in the start graph that are within a given travel time
  # from a connection vertex
  knn <- get.knnx(
    con_verts[, list(x, y)],
    verts[, list(x, y)],
    k = 1
  )
  2
  d <- as.numeric(knn$nn.dist)
  verts <- verts[d/1000/average_speed < max_travel_time*1.5]
  
  vertex_ids <- verts$vertex_id
  dict <- dict[ref %in% vertex_ids]
  aux <- graph$attrib$aux[data[, from %in% dict$id & to %in% dict$id]]
  data <- data[from %in% dict$id & to %in% dict$id]
  
  dict[, new_id := 0:(.N-1)]
  
  data <- merge(data, dict[, list(id, new_id)], by.x = "from", by.y = "id", sort = FALSE)
  data <- merge(data, dict[, list(id, new_id)], by.x = "to", by.y = "id", suffixes = c("_from", "_to"), sort = FALSE)
  data <- data[, list(from = new_id_from, to = new_id_to, dist)]
  
  dict <- dict[, list(ref, id = new_id)]
  
  graph$data <- data
  graph$dict <- dict
  graph$attrib$aux <- aux
  graph$nbnode <- nrow(dict)
  
  return(graph)
  
}


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
  
  if (contracted_graph == TRUE) {
    
    shortcuts <- as.data.table(graph$shortcuts)
    shortcuts <- merge(shortcuts, dict[, list(id, new_id)], by.x = "shortf", by.y = "id", sort = FALSE)
    shortcuts <- merge(shortcuts, dict[, list(id, new_id)], by.x = "shortt", by.y = "id", suffixes = c("_from", "_to"), sort = FALSE)
    shortcuts <- merge(shortcuts, dict[, list(id, new_id)], by.x = "shortc", by.y = "id", sort = FALSE)
    graph$shortcuts <- shortcuts[, list(shortf = new_id_from, shortt = new_id_to, shortc = new_id)]
    
    original_data <- as.data.table(graph$original$data)
    original_data <- merge(original_data, dict[, list(id, new_id)], by.x = "from", by.y = "id", sort = FALSE)
    original_data <- merge(original_data, dict[, list(id, new_id)], by.x = "to", by.y = "id", suffixes = c("_from", "_to"), sort = FALSE)
    graph$original$data <- original_data[, list(from = new_id_from, to = new_id_to, dist)]
    
  }
  
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
  # graph_1$dict <- dict[, list(ref, id = id_new)]
  graph_1$original$data <- original_data
  
  return(graph_1)
  
}


add_modal_shift_times <- function(graph, verts, first_modal_shift, last_modal_shift) {
  
  data <- as.data.table(graph$data)
  original_data <- as.data.table(graph$original$data)
  
  dict <- as.data.table(graph$dict)
  dict[, has_shortcut_from := FALSE]
  dict[, has_shortcut_to := FALSE]
  
  if (!is.null(first_modal_shift$shortcuts_locations)) {
    
    shortcuts_locations <- as.data.frame(last_modal_shift$shortcuts_locations)
    colnames(shortcuts_locations) <- c("lon", "lat")
    
    sc_locations <- sfheaders::sf_point(shortcuts_locations, x = "lon", y = "lat")
    st_crs(sc_locations) <- 4326
    sc_locations <- st_transform(sc_locations, 3035)
    sc_locations <- st_coordinates(sc_locations)
    
    verts <- verts[vertex_id %in% graph$dict$ref, ]
    
    knn <- get.knnx(
      sc_locations,
      verts[, list(x, y)],
      k = 1
    )
    
    sc_vertices <- verts[as.vector(knn$nn.dist < 400), vertex_id]
    
    dict[, has_shortcut_from := ref %in% sc_vertices]
    
    data <- merge(data, dict[, list(id, has_shortcut_from)], by.x = "from", by.y = "id", sort = FALSE)
    original_data <- merge(original_data, dict[, list(id, has_shortcut_from)], by.x = "from", by.y = "id", sort = FALSE)
    
  } else {
    
    data[, has_shortcut_from := FALSE]
    original_data[, has_shortcut_from := FALSE]
    
  }
  
  if (!is.null(last_modal_shift$shortcuts_locations)) {
    
    shortcuts_locations <- as.data.frame(last_modal_shift$shortcuts_locations)
    colnames(shortcuts_locations) <- c("lon", "lat")
    
    sc_locations <- sfheaders::sf_point(shortcuts_locations, x = "lon", y = "lat")
    st_crs(sc_locations) <- 4326
    sc_locations <- st_transform(sc_locations, 3035)
    sc_locations <- st_coordinates(sc_locations)
    
    verts <- verts[vertex_id %in% graph$dict$ref, ]
    
    knn <- get.knnx(
      sc_locations,
      verts[, list(x, y)],
      k = 1
    )
    
    sc_vertices <- verts[as.vector(knn$nn.dist < 400), vertex_id]
    
    dict[, has_shortcut_to := ref %in% sc_vertices]
    
    data <- merge(data, dict[, list(id, has_shortcut_to)], by.x = "to", by.y = "id", sort = FALSE)
    original_data <- merge(original_data, dict[, list(id, has_shortcut_to)], by.x = "to", by.y = "id", sort = FALSE)
    
  } else {
    
    data[, has_shortcut_to := FALSE]
    original_data[, has_shortcut_to := FALSE]
    
  }
  

  data[, dist := ifelse(
    has_shortcut_from == TRUE,
    dist + first_modal_shift$shortcuts_shift_time*60,
    dist + first_modal_shift$shift_time*60
  )]
  
  data[, dist := ifelse(
    has_shortcut_to == TRUE,
    dist + last_modal_shift$shortcuts_shift_time*60,
    dist + last_modal_shift$shift_time*60
  )]
  
  data[, has_shortcut_from := NULL]
  data[, has_shortcut_to := NULL]
  
  original_data[, dist := ifelse(
    has_shortcut_from == TRUE,
    dist + first_modal_shift$shortcuts_shift_time*60,
    dist + first_modal_shift$shift_time*60
  )]
  
  original_data[, dist := ifelse(
    has_shortcut_to == TRUE,
    dist + last_modal_shift$shortcuts_shift_time*60,
    dist + last_modal_shift$shift_time*60
  )]
  
  original_data[, has_shortcut_from := NULL]
  original_data[, has_shortcut_to := NULL]
  
  graph$data <- data
  graph$original$data <- original_data
  
  return(graph)
  
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
  gen_data <- gen_data[, list(from = id_from, to = id_to, dist = 60*modal_shift$shift_time + 3600*internal_distance/2/1000/modal_shift$average_speed)]
  
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
