
apply_border_crossing_speed_modifier <- function(
    cppr_graph,
    vertices,
    modifier_type,
    max_speed,
    time_penalty,
    borders,
    has_borders
) {
  
  info(logger, "Applying border crossing modifier...")

  if (has_borders == FALSE) {

    return(cppr_graph)

  } else {

    # Create sf line segments for all graph edges
    data <- as.data.table(cppr_graph$data)
    data[, edge_id := .I]
    dict <- as.data.table(cppr_graph$dict)
    edges <- merge(data, dict, by.x = "from", by.y = "id")
    edges <- merge(edges, dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
    edges <- merge(edges, vertices, by.x = "ref_from", by.y = "vertex_id")
    edges <- merge(edges, vertices, by.x = "ref_to", by.y = "vertex_id", suffixes = c("_from", "_to"))
    
    edges <- rbindlist(
      list(
        edges[, list(edge_id, x = x_from, y = y_from)],
        edges[, list(edge_id, x = x_to, y = y_to)]
      )
    )
    
    edges_sf <- sfheaders::sf_linestring(
      edges[order(edge_id)],
      x = "x",
      y = "y",
      linestring_id = "edge_id",
      keep = TRUE
    )
    
    st_crs(edges_sf) <- 3035
    
    # Find the ones intersecting a border
    borders <- lapply(borders, st_read, quiet = TRUE)
    borders <- lapply(borders, "[", "admin_level")
    borders <- st_as_sf(rbindlist(borders))
    borders <- st_transform(borders, 3035)
    
    index <- st_intersects(edges_sf, borders)
    index <- lengths(index) > 0
    
    edge_ids <- edges_sf$edge_id[index]
    
    # Convert the max speed to m/s and the time penalty to s to match the graph units
    max_speed <- max_speed/3.6
    time_penalty <- time_penalty*60.0
    
    # Compute the speed on each edge
    # It can be confusing because cppRouting calls the edge cost "dist", so in our 
    # case dist is the travel time and aux is the travel distance.
    data[, speed := cppr_graph$attrib$aux/dist]
    
    # Cap the speed of the edges that go through a border
    data[edge_id %in% edge_ids, speed := pmin(max_speed, speed)]
    
    # Recompute the travel time and add the time penalty for those edges
    data[edge_id %in% edge_ids, dist := cppr_graph$attrib$aux[edge_id]/speed + time_penalty]
    
    cppr_graph$data <- data[, list(from, to, dist)]
    
    return(cppr_graph)

  }
  

  
}
