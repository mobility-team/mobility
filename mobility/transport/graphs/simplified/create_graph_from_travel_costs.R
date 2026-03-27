


create_graph_from_travel_costs <- function(travel_costs) {
  
  stop_ids <- unique(c(
    gtfs_travel_costs$from_stop_id,
    gtfs_travel_costs$to_stop_id
  ))
  
  dict <- data.table(
    ref = stop_ids,
    id = 0:(length(stop_ids)-1)
  )
  
  travel_costs <- merge(travel_costs, dict, by.x = "from_stop_id", by.y = "ref")
  travel_costs <- merge(travel_costs, dict, by.x = "to_stop_id", by.y = "ref", suffixes = c("_from", "_to"))
  
  data <- travel_costs[, list(from = id_from, to = id_to, dist = time)]
  aux <- travel_costs$distance
  
  graph <- list(
    data = as.data.frame(data),
    coords = NULL,
    nbnodes = nrow(data),
    dict = dict,
    attrib = list(
      aux = aux,
      alpha = NULL,
      beta = NULL,
      cap = NULL
    )
  )
  
  return(graph)
  
}


# 
# create_graph_from_travel_costs <- function(
#     travel_costs,
#     stops,
#     orig_graph,
#     orig_verts,
#     dest_graph,
#     dest_verts
#   ) {
#   
#   
#   # Associate each GTFS stop with its closest graph vertex
#   orig_knn <- get.knnx(
#     orig_verts[, list(x, y)],
#     stops[, list(X, Y)],
#     k = 1
#   )
#   
#   dest_knn <- get.knnx(
#     dest_verts[, list(x, y)],
#     stops[, list(X, Y)],
#     k = 1
#   )
#   
#   stops_to_vertices <- data.table(
#     stop_id = stops$stop_id,
#     orig_vertex_id = orig_verts[orig_knn$nn.index, vertex_id],
#     dest_vertex_id = dest_verts[dest_knn$nn.index, vertex_id]
#   )
#   
#   travel_costs <- merge(
#     travel_costs,
#     stops_to_vertices[, list(stop_id, orig_vertex_id)],
#     by.x = "from_stop_id",
#     by.y = "stop_id"
#   )
#   
#   travel_costs <- merge(
#     travel_costs,
#     stops_to_vertices[, list(stop_id, dest_vertex_id)],
#     by.x = "to_stop_id",
#     by.y = "stop_id"
#   )
#   
#   travel_costs <- merge(travel_costs, orig_graph$dict, by.x = "orig_vertex_id", by.y = "ref")
#   travel_costs <- merge(travel_costs, dest_graph$dict, by.x = "dest_vertex_id", by.y = "ref", suffixes = c("_from", "_to"))
#   
#   data <- travel_costs[, list(from = id_from, to = id_to, dist = time)]
#   dict <- data.frame(ref = character(), id = integer())
#   aux <- travel_costs$distance
#   
#   graph <- list(
#     data = as.data.frame(data),
#     coords = NULL,
#     nbnodes = nrow(data),
#     dict = dict,
#     attrib = list(
#       aux = aux,
#       alpha = NULL,
#       beta = NULL,
#       cap = NULL
#     )
#   )
#   
#   return(graph)
#   
# }