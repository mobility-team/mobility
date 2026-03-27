


duplicate_cpprouting_graph <- function(graph, vertices, prefix = "d") {

  data <- as.data.table(graph$data)
  dict <- as.data.table(graph$dict)
  
  dict[, new_id := 1:nrow(dict) + max(dict$id)]
  dict[, new_ref := paste0(prefix, dict$ref)]
  
  data <- merge(data, dict[, list(id, new_id)], by.x = "from", by.y = "id", sort = FALSE)
  data <- merge(data, dict[, list(id, new_id)], by.x = "to", by.y = "id", suffixes = c("_from", "_to"), sort = FALSE)
  
  data <- data[, list(from = new_id_from, to = new_id_to, dist)]
  dict <- dict[, list(ref = new_ref, id = new_id)]
  
  graph_duplicate <- list(
    data = as.data.frame(data),
    coords = NULL,
    nbnodes = nrow(data),
    dict = as.data.frame(dict),
    attrib = graph$attrib
  )
  
  vertices <- copy(vertices)
  vertices[, vertex_id := paste0(prefix, vertex_id)]
  
  return(list(graph = graph_duplicate, vertices = vertices))
  
}