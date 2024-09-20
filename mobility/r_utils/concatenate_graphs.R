
concatenate_graphs <- function(graph_1, graph_2, graph_3) {
  
  info(
    logger,
    "Adding the public transport routes to the walk graph..."
  )
  
  data <- rbind(graph_3$data, graph_1$data, graph_2$data)
  dict <- rbind(graph_3$dict, graph_1$dict, graph_2$dict)
  
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
    graph_3$attrib$aux,
    graph_1$attrib$aux,
    graph_2$attrib$aux
  )
  
  return(graph)
  
}