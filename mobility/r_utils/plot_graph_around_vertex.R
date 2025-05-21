
plot_graph_around_vertex <- function(graph, verts, v_id, d = 400) {
  
  dict <- as.data.table(graph$dict)
  
  vertex_index <- dict[dict$ref == v_id, id]
  vertex_xy <- verts[vertex_id == v_id, list(x, y)]
  
  nearby_vertices <- verts[sqrt((x - vertex_xy$x)^2 + (y - vertex_xy$y)^2) < d]
  nearby_vertices_index <- dict[ref %in% nearby_vertices$vertex_id, id]
  
  shortcuts <- as.data.table(graph$shortcuts)
  setnames(shortcuts, c("from", "to", "shortc"))
  
  edges <- as.data.table(graph$data)
  edges <- edges[from %in% nearby_vertices_index | to %in% nearby_vertices_index]
  # edges <- edges[!shortcuts[, list(from, to)], on = list(from, to)]
  edges <- merge(edges, dict, by.x = "from", by.y = "id")
  edges <- merge(edges, dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
  edges <- merge(edges, verts, by.x = "ref_from", by.y = "vertex_id")
  edges <- merge(edges, verts, by.x = "ref_to", by.y = "vertex_id", suffixes = c("_from", "_to"))
  
  p <- ggplot(edges)
  p <- p + geom_point(data = nearby_vertices, aes(x = x, y = y))
  p <- p + geom_segment(data = edges, aes(x = x_from, y = y_from, xend = x_to, yend = y_to), arrow = arrow(length = unit(5, "pt")))
  p <- p + geom_point(data = vertex_xy, aes(x = x, y = y), color = "red")
  p <- p + coord_equal(xlim = c(vertex_xy$x -d/2, vertex_xy$x+d/2), ylim = c(vertex_xy$y -d/2, vertex_xy$y+d/2))
  p <- p + theme_void()
  p
  
}