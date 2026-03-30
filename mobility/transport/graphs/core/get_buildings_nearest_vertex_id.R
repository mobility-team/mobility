library(FNN)

get_buildings_nearest_vertex_id <- function(buildings, vertices) {
  knn <- get.knnx(
    vertices[, list(x, y)],
    buildings[, list(x, y)],
    k = 1
  )
  return(vertices$vertex_id[knn$nn.index])
}