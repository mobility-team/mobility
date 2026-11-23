# Paths contain node IDs, not edge IDs. If several edges connect the same
# nodes, routing takes a minimum perceived-cost edge, not all of them.
select_shortest_path_edges <- function(edges) {
  # Keep the first edge when equally costly alternatives cannot be distinguished
  # from the node sequence returned by the router.
  data.table::setorderv(edges, c("from", "to", "perceived_time"))
  edges[!duplicated(edges, by = c("from", "to"))]
}
