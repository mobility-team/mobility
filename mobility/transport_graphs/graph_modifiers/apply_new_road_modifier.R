apply_new_road_modifier <- function(
    cppr_graph,
    vertices,
    modifier_type,
    zones_geometry_file_path,
    max_speed,
    capacity,
    alpha,
    beta
) {
  
  if (is.na(zones_geometry_file_path)) {
    stop(paste0("No geometry file was provided, please provide a value for the zones_geometry_file_path argument."))
  }
  
  if (file.exists(zones_geometry_file_path) == FALSE) {
    stop(paste0("Road modifier file '", zones_geometry_file_path, "' does not exist."))
  }
  
  info(logger, "Applying new road modifier...")
  
  # cppr_graph_fp <- "D:/data/mobility/projects/grand-geneve/path_graph_bicycle/simplified/3b2ce3bbbd69a5a911eaf1bf367e581c-bicycle-simplified-path-graph"
  # hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
  # cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
  # vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))
  # zones_geometry_file_path <- "D:/data/mobility/projects/grand-geneve/cycleways.geojson"
  
  new_roads <- st_read(zones_geometry_file_path, quiet = TRUE)
  new_roads <- st_transform(new_roads, 3035)
  new_roads <- st_buffer(new_roads, 100.0)
  
  verts <- merge(
    vertices,
    as.data.table(cppr_graph$dict),
    by.x = "vertex_id",
    by.y = "ref"
  )
  
  verts_sf <- sfheaders::sf_point(verts, x = "x", y = "y", keep = TRUE)
  st_crs(verts_sf) <- 3035
  
  # Find which graph nodes are in the speed modifier 
  index <- unlist(st_intersects(new_roads, verts_sf))
  mod_vertex_ids <- verts_sf$id[index]
  
  data <- as.data.table(cppr_graph$data)
  data[, speed := cppr_graph$attrib$aux/dist]
  
  max_speed <- max_speed/3.6
  
  data[from %in% mod_vertex_ids & to %in% mod_vertex_ids, speed := pmin(max_speed, speed)]
  data[, dist := cppr_graph$attrib$aux/speed]
  
  cppr_graph$data <- data[, list(from, to, dist)]
  
  return(cppr_graph)
  
}