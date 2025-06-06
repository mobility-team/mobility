apply_road_lane_number_modifier <- function(
    cppr_graph,
    vertices,
    modifier_type,
    zones_geometry_file_path,
    lane_delta
) {
  
  if (is.na(zones_geometry_file_path)) {
    stop(paste0("No geometry file was provided, please provide a value for the zones_geometry_file_path argument."))
  }
  
  if (file.exists(zones_geometry_file_path) == FALSE) {
    stop(paste0("Road modifier file '", zones_geometry_file_path, "' does not exist."))
  }
  
  info(logger, "Applying road lane number delta modifier...")
  
  # cppr_graph_fp <- "D:/data/mobility/projects/grand-geneve/path_graph_car/simplified/49adfaa59c6f535100d201e618521ddd-car-simplified-path-graph"
  # hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
  # cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
  # vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))
  # zones_geometry_file_path <- "D:/data/mobility/projects/grand-geneve/patatoide_réduction_voies.geojson"
  
  modifier <- st_read(zones_geometry_file_path, quiet = TRUE)
  modifier <- st_transform(modifier, 3035)
  
  verts <- merge(
    vertices,
    as.data.table(cppr_graph$dict),
    by.x = "vertex_id",
    by.y = "ref"
  )
  
  verts_sf <- sfheaders::sf_point(verts, x = "x", y = "y", keep = TRUE)
  st_crs(verts_sf) <- 3035
  
  # Find which graph nodes are in the speed modifier 
  index <- unlist(st_intersects(modifier, verts_sf))
  mod_vertex_ids <- verts_sf$id[index]
  data <- as.data.table(cppr_graph$data)
  edge_index <- which(data$from %in% mod_vertex_ids & data$to %in% mod_vertex_ids)
  
  # Remove one lane to motorways
  cppr_graph$attrib$cap[edge_index] <- ifelse(
    cppr_graph$attrib$cap[edge_index] > 3999.0,
    cppr_graph$attrib$cap[edge_index] - 2000.0,
  )
  
  return(cppr_graph)
  
}