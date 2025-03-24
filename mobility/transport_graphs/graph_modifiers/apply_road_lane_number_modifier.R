apply_road_lane_number_modifier <- function(
    cppr_graph,
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
  index <- unlist(st_intersects(speed_modifiers, verts_sf))
  mod_vertex_ids <- verts_sf$id[index]
  
  data <- as.data.table(cppr_graph$data)
  data[, speed := cppr_graph$attrib$aux/dist]
  
  max_speed <- max_speed/3.6
  
  data[, speed := pmin(max_speed, speed)]
  data[, dist := cppr_graph$attrib$aux/speed]
  
  cppr_graph$data <- data[, list(from, to, dist)]
  
  return(cppr_graph)
  
}