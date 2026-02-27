library(dodgr)
library(osmdata)
library(log4r)
library(sf)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)
library(arrow)
library(FNN)
library(dplyr)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility\\mobility',
#   'd:\\data\\mobility\\projects\\dolancourt\\b8f2f9296a57c29bb6aa2d11e02994c7-transport_zones.gpkg',
#   'd:\\data\\mobility\\projects\\dolancourt\\dbdb0e14874405c398e1c237890c12ab-highway-osm_data.osm',
#   'car',
#   '{"motorway": {"capacity": 2000.0, "alpha": 0.15, "beta": 4.0}, "trunk": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "primary": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "secondary": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "tertiary": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "unclassified": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "residential": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "living_street": {"capacity": 300.0, "alpha": 0.15, "beta": 4.0}, "motorway_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "trunk_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "primary_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "secondary_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "tertiary_link": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}}',
#   'd:\\data\\mobility\\projects\\dolancourt\\path_graph_car\\simplified\\957d77e3fac8b10fa874c88552c68145-car-simplified-path-graph'
# )

package_path <- args[1]
tz_fp <- args[2]
osm_file_path <- args[3]
mode <- args[4]
osm_capacity_parameters <- args[5]
output_file_path <- args[6]

buildings_sample_fp <- file.path(
  dirname(tz_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_fp)),
    "-transport_zones_buildings.parquet"
  )
)

osm_capacity_parameters <- fromJSON(osm_capacity_parameters)
osm_capacity_parameters <- cbind(
  data.table(highway = names(osm_capacity_parameters)),
  rbindlist(osm_capacity_parameters)
)

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_fp, quiet = TRUE)
transport_zones <- st_transform(transport_zones, 4326)
bbox <- st_bbox(transport_zones)

info(logger, "Parsing OSM data...")

osm_data <- osmdata_sc(q = opq(bbox), doc = osm_file_path)

# Avoid dodgr 0.4.3 regression (does not handle all oneway tags)
# Map all existing tags to "yes or "no", with a oneway="no" fallback
osm_data$object <- osm_data$object %>%
  mutate(
    oneway = if_else(key == "oneway", value, NA_character_)
  ) %>%
  mutate(
    oneway = case_when(
        oneway %in% c("yes") ~ "yes",
        oneway %in% c("no","-1", "alternating", "reversible") ~ "no",
        TRUE ~ "no"
     )
  ) %>%
  mutate(
    value = if_else(key == "oneway", oneway, value)
  ) %>%
  select(
    -oneway
  )

# Some ways have no oneway tag and must also be set to oneway="no" so they are not NA when dodgr parses them
all_ids <- unique(osm_data$object$object_)
oneway_ids <- osm_data$object %>%
  filter(key == "oneway") %>% 
  pull(object_)

no_oneway_ids <- setdiff(all_ids, oneway_ids)

new_rows <- tibble(
  object_ = no_oneway_ids,
  key = "oneway",
  value = "no"
)

osm_data$object <- bind_rows(osm_data$object, new_rows)


info(logger, "Weighting network with dodgr...")

dodgr_cache_off()

# Map mobility modes to dodgr modes
modes <- list(
  walk = "foot",
  car = "motorcar",
  bicycle = "bicycle"
)

dodgr_mode <- modes[[mode]]

# Restrict OSM ways to the ones specified in the OSMCapacityParameters object
wt_profiles <- dodgr::weighting_profiles
df <- wt_profiles$weighting_profiles
df <- df[df$way %in% osm_capacity_parameters$highway, ]
wt_profiles$weighting_profiles <- df
hash <- strsplit(basename(output_file_path), "-")[[1]][1]
wt_fp <- paste0(dirname(output_file_path), "/", paste0(hash, "-dodgr-wt_profile.json"))
write(toJSON(wt_profiles), wt_fp)


if (mode == "car") {
  keep_cols <- c("hgv", "hov", "access")
} else {
  keep_cols <- NULL
}


graph <- weight_streetnet(
  osm_data,
  wt_profile = dodgr_mode,
  wt_profile_file = wt_fp,
  turn_penalty = FALSE,
  keep_cols = keep_cols
)

graph <- graph[graph$component == 1, ]

# Remove Heavy Goods Vehicles only and ridesharing only ways
if (mode == "car") {
  if ("hgv" %in% colnames(graph)) {
    graph <- graph[graph$hgv != "designated" | is.na(graph$hgv), ]
  }
  if ("hov" %in% colnames(graph)) {
    graph <- graph[graph$hov != "designated" | is.na(graph$hov), ]
  }
  if ("access" %in% colnames(graph)) {
    graph <- graph[!(graph$access %in% c("private", "no")) | is.na(graph$access), ]
  }
}

info(logger, "Extracting edges and nodes..")

# Compute road capacity
if (mode == "car") {
  
  edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted", "highway", "lanes")])
  edges <- merge(edges, osm_capacity_parameters, by = "highway", sort = FALSE)
  edges[, lanes := as.numeric(lanes)]
  edges[is.na(lanes) | lanes == 0, lanes := 1]
  edges[, capacity := capacity*lanes]
  
} else {
  
  edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted")])
  
}

vertices <- as.data.table(graph[, c(".vx0", ".vx0_x", ".vx0_y", ".vx1", ".vx1_x", ".vx1_y")])
vertices <- rbind(
  vertices[, list(vertex_id = .vx0, x = .vx0_x, y = .vx0_y)][!duplicated(vertex_id)],
  vertices[, list(vertex_id = .vx1, x = .vx1_x, y = .vx1_y)][!duplicated(vertex_id)]
)
vertices <- vertices[!duplicated(vertex_id)]


# Locate transport zones buildings on the graph
vertices_3035 <- sfheaders::sf_point(vertices, x = "x", y = "y", keep = TRUE)
st_crs(vertices_3035) <- 4326
vertices_3035 <- st_transform(vertices_3035, 3035)
vertices_3035 <- as.data.table(cbind(st_drop_geometry(vertices_3035), st_coordinates(vertices_3035)))
setnames(vertices_3035, c("vertex_id", "x", "y"))


# Create the cppRouting graph
cppr_graph <- makegraph(
  df = edges[, list(.vx0, .vx1, time_weighted)],
  directed = TRUE,
  aux = graph$d,
  capacity = edges$capacity,
  alpha = edges$alpha,
  beta = edges$beta
)

cppr_graph$attrib$n_edges <- 1.0

# Simplify the graph and accumulate travel times along edges that can be 
# collapsed (avoid dropping nodes that are close to the buildings that were 
# selected in the transport zones creation process)
info(logger, "Simplifying graph...")

cppr_graph_simple <- cpp_simplify(
  cppr_graph,
  rm_loop = TRUE,
  iterate = TRUE,
  # keep = vertices_to_keep
)

# Replace edges times costs by distance, capacity, alpha, beta to be able to 
# aggregate these when simplifying the network
aggregate_aux_data <- function(cppr_graph, dist_var) {
  
  cppr_graph$data$dist <- cppr_graph$attrib[[dist_var]]
  
  cppr_graph_s_dist <- cpp_simplify(
    cppr_graph,
    rm_loop = TRUE,
    iterate = TRUE,
    # keep = vertices_to_keep
  )
  
  return(cppr_graph_s_dist$data$dist)
  
}


dist <- aggregate_aux_data(cppr_graph, "aux")
cppr_graph_simple$attrib$aux <- dist

if (mode == "car") {
  
  n_edges <- aggregate_aux_data(cppr_graph, "n_edges")
  cap <- aggregate_aux_data(cppr_graph, "cap")
  alpha <- aggregate_aux_data(cppr_graph, "alpha")
  beta <- aggregate_aux_data(cppr_graph, "beta")
  
  cppr_graph_simple$attrib$cap <- cap/n_edges
  cppr_graph_simple$attrib$alpha <- alpha/n_edges
  cppr_graph_simple$attrib$beta <- beta/n_edges
  
}


# Remove all dead ends
info(logger, "Removing dead ends...")

# Remove all nodes that have a degree that is less than 3
# (which means all nodes that are dead ends because the simplification step
# has already removed all non junction nodes)
edges <- as.data.table(cppr_graph_simple$data)

nodes <- c(0)

while(length(nodes) > 0) {

  n_incoming <- edges[, list(n_incoming = .N), by = list(index = to)]
  n_outgoing <- edges[, list(n_outgoing = .N), by = list(index = from)]
  # degree <- merge(n_incoming, n_outgoing, by = "index")
  degree <- merge(n_incoming, n_outgoing, by = "index", all = TRUE)
  degree[is.na(n_incoming), n_incoming := 0]
  degree[is.na(n_outgoing), n_outgoing := 0]
  degree[, deg := n_incoming + n_outgoing]

  one_way_dead_end_nodes <- degree[deg == 1, index]

  two_way_nodes <- degree[deg == 2, list(index)]
  two_way_nodes <- merge(two_way_nodes, edges[, list(from, to)], by.x = "index", by.y = "from")
  two_way_nodes <- merge(two_way_nodes, edges[, list(from, to)], by.x = "index", by.y = "to")
  two_way_dead_end_nodes <- two_way_nodes[from == to, index]

  nodes <- unique(c(one_way_dead_end_nodes, two_way_dead_end_nodes))

  edges <- edges[!(from %in% nodes | to %in% nodes)]

}

# Filter and recreate the cpprouting graph
remaining_node_index <- unique(c(edges$from, edges$to))

data <- as.data.table(cppr_graph_simple$data)
data[, keep_from := from %in% remaining_node_index]
data[, keep_to := to %in% remaining_node_index]

keep_index <- data[, keep_from & keep_to]

dict <- as.data.table(cppr_graph_simple$dict)
dict <- dict[id %in% remaining_node_index]
dict[, new_index := 0:(.N-1)]

data <- merge(data, dict[, list(id, new_index)], by.x = "from", by.y = "id", sort = FALSE)
data <- merge(data, dict[, list(id, new_index)], by.x = "to", by.y = "id", sort = FALSE)
data <- data[, list(from = new_index.x, to = new_index.y, dist)]

dict <- dict[, list(ref, id = new_index)]

cppr_graph_simple$attrib$aux <- cppr_graph_simple$attrib$aux[keep_index]
cppr_graph_simple$attrib$alpha <- cppr_graph_simple$attrib$alpha[keep_index]
cppr_graph_simple$attrib$beta <- cppr_graph_simple$attrib$beta[keep_index]
cppr_graph_simple$attrib$cap <- cppr_graph_simple$attrib$cap[keep_index]

cppr_graph_simple$data <- data
cppr_graph_simple$dict <- dict
cppr_graph_simple$nbnode <- nrow(dict)



vertices_3035 <- vertices_3035[vertex_id %in% cppr_graph_simple$dict$ref]

info(logger, "Saving cppRouting graph and vertices coordinates...")

hash <- strsplit(basename(output_file_path), "-")[[1]][1]
folder_path <- dirname(output_file_path)

save_cppr_graph(cppr_graph_simple, folder_path, hash)

write_parquet(
  vertices_3035,
  file.path(
    dirname(folder_path),
    paste0(hash, "-vertices.parquet")
  )
)

file.create(output_file_path)

