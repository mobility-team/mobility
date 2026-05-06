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
#   'd:\\data\\mobility\\projects\\grand-geneve\\c342828dcd0f0af0e9f15e00009ad911-transport_zones.gpkg',
#   'd:\\data\\mobility\\projects\\grand-geneve\\03748c70bbd44d8414efd32839aae124-highway-osm_data.osm',
#   'car',
#   '{"motorway": {"capacity": 2000.0, "alpha": 0.15, "beta": 4.0}, "trunk": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "primary": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "secondary": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "tertiary": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "unclassified": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "residential": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}, "living_street": {"capacity": 300.0, "alpha": 0.15, "beta": 4.0}, "motorway_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "trunk_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "primary_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "secondary_link": {"capacity": 1000.0, "alpha": 0.15, "beta": 4.0}, "tertiary_link": {"capacity": 600.0, "alpha": 0.15, "beta": 4.0}}',
#   'd:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\simplified\\70a9e44cdf0262a6dda7c578b604b298-car-simplified-path-graph'
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

source(file.path(package_path, "transport", "graphs", "core", "cpprouting_io.R"))
source(file.path(package_path, "transport", "graphs", "core", "project_od_nodes.R"))

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
  keep_cols <- c(
    "hgv", "hov", "access",
    "lanes:forward", "lanes:backward",
    "lanes:psv:forward", "lanes:psv:backward",
    "psv:lanes:forward", "psv:lanes:backward"
  )
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

# Parse a lane-count tag to numeric. This keeps the first numeric value when a
# tag contains separators such as ";" or "|" and returns NA when no number is
# present.
parse_lane_count <- function(x) {
  x <- as.character(x)
  x[is.na(x) | x == ""] <- NA_character_
  has_number <- grepl("^[[:space:]]*[0-9]+(?:\\.[0-9]+)?", x, perl = TRUE)
  x[has_number] <- sub(
    "^[[:space:]]*([0-9]+(?:\\.[0-9]+)?).*$",
    "\\1",
    x[has_number],
    perl = TRUE
  )
  x[!has_number] <- NA_character_
  return(as.numeric(x))
}

# Some OSM exports do not provide lanes:psv:* counts, but do provide
# psv:lanes:* strings such as "yes|yes|designated". Count the designated
# entries so we can subtract reserved public-service-vehicle lanes from the
# general-traffic capacity.
count_designated_psv_lanes <- function(x) {
  x <- as.character(x)
  x[is.na(x) | x == ""] <- NA_character_
  counts <- rep(NA_real_, length(x))
  valid <- !is.na(x)
  if (any(valid)) {
    matches <- gregexpr("(^|\\|)[[:space:]]*designated[[:space:]]*(\\||$)", x[valid], perl = TRUE)
    counts[valid] <- lengths(matches)
  }
  return(counts)
}

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
  edge_columns <- c(
    "edge_",
    ".vx0", ".vx1",
    "time_weighted", "d",
    "highway",
    "access",
    "lanes",
    "lanes:forward", "lanes:backward",
    "lanes:psv:forward", "lanes:psv:backward",
    "psv:lanes:forward", "psv:lanes:backward"
  )

  graph_dt <- as.data.table(graph)

  missing_edge_columns <- setdiff(edge_columns, colnames(graph_dt))
  for (col in missing_edge_columns) {
    graph_dt[, (col) := NA_character_]
  }
  
  edges <- graph_dt[, ..edge_columns]

  # Original dodgr edges keep the edge_ identifiers from osmdata_sc.
  # Reversed duplicates created by dodgr receive new edge_ hashes, so this flag
  # tells us whether the row follows the original OSM way direction.
  edges[, is_original_direction := edge_ %in% osm_data$object_link_edge$edge_]

  # Detect whether the edge has an opposite-direction companion. When only a
  # total `lanes` tag is available on a bidirectional road, we split that total
  # between the two directed edges instead of giving the full total to both.
  edges[, pair_v0 := pmin(.vx0, .vx1)]
  edges[, pair_v1 := pmax(.vx0, .vx1)]
  edges[, pair_n_edges := .N, by = .(pair_v0, pair_v1)]

  edges[, lanes_total := parse_lane_count(lanes)]
  edges[, lanes_forward := parse_lane_count(`lanes:forward`)]
  edges[, lanes_backward := parse_lane_count(`lanes:backward`)]

  edges[, psv_lanes_forward := parse_lane_count(`lanes:psv:forward`)]
  edges[, psv_lanes_backward := parse_lane_count(`lanes:psv:backward`)]

  missing_forward_psv <- is.na(edges$psv_lanes_forward)
  edges[missing_forward_psv, psv_lanes_forward := count_designated_psv_lanes(`psv:lanes:forward`[missing_forward_psv])]

  missing_backward_psv <- is.na(edges$psv_lanes_backward)
  edges[missing_backward_psv, psv_lanes_backward := count_designated_psv_lanes(`psv:lanes:backward`[missing_backward_psv])]

  edges[is.na(psv_lanes_forward), psv_lanes_forward := 0]
  edges[is.na(psv_lanes_backward), psv_lanes_backward := 0]

  # Infer one directional lane count from the opposite direction plus the total
  # lane count when possible.
  edges[
    is.na(lanes_forward) & !is.na(lanes_total) & !is.na(lanes_backward),
    lanes_forward := pmax(lanes_total - lanes_backward, 0)
  ]
  edges[
    is.na(lanes_backward) & !is.na(lanes_total) & !is.na(lanes_forward),
    lanes_backward := pmax(lanes_total - lanes_forward, 0)
  ]

  # When only one directional lane tag is available and the total lane count is
  # missing, reuse the known direction as a pragmatic fallback. This avoids
  # dropping capacity information on roads where OSM only tagged one side.
  edges[
    is.na(lanes_forward) & is.na(lanes_total) & !is.na(lanes_backward),
    lanes_forward := lanes_backward
  ]
  edges[
    is.na(lanes_backward) & is.na(lanes_total) & !is.na(lanes_forward),
    lanes_backward := lanes_forward
  ]

  # When only the total lane count is known on a bidirectional road, split it
  # between the two directed edges. On one-way roads, keep the full total on
  # the original direction.
  edges[
    is.na(lanes_forward) & is.na(lanes_backward) & !is.na(lanes_total) & pair_n_edges >= 2,
    `:=`(
      lanes_forward = ceiling(lanes_total / 2),
      lanes_backward = floor(lanes_total / 2)
    )
  ]
  edges[
    is.na(lanes_forward) & is.na(lanes_backward) & !is.na(lanes_total) & pair_n_edges == 1,
    `:=`(
      lanes_forward = lanes_total,
      lanes_backward = 0
    )
  ]

  edges[
    is.na(lanes_forward) & is.na(lanes_backward),
    `:=`(
      lanes_forward = 1,
      lanes_backward = 1
    )
  ]

  edges[
    is_original_direction == TRUE,
    car_lanes := pmax(lanes_forward - psv_lanes_forward, 0)
  ]
  edges[
    is_original_direction == FALSE,
    car_lanes := pmax(lanes_backward - psv_lanes_backward, 0)
  ]

  # Keep at least one usable car lane as a conservative fallback when OSM tags
  # are incomplete or inconsistent. This avoids turning a routable car edge
  # into a zero-capacity edge solely because the directional lane tags are
  # partial.
  edges[is.na(car_lanes) | car_lanes <= 0, car_lanes := 1]

  # Multiply capacities per lane in veh/h by the number of lane to get overall
  # edge capacity
  edges <- merge(edges, osm_capacity_parameters, by = "highway", sort = FALSE)
  edges[, capacity := capacity*car_lanes]

  edges[, c(
    "pair_v0", "pair_v1", "pair_n_edges",
    "lanes_total", "lanes_forward", "lanes_backward",
    "psv_lanes_forward", "psv_lanes_backward", "car_lanes"
  ) := NULL]
  
} else {
  
  edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted", "d")])
  
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
if (mode == "car") {
  cppr_graph <- makegraph(
    df = edges[, list(.vx0, .vx1, time_weighted)],
    directed = TRUE,
    aux = edges$d,
    capacity = edges$capacity,
    alpha = edges$alpha,
    beta = edges$beta
  )
} else {
  cppr_graph <- makegraph(
    df = edges[, list(.vx0, .vx1, time_weighted)],
    directed = TRUE,
    aux = edges$d
  )
}

cppr_graph[["attrib"]][["n_edges"]] <- rep(1.0, nrow(cppr_graph[["data"]]))

if (mode == "car") {
  direct_access_highways <- c(
    "primary",
    "secondary",
    "tertiary",
    "unclassified",
    "residential",
    "living_street",
    "service",
    "road"
  )

  direct_access <- edges$highway %in% direct_access_highways

  if ("access" %in% colnames(edges)) {
    direct_access <- direct_access & !(edges$access %in% c("private", "no"))
    direct_access[is.na(direct_access)] <- FALSE
  }

  cppr_graph[["attrib"]][["direct_access"]] <- as.numeric(direct_access)
}

# Simplify the graph and accumulate travel times along edges that can be 
# collapsed (avoid dropping nodes that are close to the buildings that were 
# selected in the transport zones creation process)
info(logger, "Simplifying graph...")

cppr_graph_simple <- simplify_cppr_graph(cppr_graph, mode = mode)


# Remove all dead ends
info(logger, "Removing dead ends...")

# Remove all nodes that have a degree that is less than 3
# (which means all nodes that are dead ends because the simplification step
# has already removed all non junction nodes)
edges <- as.data.table(cppr_graph_simple[["data"]])

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

data <- as.data.table(cppr_graph_simple[["data"]])
data[, keep_from := from %in% remaining_node_index]
data[, keep_to := to %in% remaining_node_index]

keep_index <- data[, keep_from & keep_to]

dict <- as.data.table(cppr_graph_simple[["dict"]])
dict <- dict[id %in% remaining_node_index]
dict[, new_index := 0:(.N-1)]

data <- merge(data, dict[, list(id, new_index)], by.x = "from", by.y = "id", sort = FALSE)
data <- merge(data, dict[, list(id, new_index)], by.x = "to", by.y = "id", sort = FALSE)
data <- data[, list(from = new_index.x, to = new_index.y, dist)]

dict <- dict[, list(ref, id = new_index)]

cppr_graph_simple[["attrib"]][["aux"]] <- cppr_graph_simple[["attrib"]][["aux"]][keep_index]
cppr_graph_simple[["attrib"]][["n_edges"]] <- cppr_graph_simple[["attrib"]][["n_edges"]][keep_index]
if ("direct_access" %in% names(cppr_graph_simple[["attrib"]])) {
  cppr_graph_simple[["attrib"]][["direct_access"]] <- cppr_graph_simple[["attrib"]][["direct_access"]][keep_index]
}
cppr_graph_simple[["attrib"]][["alpha"]] <- cppr_graph_simple[["attrib"]][["alpha"]][keep_index]
cppr_graph_simple[["attrib"]][["beta"]] <- cppr_graph_simple[["attrib"]][["beta"]][keep_index]
cppr_graph_simple[["attrib"]][["cap"]] <- cppr_graph_simple[["attrib"]][["cap"]][keep_index]

cppr_graph_simple[["data"]] <- data
cppr_graph_simple[["dict"]] <- dict
cppr_graph_simple[["nbnode"]] <- nrow(dict)

info(logger, "Simplifying pruned graph...")

cppr_graph_simple <- simplify_cppr_graph(cppr_graph_simple, mode = mode)

vertices_3035 <- vertices_3035[vertex_id %in% cppr_graph_simple[["dict"]][["ref"]]]

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))

info(logger, "Projecting OD points onto the final graph...")

projection_result <- insert_projected_od_nodes(
  cppr_graph_simple,
  vertices_3035,
  buildings_sample,
  mode
)


cppr_graph_simple <- projection_result$graph
vertices_3035 <- projection_result$vertices
od_vertex_map <- projection_result$od_vertex_map

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

write_parquet(
  od_vertex_map,
  file.path(
    dirname(folder_path),
    paste0(hash, "-od-vertex-map.parquet")
  )
)

file.create(output_file_path)

