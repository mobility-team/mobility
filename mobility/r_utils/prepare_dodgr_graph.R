library(dodgr)
library(osmdata)
library(log4r)
library(sf)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(data.table)

args <- commandArgs(trailingOnly = TRUE)

tz_file_path <- args[1]
osm_file_path <- args[2]
mode <- args[3]
output_file_path <- args[4]

#tz_file_path <- "D:/data/mobility/projects/grand-geneve/fa3d29c9577e794ad6ca289986a94368-transport_zones.gpkg"
#osm_file_path <- "D:/data/mobility/projects/grand-geneve/5b923b0264a3c52233227fec7998fb7e-osm_data.osm"
#mode <- "motorcar"
#output_file_path <- "D:/data/mobility/projects/grand-geneve/cppr_car"

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path, quiet = TRUE)
transport_zones <- st_transform(transport_zones, 4326)
bbox <- st_bbox(transport_zones)

info(logger, "Parsing OSM data...")

osm_data <- osmdata_sc(q = opq(bbox), doc = osm_file_path)

info(logger, "Weighting network...")

dodgr_cache_off()

graph <- weight_streetnet(
  osm_data,
  wt_profile = mode,
  turn_penalty = FALSE
)

graph <- graph[graph$component == 1, ]


info(logger, "Converting to cppRouting graph...")

edges <- as.data.table(graph[, c(".vx0", ".vx1", "time_weighted")])

vertices <- as.data.table(graph[, c(".vx0", ".vx0_x", ".vx0_y", ".vx1", ".vx1_x", ".vx1_y")])
vertices <- rbind(
  vertices[, list(vertex_id = .vx0, x = .vx0_x, y = .vx0_y)][!duplicated(vertex_id)],
  vertices[, list(vertex_id = .vx1, x = .vx1_x, y = .vx1_y)][!duplicated(vertex_id)]
)
vertices <- vertices[!duplicated(vertex_id)]

cppr_graph <- makegraph(
  df = edges,
  directed = TRUE,
  coords = vertices,
  aux = graph$d
)

cppr_graph_contr <- cpp_contract(cppr_graph)
cppr_graph_contr$coords <- cppr_graph$coords

info(logger, "Saving cppRouting graph...")


duckdb_df_to_parquet <- function(df, con, path) {
  
  duckdb_register(con, "df", df)
  
  dbExecute(
    con,
    sprintf(
      "COPY df TO '%s' (FORMAT 'parquet')",
      path
    )
  )
  
  duckdb_unregister(con, "df")
  
}

duckdb_vector_to_parquet <- function(v, con, path) {
  
  df <- as.data.frame(v)
  
  duckdb_register(con, "df", df)
  
  dbExecute(
    con,
    sprintf(
      "COPY df TO '%s' (FORMAT 'parquet')",
      path
    )
  )
  
  duckdb_unregister(con, "df")
  
}


save_cpprouting_contracted_graph <- function(graph, path) {
  
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  duckdb_df_to_parquet(graph$data, con, file.path(path, "data.parquet"))
  duckdb_df_to_parquet(graph$coords, con, file.path(path, "coords.parquet"))
  duckdb_vector_to_parquet(graph$rank, con, file.path(path, "rank.parquet"))
  
  duckdb_df_to_parquet(graph$shortcuts, con, file.path(path, "shortcuts.parquet"))
  duckdb_df_to_parquet(graph$dict, con, file.path(path, "dict.parquet"))
  
  duckdb_df_to_parquet(graph$original$data, con, file.path(path, "original_data.parquet"))
  duckdb_vector_to_parquet(graph$original$attrib$aux, con, file.path(path, "original_data_attrib_aux.parquet"))
  
  dbDisconnect(con, shutdown = TRUE)
  
  write_json(graph$nbnode, file.path(path, "nbnode.json"))
  
}

save_cpprouting_contracted_graph(cppr_graph_contr, output_file_path)

