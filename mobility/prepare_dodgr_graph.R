library(dodgr)
library(osmdata)
library(log4r)
library(sf)

args <- commandArgs(trailingOnly = TRUE)

tz_file_path <- args[1]
osm_file_path <- args[2]
mode <- args[3]
output_file_path <- args[4]

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_file_path, quiet = TRUE)
transport_zones <- st_transform(transport_zones, 4326)
bbox <- st_bbox(transport_zones)

info(logger, "Parsing OSM data, this might also take a while...")

osm_data <- osmdata_sc(q = opq(bbox), doc = osm_file_path)

info(logger, "Building dodgr graph...")

graph <- weight_streetnet(
  osm_data,
  wt_profile = mode,
  turn_penalty = FALSE
)

info(logger, "Contracting dodgr graph...")

graph <- dodgr_contract_graph(graph)
graph <- graph[graph$component == 1, ]

info(logger, "Saving dodgr graph...")

saveRDS(graph, output_file_path)

