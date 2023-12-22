library(dodgr)
library(osmdata)
library(sf)
library(log4r)
library(optparse)

logger <- logger()

option_list = list(
  make_option(c("-t", "--tz-file-path"), type = "character"),
  make_option(c("-n", "--osm-file-path"), type = "character"),
  make_option(c("-m", "--mode"), type = "character"),
  make_option(c("-o", "--output-file-path"), type = "character")
)

opt_parser = OptionParser(option_list = option_list)
opt = parse_args(opt_parser)

transport_zones <- st_read(opt[["tz-file-path"]], quiet = TRUE)
transport_zones <- st_transform(transport_zones, 4326)
bbox <- st_bbox(transport_zones)

info(logger, "Parsing OSM data...")

osm_data <- osmdata_sc(q = opq(bbox), doc = opt[["osm-file-path"]])

info(logger, "Building dodgr graph...")

graph <- weight_streetnet(
  osm_data,
  wt_profile = opt[["mode"]],
  turn_penalty = FALSE
)

info(logger, "Contracting dodgr graph...")

graph <- dodgr_contract_graph(graph)
graph <- graph[graph$component == 1, ]

info(logger, "Saving dodgr graph...")

saveRDS(graph, opt[["output-file-path"]])

