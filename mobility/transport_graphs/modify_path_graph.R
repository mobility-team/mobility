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

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\simplified\\775c688bc00234ed3f9b04ec86bbd9d6-car-simplified-path-graph',
#   '[{"modifier_type": "border_crossing", "max_speed": 30.0, "time_penalty": 10.0001, "borders": ["D:\\\\data\\\\mobility\\\\data\\\\osm\\\\286ac9c64e6fa7cbdcef27183624db8e-osm_border.geojson",
#   "D:\\\\data\\\\mobility\\\\data\\\\osm\\\\5a2241d7b589441aaa0dd526ff9f6b76-osm_border.geojson"]}]',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\modified\\a4d8d9065e1217e8f5ef1ca48cca4789-car-modified-path-graph'
# )

package_fp <- args[1]
cppr_graph_fp <- args[2]
speed_modifiers <- args[3]
output_fp <- args[4]

source(file.path(package_fp, "r_utils", "cpprouting_io.R"))
source(file.path(package_fp, "transport_graphs", "graph_modifiers", "apply_limited_speed_zones_modifier.R"))
source(file.path(package_fp, "transport_graphs", "graph_modifiers", "apply_border_crossing_speed_modifier.R"))
source(file.path(package_fp, "transport_graphs", "graph_modifiers", "apply_road_lane_number_modifier.R"))
source(file.path(package_fp, "transport_graphs", "graph_modifiers", "apply_new_road_modifier.R"))

logger <- logger(appenders = console_appender())

speed_modifiers <- fromJSON(
  speed_modifiers,
  simplifyDataFrame = FALSE,
  simplifyVector = FALSE
)


# Load the cpprouting graph
hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))


# If speed modifiers are provided, update the speed of the links in the graph
modifiers <- list(
  border_crossing = apply_border_crossing_speed_modifier,
  limited_speed_zones = apply_limited_speed_zones_modifier,
  lane_number_modification = apply_road_lane_number_modifier,
  new_road = apply_new_road_modifier
)

for (sm in speed_modifiers) {
  cppr_graph <- do.call(
    what = modifiers[[sm[["modifier_type"]]]],
    args = c(list(cppr_graph), list(vertices), sm)
  )
}


# Save the graph
hash <- strsplit(basename(output_fp), "-")[[1]][1]
folder_path <- dirname(output_fp)
save_cppr_graph(cppr_graph, folder_path, hash)
write_parquet(vertices, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))
file.create(output_fp)

