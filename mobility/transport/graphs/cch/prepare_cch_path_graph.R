library(log4r)
library(cppRoutingCCH)
library(arrow)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\modified\\a4d8d9065e1217e8f5ef1ca48cca4789-car-modified-path-graph',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\cch\\a4d8d9065e1217e8f5ef1ca48cca4789-car-cch-path-graph'
# )

package_fp <- args[1]
cppr_graph_fp <- args[2]
output_fp <- args[3]

source(file.path(package_fp, "transport", "graphs", "core", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

input_hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
output_hash <- strsplit(basename(output_fp), "-")[[1]][1]

info(logger, "Loading modified graph for CCH preparation...")
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), input_hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(input_hash, "-vertices.parquet")))
od_vertex_map_fp <- file.path(dirname(dirname(cppr_graph_fp)), paste0(input_hash, "-od-vertex-map.parquet"))
od_vertex_map <- NULL
if (file.exists(od_vertex_map_fp)) {
  od_vertex_map <- read_parquet(od_vertex_map_fp)
}

info(logger, "Preparing CCH topology...")
cch <- cpp_cch_prepare(cppr_graph)

info(logger, "Saving CCH topology...")
save_cppr_cch(cch, dirname(output_fp), output_hash)

info(logger, "Saving CCH graph vertices...")
write_parquet(vertices, file.path(dirname(dirname(output_fp)), paste0(output_hash, "-vertices.parquet")))

if (!is.null(od_vertex_map)) {
  info(logger, "Saving CCH graph OD vertex map...")
  write_parquet(od_vertex_map, file.path(dirname(dirname(output_fp)), paste0(output_hash, "-od-vertex-map.parquet")))
}

info(logger, "Creating CCH graph marker file...")
file.create(output_fp)
info(logger, "Finished saving CCH graph.")