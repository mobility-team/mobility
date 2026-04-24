library(arrow)
library(data.table)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
graph_fp <- args[2]
output_fp <- args[3]

source(file.path(package_path, "transport", "graphs", "core", "cpprouting_io.R"))

hash <- strsplit(basename(graph_fp), "-")[[1]][1]
graph <- read_cppr_graph(dirname(graph_fp), hash)

data <- as.data.table(graph$data)
attrib <- as.data.table(graph$attrib)
dict <- as.data.table(graph$dict)
setnames(dict, c("id", "ref"), c("dict_id", "vertex_id"))

edges <- cbind(data, attrib)
edges <- merge(edges, dict, by.x = "from", by.y = "dict_id", all.x = TRUE, sort = FALSE)
setnames(edges, "vertex_id", "from_vertex")
edges <- merge(edges, dict, by.x = "to", by.y = "dict_id", all.x = TRUE, sort = FALSE)
setnames(edges, "vertex_id", "to_vertex")

write_parquet(
  edges[, .(from_vertex, to_vertex, dist, real_time)],
  output_fp
)
