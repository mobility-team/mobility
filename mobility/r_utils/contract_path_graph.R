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

package_path <- args[1]
cppr_graph_path <- args[2]
output_file_path <- args[3]

# package_path <- "D:/dev/mobility_oss/mobility"
# cppr_graph_path <- "D:/data/mobility/projects/study_area/path_graph_walk/simplified/0fc1301e47e4f0bc1c161ab1d1097fb5-done"
# output_file_path <- "D:/data/mobility/projects/study_area/path_graph_walk/contracted/0fc1301e47e4f0bc1c161ab1d1097fb5-done"

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

cppr_graph <- read_cppr_graph(cppr_graph_path)
cppr_graph <- cpp_contract(cppr_graph)

save_cppr_contracted_graph(cppr_graph, dirname(output_file_path))

file.create(output_file_path)
