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
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\congested\\6666b03c21c085dfa46927e0a578b0aa-car-congested-path-graph',
#   'D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\contracted\\d6c85871fc14b5946c8cde33ac021972-car-contracted-path-graph'
# )

package_fp <- args[1]
cppr_graph_fp <- args[2]
output_fp <- args[3]

source(file.path(package_fp, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

# Load the cpprouting graph
info(logger, "Loading simplified/modified/congested graph...")
hash <- strsplit(basename(cppr_graph_fp), "-")[[1]][1]
cppr_graph <- read_cppr_graph(dirname(cppr_graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(cppr_graph_fp)), paste0(hash, "-vertices.parquet")))

# Contract the graph and save it
info(logger, "Contracting graph...")
cppr_graph <- cpp_contract(cppr_graph)

info(logger, "Saving contracted graph...")
hash <- strsplit(basename(output_fp), "-")[[1]][1]
save_cppr_contracted_graph(cppr_graph, dirname(output_fp), hash)
write_parquet(vertices, file.path(dirname(dirname(output_fp)), paste0(hash, "-vertices.parquet")))

file.create(output_fp)
