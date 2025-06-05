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
#   'D:\\data\\mobility\\tests\\results\\path_graph_walk\\congested\\ec755f08e92f3a8b7694f3317a3767a7-walk-congested-path-graph',
#   'D:\\data\\mobility\\tests\\results\\path_graph_walk\\contracted\\256237bc2e369cc08469187b3891b2e6-walk-contracted-path-graph'
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
