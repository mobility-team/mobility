library(plumber)
library(dodgr)
library(log4r)
library(sfheaders)
library(nngeo)
library(data.table)
library(reshape2)
library(arrow)
library(cppRouting)
library(DBI)
library(duckdb)
library(jsonlite)
library(FNN)
library(fields)
library(ggplot2)
library(FNN)
library(xgboost)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_fp <- args[2]
graph_fp <- args[3]
max_speed <- as.numeric(args[4])
max_time <- as.numeric(args[5])
output_fp <- args[6]

package_path <- "D:/dev/mobility_oss/mobility"
tz_fp <- "D:\\data\\mobility\\projects\\study_area\\d2b01e6ba3afa070549c111f1012c92d-transport_zones.gpkg"
max_speed <- 80.0
max_time <- 1.0
graph_fp <- "D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\simplified\\9a6f4500ffbf148bfe6aa215a322e045-done"

buildings_sample_fp <- file.path(
  dirname(tz_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_fp)),
    "-transport_zones_buildings.parquet"
  )
)

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

logger <- logger(appenders = console_appender())

transport_zones <- st_read(tz_fp)
transport_zones <- as.data.table(st_drop_geometry(transport_zones))

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]

# Load cpprouting graph
hash <- strsplit(basename(graph_fp), "-")[[1]][1]
graph <- read_cppr_graph(dirname(graph_fp), hash)
vertices <- read_parquet(file.path(dirname(dirname(graph_fp)), paste0(hash, "-vertices.parquet")))
graph$coords <- vertices

set.seed(0)

# Sample 1000 origins and 1000 destinations
# from <- sample(graph$dict$ref, 10000)
# to <- sample(graph$dict$ref, 10000)




plumber <- pr(file.path(package_path, "r_utils", "plumber_functions.R"))

pr_run(plumber)
