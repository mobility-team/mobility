library(sf)
library(gtfsrouter)
library(log4r)
library(data.table)
library(arrow)
library(lubridate)
library(future.apply)
library(lubridate)
library(FNN)
library(cppRouting)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility_oss\\mobility',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\congested\\7a0adf16b00e26401a5d8a12e38378ac-car-congested-path-graph',
#   '["31396125", "9516471670", "8517853218", "1960409440", "1935994823", "1960409440", "1935994823", "1443371808", "25647142"]',
#   '["283850710", "25648101", "25648101", "780131421", "780131421", "25647362", "25647362", "6894166742", "7714311793"]',
#   'D:\\data\\mobility\\projects\\grand-geneve\\path_graph_car\\congested\\path_pairs.parquet'
# )

package_path <- args[1]
graph_fp <- args[2]
from <- args[3]
to <- args[4]
output_fp <- args[5]

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

from <- fromJSON(from)
to <- fromJSON(to)

hash <- strsplit(basename(graph_fp), "-")[[1]][1]
graph <- read_cppr_graph(dirname(graph_fp), hash)

paths <- get_path_pair(
  graph,
  from,
  to,
  long = TRUE
)

write_parquet(paths, output_fp)
