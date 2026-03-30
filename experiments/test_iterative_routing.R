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
tz_fp <- "D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg"
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
from <- sample(graph$dict$ref, 10000)
to <- sample(graph$dict$ref, 10000)

time <- get_distance_pair(graph, from, to, algorithm = "NBA", constant = 1/(60/3.6))
distance <- get_distance_pair(graph, from, to, algorithm = "NBA", constant = 1/(60/3.6), aggregate_aux = TRUE)

real_costs <- data.table(
  from = from,
  to = to,
  time = time,
  distance = distance
)

approx_costs <- CJ(
  from = from,
  to = to
)

approx_costs <- merge(approx_costs, vertices, by.x = "from", by.y = "vertex_id")
approx_costs <- merge(approx_costs, vertices, by.x = "to", by.y = "vertex_id", suffixes = c("_from", "_to"))
approx_costs[, approx_distance := sqrt((x_to - x_from)^2 + (y_to - y_from)^2)]

approx_speed <- median(distance/time, na.rm = TRUE)

approx_costs[, approx_time := approx_distance/approx_speed]

approx_costs <- merge(approx_costs, real_costs, by = c("from", "to"), all.x = TRUE)

approx_costs[, k_distance := distance/approx_distance]
approx_costs[, k_time := time/approx_time]



# KNN interpolation
knn <- get.knnx(
  approx_costs[!is.na(time), list(x_from, y_from, x_to, y_to)],
  approx_costs[is.na(time), list(x_from, y_from, x_to, y_to)],
  k = 10
)

k_distance_nn <- apply(knn$nn.index, 2, function(i) {approx_costs[!is.na(time)][i, k_distance]})

weights <- 1/knn$nn.dist^2
weights <- weights/rowSums(weights)

k_distance_nn <- rowSums(k_distance_nn*weights)

approx_costs[is.na(time), nn_k_distance := k_distance_nn]
approx_costs[, nn_distance := approx_distance*nn_k_distance]


# Compare the results with the true values
from_test <- sample(approx_costs[is.na(time), from], 1000)
to_test <- sample(approx_costs[is.na(time), to], 1000)

time <- get_distance_pair(graph, from_test, to_test, algorithm = "NBA", constant = 1/(60/3.6))
distance <- get_distance_pair(graph, from_test, to_test, algorithm = "NBA", constant = 1/(60/3.6), aggregate_aux = TRUE)

test_costs <- data.table(
  from = from_test,
  to = to_test,
  test_time = time,
  test_distance = distance
)

test_costs <- merge(test_costs, approx_costs, by = c("from", "to"))



# p <- ggplot(approx_costs[from == "10008779677"])
# p <- p + geom_point(aes(x = x_to, y = y_to, color = krig_k_distance))
# p <- p + coord_equal()
# p
# 
# p <- ggplot(approx_costs[from == "10008779677"])
# p <- p + geom_point(aes(x = x_to, y = y_to, color = nn_k_distance))
# p <- p + coord_equal()
# p


# aon <- get_aon(graph, from, to, demand = rep(1.0, length(from)), algorithm = "bi", constant = 1/(60/3.6))




from <- sample(graph$dict$ref, 1000)
to <- sample(graph$dict$ref, 1000)

paths <- get_path_pair(graph, from, to, algorithm = "NBA", constant = 1/(60/3.6), long = TRUE)
paths <- as.data.table(paths)
paths[, previous_node := shift(node, 1, type = "lag"), by = list(from, to)]
paths[, flow := 1000.0, by = list(from, to)]




# Add the time take to travel each edge
times <- as.data.table(graph$data)
setnames(times, c("from", "to", "time"))

times <- merge(times, graph$dict, by.x = "from", by.y = "id")
times <- merge(times, graph$dict, by.x = "to", by.y = "id", suffixes = c("_from", "_to"))
times <- times[, list(previous_node = ref_from, node = ref_to, time)]

# Compute the cumulative time from the start of each trip
paths <- merge(paths, times, by = c("previous_node", "node"), sort = FALSE)
paths[, cum_time := cumsum(time), by = list(from, to)]

# Offset all times to make all trips arrive at the same time
paths[, cum_time := cum_time - max(cum_time), by = list(from, to)]
paths[, cum_time := cum_time - min(cum_time)]



# Assuming 'paths' is your data.table

# Assuming 'paths' is your data.table
paths[, `:=`(
  edge = paste(previous_node, node, sep = "_"),
  start_time = cum_time - time,
  end_time = cum_time
)]

# Create events for flow changes
events <- rbind(
  paths[, .(edge, time = start_time, flow_change = flow)],
  paths[, .(edge, time = end_time, flow_change = -flow)]
)

# Sort events by edge and time
setorder(events, edge, time)

# Compute cumulative flow over time at event times
events[, cum_flow := cumsum(flow_change), by = edge]


events[, next_time := shift(time, type = "lead"), by = edge]

delta_time <- 15*60

intervals <- data.table(
  start_time = seq(22500, 35000, delta_time),
  end_time = seq(22500 + delta_time, 35000 + delta_time, delta_time)
)

intervals$start_time_cp <- intervals$start_time
intervals$end_time_cp <- intervals$end_time

events_regular <- copy(events)
# events_regular <- events_regular[cum_flow > 0.0]

events_regular[, next_time_cp := next_time]
events_regular[, time_cp := time]

events_regular <- events_regular[intervals, on = .(next_time_cp >= start_time_cp, time_cp < end_time_cp), list(edge, start_time, end_time, time, next_time, cum_flow), nomatch = 0L]
events_regular[, interval_overlap := (pmin(end_time, next_time) - pmax(start_time, time))/delta_time]
events_regular <- events_regular[, list(cum_flow = sum(cum_flow*3600/delta_time*interval_overlap)), by = list(edge, start_time, end_time)]

events_regular_smooth <- copy(events_regular)


offsets <- seq(-1, 1, 1)
weights <- rep(1/length(offsets), length(offsets))
cols <- paste0("offset", offsets)

events_regular_smooth[, c(cols) := shift(cum_flow, n = offsets, fill = 0.0), by = edge]
events_regular_smooth[, cum_flow_smooth := rowSums(.SD[, cols, with = FALSE]*weights)]

edge_id <- sample(events$edge, 1)
edge_id <- "1697556012_318654302"
# edge_id <- "2889991122_198078891"
# edge_id <- "444561798_31364581"

p <- ggplot(events_regular_smooth[edge == edge_id])
p <- p + geom_col(data = events_regular[edge == edge_id], aes(x = start_time + delta_time/2, y = cum_flow))
p <- p + geom_line(aes(x = start_time + delta_time/2, y = cum_flow_smooth), col = "red")
p <- p + geom_point(aes(x = start_time + delta_time/2, y = cum_flow_smooth), size = 1, col = "red")
p

edges_max_flow <- events_regular_smooth[, list(max_flow = max(cum_flow_smooth)), by = edge]

edges_max_flow[, node := unlist(lapply(strsplit(edge, "_"), "[[", 1))]
edges_max_flow[, previous_node := unlist(lapply(strsplit(edge, "_"), "[[", 2))]


# edges_max_flow <- merge(edges_max_flow, vertices, by.x = "previous_node", by.y = "vertex_id")
# edges_max_flow <- merge(edges_max_flow, vertices, by.x = "node", by.y = "vertex_id", suffixes = c("_from", "_to"))



# p <- ggplot(edges_max_flow)
# p <- p + geom_segment(aes(x = x_from, y = y_from, xend = x_to, yend = y_to, linewidth = max_flow*3600/delta_time, color = max_flow*3600/delta_time), lineend = "round")
# p <- p + scale_linewidth(range = c(0, 6))
# p <- p + scale_color_gradientn(colors = viridis::magma(9))
# p <- p + coord_equal()
# p <- p + theme_void()
# p



edges_max_flow <- merge(edges_max_flow, graph$dict, by.x = "node", by.y = "ref")
edges_max_flow <- merge(edges_max_flow, graph$dict, by.x = "previous_node", by.y = "ref", suffixes = c("_from", "_to"))

edges_max_flow <- merge(as.data.table(graph$data), edges_max_flow[, c("id_from", "id_to", "max_flow")], by.x = c("from", "to"), by.y = c("id_from", "id_to"), all.x = TRUE, sort = FALSE)

edges_max_flow[is.na(max_flow), max_flow := 0.0]

edges_max_flow[, k_speed := 1/(1 + graph$attrib$alpha*(max_flow/graph$attrib$cap)^graph$attrib$beta)]
edges_max_flow[, dist := dist/k_speed]





  
library(gganimate)


p <- ggplot(flows[end_time %in% -sort(unique(-end_time))[1:6]])
p <- p + geom_segment(aes(x = x_from, y = y_from, xend = x_to, yend = y_to, linewidth = flow, color = flow), lineend = "round")
p <- p + scale_linewidth(range = c(0, 6))
p <- p + scale_color_gradientn(colors = viridis::magma(9))
p <- p + coord_equal()
p <- p + theme_void()
p <- p + facet_wrap(~end_time)
p
# p <- p + transition_time(start_time)

anim_save("C:/Users/pouchaif/Desktop/flows.gif", animation = p, width = 1280, height = 1280)


library(magick)

base <- image_read("C:/Users/pouchaif/Desktop/map.png")
animated_layer <- image_read("C:/Users/pouchaif/Desktop/flows.gif")
final_animation <- image_composite(base, animated_layer, operator = "atop")
image_write(final_animation, "C:/Users/pouchaif/Desktop/map_flows.gif")



x <- paths[from == "7008398525"]
x <- merge(x, vertices, by.x = "previous_node", by.y = "vertex_id")
x <- merge(x, vertices, by.x = "node", by.y = "vertex_id", suffixes = c("_from", "_to"))

p <- ggplot(x)
p <- p + geom_point(aes(x = x_from, y = y_from))
p <- p + transition_time(cum_time_bin)
p

