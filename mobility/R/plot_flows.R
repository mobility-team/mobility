library(log4r)
library(arrow)
library(ggplot2)
library(data.table)
library(sf)
library(patchwork)

args <- commandArgs(trailingOnly = TRUE)

args <- c(
  "D:/data/mobility/projects/haut-doubs/a25b56abc681fdfbf95b35a21c4b59db-transport_zones.gpkg",
  "D:/data/mobility/projects/haut-doubs/dff33ecdc690f616a2538b008ee2f15f-work_od_flows.parquet",
  "D:/data/mobility/projects/haut-doubs/home_work_flows.svg"
)

tz_file_path <- args[1]
flows_file_path <- args[2]
output_file_path <- args[3]

transport_zones <- st_read(tz_file_path, quiet = TRUE)

transport_zones_centroids <- as.data.table(st_coordinates(st_centroid(transport_zones)))
transport_zones_centroids$transport_zone_id <- transport_zones$transport_zone_id

study_zone_outline <- st_union(transport_zones)
study_zone_outline <- st_buffer(st_buffer(study_zone_outline, 500), -500)

ch_outline <- st_union(transport_zones[substr(transport_zones$local_admin_unit_id, 1, 2) == "ch", ])
ch_outline <- st_buffer(st_buffer(ch_outline, 500), -500)

flows <- as.data.table(read_parquet(flows_file_path))
# flows <- flows[flow_volume > 10]

flows <- merge(flows, transport_zones_centroids, by.x = "from", by.y = "transport_zone_id")
flows <- merge(flows, transport_zones_centroids, by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))

# p <- ggplot()
# p <- p + geom_sf(data = transport_zones, fill = NA, linewidth = 0.25, color = "#f5f6fa")
# p <- p + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
# p <- p + geom_segment(data = flows[from != to], aes(x = X_from, y = Y_from, xend = X_to, yend = Y_to, size = flow_volume), alpha = 0.075, col = "#2f4b7c", lineend = "round")
# p <- p + geom_segment(data = flows[from == to], aes(x = X_from, y = Y_from, xend = X_to, yend = Y_to, size = flow_volume), alpha = 0.3, col = "#2f4b7c", lineend = "round")
# p <- p + scale_size(range = c(0, 10), limits = c(0, max(flows$flow_volume)))
# p <- p + theme_void()

# ggsave(plot = p, filename = output_file_path, width = 8, height = 8)


flows[order(-abs(flow_volume - ref_flow_volume))][1:20]

flows[, distance := sqrt((X_from - X_to)^2 + (Y_to - Y_from)^2)]

flows[local_admin_unit_id_from == "fr-25056"][order(-flow_volume)][1:10]
flows[local_admin_unit_id_from == "fr-25056"][order(-ref_flow_volume)][1:10]

flows[local_admin_unit_id_from == "fr-25462"][order(-flow_volume)][1:10]
flows[local_admin_unit_id_from == "fr-25462"][order(-ref_flow_volume)][1:10]

flows[local_admin_unit_id_from == "ch-5586"][order(-flow_volume)][1:10]
flows[local_admin_unit_id_from == "ch-5586"][order(-ref_flow_volume)][1:10]

flows[local_admin_unit_id_from == "fr-25056", sum(flow_volume*distance/1000)/sum(flow_volume)]

x <- flows[local_admin_unit_id_from == "fr-25056"]
plot(log(x$ref_flow_volume, base = 2), log(x$flow_volume, base = 2))
abline(0, 1, col = "red")
abline(-1, 1, col = "red")
abline(1, 1, col = "red")

x <- flows[local_admin_unit_id_from == "ch-5586"]
plot(log(x$ref_flow_volume, base = 2), log(x$flow_volume, base = 2))
abline(0, 1, col = "red")
abline(-1, 1, col = "red")
abline(1, 1, col = "red")

plot(log(flows$ref_flow_volume, base = 2), log(flows$flow_volume, base = 2))
abline(0, 1, col = "red")
abline(-1, 1, col = "red")
abline(1, 1, col = "red")

# p <- ggplot(flows[flow_volume > 10])
# p <- p + geom_point(aes(x = log(ref_flow_volume), y = log(flow_volume), color = substr(local_admin_unit_id_from, 1, 2) == "ch"))
# p <- p + coord_equal()
# p

p1 <- ggplot()
p1 <- p1 + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p1 <- p1 + geom_sf(data = ch_outline, fill = NA, linewidth = 0.1, color = "red")
p1 <- p1 + geom_point(data = flows[local_admin_unit_id_from  == "fr-25056" & flow_volume > 10.0], aes(x = X_to, y = Y_to, size = flow_volume), alpha = 0.5, col = "#2f4b7c")
p1 <- p1 + scale_size_area(max_size = 20)
p1 <- p1 + theme_void()

p2 <- ggplot()
p2 <- p2 + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p2 <- p2 + geom_sf(data = ch_outline, fill = NA, linewidth = 0.1, color = "red")
p2 <- p2 + geom_point(data = flows[local_admin_unit_id_from  == "fr-25056" & flow_volume > 10.0], aes(x = X_to, y = Y_to, size = ref_flow_volume), alpha = 0.5, col = "#2f4b7c")
p2 <- p2 + scale_size_area(max_size = 20)
p2 <- p2 + theme_void()

p1 + p2


p1 <- ggplot()
p1 <- p1 + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p1 <- p1 + geom_sf(data = ch_outline, fill = NA, linewidth = 0.1, color = "red")
p1 <- p1 + geom_point(data = flows[local_admin_unit_id_from  == "fr-25462" & flow_volume > 10.0], aes(x = X_to, y = Y_to, size = flow_volume), alpha = 0.5, col = "#2f4b7c")
p1 <- p1 + scale_size_area(max_size = 20)
p1 <- p1 + theme_void()

p2 <- ggplot()
p2 <- p2 + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p2 <- p2 + geom_sf(data = ch_outline, fill = NA, linewidth = 0.1, color = "red")
p2 <- p2 + geom_point(data = flows[local_admin_unit_id_from  == "fr-25462" & flow_volume > 10.0], aes(x = X_to, y = Y_to, size = ref_flow_volume), alpha = 0.5, col = "#2f4b7c")
p2 <- p2 + scale_size_area(max_size = 20)
p2 <- p2 + theme_void()

p1 + p2



p1 <- ggplot()
p1 <- p1 + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p1 <- p1 + geom_sf(data = ch_outline, fill = NA, linewidth = 0.1, color = "red")
p1 <- p1 + geom_point(data = flows[local_admin_unit_id_from  == "ch-5586" & flow_volume > 10.0], aes(x = X_to, y = Y_to, size = flow_volume), alpha = 0.5, col = "#2f4b7c")
p1 <- p1 + scale_size_area(max_size = 20)
p1 <- p1 + theme_void()

p2 <- ggplot()
p2 <- p2 + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p2 <- p2 + geom_sf(data = ch_outline, fill = NA, linewidth = 0.1, color = "red")
p2 <- p2 + geom_point(data = flows[local_admin_unit_id_from  == "ch-5586" & flow_volume > 10.0], aes(x = X_to, y = Y_to, size = ref_flow_volume), alpha = 0.5, col = "#2f4b7c")
p2 <- p2 + scale_size_area(max_size = 20)
p2 <- p2 + theme_void()

p1 + p2


jobs <- read_parquet("D:/data/mobility/projects/haut-doubs/jobs.parquet")
act <- read_parquet("D:/data/mobility/projects/haut-doubs/active_population.parquet")

flows[substr(local_admin_unit_id_to, 1, 2) == "ch" & substr(local_admin_unit_id_from, 1, 2) == "fr"][order(-flow_volume)]

jobs <- merge(transport_zones_centroids, jobs, by = "transport_zone_id", all.x = TRUE)
act <- merge(transport_zones_centroids, act, by = "transport_zone_id", all.x = TRUE)

p1 <- ggplot()
p1 <- p1 + geom_point(data = jobs, aes(x = X, y = Y, size = sink_volume), alpha = 0.5)
p1 <- p1 + scale_size_area(, max_size = 10)
p1 <- p1 + theme_void()
p1 <- p1 + coord_equal()

p2 <- ggplot()
p2 <- p2 + geom_point(data = act, aes(x = X, y = Y, size = source_volume), alpha = 0.5)
p2 <- p2 + scale_size_area(, max_size = 10)
p2 <- p2 + theme_void()
p2 <- p2 + coord_equal()

library(patchwork)
p1 + p2



flows[substr(local_admin_unit_id_to, 1, 2) == "ch" & substr(local_admin_unit_id_from, 1, 2) == "fr"][order(-flow_volume)]
flows[substr(local_admin_unit_id_to, 1, 2) == "fr" & substr(local_admin_unit_id_from, 1, 2) == "ch"][order(-flow_volume)]

flows[order(-abs(flow_volume - ref_flow_volume))][1:10]

costs <- read_parquet("D:/data/mobility/projects/haut-doubs/a9d0ba0c8285b603c904c981ee3d7b53-multimodal_travel_costs.parquet")
costs <- as.data.table(costs)

costs <- merge(transport_zones, costs[costs$from == 317], by.x = "transport_zone_id", by.y = "to", all.x = TRUE)


p <- ggplot()
p <- p + geom_sf(data = costs[costs$mode == "car", ], aes(fill = distance))
p <- p + facet_wrap(~mode)
p
