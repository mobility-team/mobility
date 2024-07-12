library(log4r)
library(arrow)
library(ggplot2)
library(data.table)
library(sf)

args <- commandArgs(trailingOnly = TRUE)

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
flows <- flows[flow_volume > 10]

flows <- merge(flows, transport_zones_centroids, by.x = "from", by.y = "transport_zone_id")
flows <- merge(flows, transport_zones_centroids, by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))

p <- ggplot()
p <- p + geom_sf(data = transport_zones, fill = NA, linewidth = 0.25, color = "#f5f6fa")
p <- p + geom_sf(data = study_zone_outline, fill = NA, linewidth = 0.25, color = "#7f8fa6")
p <- p + geom_segment(data = flows[from != to], aes(x = X_from, y = Y_from, xend = X_to, yend = Y_to, linewidth = flow_volume), alpha = 0.075, col = "#2f4b7c", lineend = "round")
p <- p + geom_segment(data = flows[from == to], aes(x = X_from, y = Y_from, xend = X_to, yend = Y_to, linewidth = flow_volume), alpha = 0.3, col = "#2f4b7c", lineend = "round")
p <- p + scale_linewidth(range = c(0, 20), limits = c(0, max(flows$flow_volume)))
p <- p + theme_void()

ggsave(plot = p, filename = output_file_path, width = 8, height = 8)

