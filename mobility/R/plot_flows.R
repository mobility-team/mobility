library(log4r)
library(arrow)
library(ggplot2)
library(data.table)
library(sf)

args <- commandArgs(trailingOnly = TRUE)

args <- c(
  "D:/data/mobility/projects/haut-doubs/a25b56abc681fdfbf95b35a21c4b59db-transport_zones.gpkg",
  "D:/data/mobility/projects/haut-doubs/e201a8ed4e662209cb7f66aa576aa20b-work_od_flows.parquet"
)

tz_file_path <- args[1]
flows_file_path <- args[2]

transport_zones <- st_read(tz_file_path, quiet = TRUE)

transport_zones_centroids <- as.data.table(st_coordinates(st_centroid(transport_zones)))
transport_zones_centroids$transport_zone_id <- transport_zones$transport_zone_id

study_zone_outline <- st_union(transport_zones)
study_zone_outline <- st_buffer(st_buffer(study_zone_outline, 500), -500)

flows <- as.data.table(read_parquet(flows_file_path))
flows <- flows[flow_volume > 10]

flows <- merge(flows, transport_zones_centroids, by.x = "from", by.y = "transport_zone_id")
flows <- merge(flows, transport_zones_centroids, by.x = "to", by.y = "transport_zone_id", suffixes = c("_from", "_to"))



p <- ggplot()
p <- p + geom_sf(data = transport_zones, fill = NA, size = 0.25, color = "#f5f6fa")
p <- p + geom_sf(data = study_zone_outline, fill = NA, size = 0.25, color = "#7f8fa6")
p <- p + geom_segment(data = flows[from != to], aes(x = X_from, y = Y_from, xend = X_to, yend = Y_to, size = flow_volume), alpha = 0.075, col = "red", lineend = "round")
p <- p + geom_segment(data = flows[from == to], aes(x = X_from, y = Y_from, xend = X_to, yend = Y_to, size = flow_volume), alpha = 0.3, col = "red", lineend = "round")
p <- p + scale_size(range = c(0, 10), limits = c(0, max(flows$flow_volume)))
p <- p + theme_void()
p <- p + theme(
  text = element_text(family = "Avenir"),
  plot.title = element_text(face = "bold", size = 14),
  plot.title.position = "plot",
  plot.subtitle = element_text(face = "italic", size = 12),
  legend.title = element_blank(),
  legend.text = element_text(size = 10, family = "AvenirLight"),
  panel.grid = element_blank(),
)
p