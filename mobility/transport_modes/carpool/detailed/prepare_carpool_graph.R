library(sf)
library(log4r)
library(data.table)
library(arrow)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]
tz_file_path <- args[2]
graph_fp <- args[3]
modal_shift <- args[4]

package_path <- 'D:/dev/mobility_oss/mobility'
tz_file_path <- 'D:\\data\\mobility\\projects\\haut-doubs\\94c4efec9c89bdd5fae5a9203ae729d0-transport_zones.gpkg'
graph_fp <- "D:\\data\\mobility\\projects\\haut-doubs\\path_graph_car\\contracted\\f92c93e808a0af018f4858ddcc6bb1b5-done"
modal_shift <- '{"max_travel_time": 0.33, "average_speed": 50.0, "shift_time": 10.0, "shortcuts_shift_time": null, "shortcuts_locations": null}'

source(file.path(package_path, "r_utils", "cpprouting_io.R"))

transport_zones <- st_read(tz_file_path)

buildings_sample_fp <- file.path(
  dirname(tz_file_path),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(tz_file_path)),
    "-transport_zones_buildings.parquet"
  )
)

buildings_sample <- as.data.table(read_parquet(buildings_sample_fp))
buildings_sample[, building_id := 1:.N]


hash <- strsplit(basename(graph_fp), "-")[[1]][1]
graph <- read_cppr_contracted_graph(dirname(graph_fp), hash)
verts <- read_parquet(file.path(dirname(dirname(graph_fp)), paste0(hash, "-vertices.parquet")))

modal_shift <- fromJSON(modal_shift)

buildings_sf <- sfheaders::sf_point(buildings_sample, x = "x", y = "y", keep = TRUE)
buildings_buffer <- st_buffer(buildings_sf, dist = 1.1*1000.0*modal_shift$average_speed*modal_shift$max_travel_time)

intersects <- st_intersects(buildings_buffer, buildings_sf)

hist(lengths(intersects))

# Create informal carpooling spots at the centers of each transport zone

# Create carpooling spots where the user wants them

# Compute the travel times and distances between all transport zones and their nearest carpooling spots
# (distance and time are zero by construction within a transport zone, so we use the internal distance to estimate them)

# Create an access graph to the carpooling graph with these costs

# Concatenate access and carpooling costs

