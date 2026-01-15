library(sf)
library(nngeo)
library(data.table)
library(geos)
library(wk)
library(arrow)
library(cluster)
library(future)
library(future.apply)
library(log4r)

logger <- logger(appenders = console_appender())

args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility\\mobility', 
#   'd:/data/mobility/projects/dolancourt/fcd6652ba13b6f4cb9bbda6f7ba35372-study_area.gpkg', 
#   'd:/data/mobility/projects/dolancourt/building-osm_data',
#   '1',
#   'd:/data/mobility/projects/dolancourt/470538bc0411a93b96abf28c4e6848b8-transport_zones.gpkg'
# )

package_path <- args[1]
study_area_fp <- args[2]
osm_buildings_fp <- args[3]
level_of_detail <- as.integer(args[4])
output_fp <- args[5]



# package_path <- 'D:/dev/mobility_oss/mobility'
# study_area_fp <- 'D:/data/mobility/projects/post-carbon/770d300c5e292c864d61ca4cd7bcbb62-study_area.gpkg'
# osm_buildings_fp <- 'D:/data/mobility/projects/post-carbon/building-osm_data'
# level_of_detail <- 1
# output_fp <- 'D:/data/mobility/projects/study_area/51d687bdfde7cd7e33d288929ffe4ff6-transport_zones.gpkg'


clusters_fp <- file.path(
  dirname(output_fp),
  paste0(
    gsub("-transport_zones.gpkg", "", basename(output_fp)),
    "-transport_zones_buildings.parquet"
  )
)

buildings_area_threshold <- 2e5
n_buildings_sample <- 10
min_building_area <- 20
max_building_area <- 500e3

convert_sf_to_geos_dt <- function(sf_df) {
  
  st_geometry(sf_df) <- "geometry"
  
  dt <- as.data.table(sf_df)
  
  if (nrow(dt) == 1){
    dt <- dt[rep(1:.N, each = 2)]
    dt[, geometry := as_geos_geometry(geometry)]
    dt <- dt[1, ]
  } else{
    dt[, geometry := as_geos_geometry(geometry)]
  }
  
  return(dt)
  
}


compute_cluster_internal_distance <- function(buildings_dt) {
  
  # Compute the median distance between random buildings within each cluster 
  # with a coefficient of detour based and the crow fly distance
  set.seed(0)
  
  from_buildings <- buildings_dt[,
                                 .SD[sample(.N, 1000, replace = TRUE, prob = area)],
                                 by = cluster,
                                 .SDcols = c("X", "Y")
  ]
  
  to_buildings <- buildings_dt[,
                               .SD[sample(.N, 1000, replace = TRUE, prob = area)],
                               by = cluster,
                               .SDcols = c("X", "Y")
  ]
  
  distances <- cbind(
    from_buildings[, list(cluster, x_from = X, y_from = Y)],
    to_buildings[, list(x_to = X, y_to = Y)]
  )
  
  distances[, distance := sqrt((x_from - x_to)^2 + (y_from - y_to)^2)]
  distances[, distance := distance*(1.1+0.3*exp(-distance/20))]
  
  internal_distance <- distances[, list(internal_distance = median(distance)), by = cluster]
  
  return(internal_distance)
  
}

compute_k_medoids <- function(buildings_dt) {
  
  bdt <- copy(buildings_dt)
  n_buildings <- nrow(bdt)
  
  k_medoids <- lapply(1:5, function(i) {
    
    # Make sure at least 10 buildings are in each subcluster
    n <- max(1, min(i, floor(n_buildings/10)))
    
    pam_result <- cluster::clara(bdt[, list(X, Y)], n)
    
    bdt[, subcluster := pam_result$clustering]
    subcluster_area <- bdt[, list(area = sum(area)), by = subcluster]
    subcluster_area[, weight := area/sum(area)]
    
    k_medoids <- as.data.table(pam_result$medoids)[, list(x = X, y = Y)]
    k_medoids[, subcluster := 1:.N]
    
    k_medoids <- merge(k_medoids, subcluster_area, by = "subcluster")
    
    k_medoids[, n_clusters := i]
    
  })
  k_medoids <- rbindlist(k_medoids)
  k_medoids <- k_medoids[, list(n_clusters, x, y, weight)]
  
  return(k_medoids)
  
}


clusters_to_voronoi <- function(lau_id, lau_geom, level_of_detail, buildings_area_threshold, n_buildings_sample, minimum_building_area) {
  
  # Get the coordinates and area of all buildings in the area
  # Keep only buildings with footprints larger than 20 m²
  buildings <- st_read(
    file.path(osm_buildings_fp, lau_id, "building.pbf"),
    query = "select osm_id from multipolygons",
    quiet = TRUE
  )
  
  buildings <- st_transform(buildings, wk_crs(lau_geom))
  buildings$area <- as.numeric(st_area(buildings))
  buildings <- buildings[buildings$area > min_building_area & buildings$area < max_building_area, ]
  
  st_agr(buildings) <- "constant"
  buildings <- st_centroid(buildings)
  
  buildings_dt <- cbind(
    as.data.table(st_drop_geometry(buildings)),
    as.data.table(st_coordinates(buildings))
  )
  buildings_dt[, building_id := 1:nrow(buildings_dt)]
  
  n_clusters <- ceiling(sum(buildings_dt$area)/buildings_area_threshold)
  
  # Split the transport zone into clusters based on the area of buildings
  if (level_of_detail == 1 & n_clusters > 1) {
    
    k_medoids <- clara(buildings_dt[, list(X, Y)], n_clusters, samples = 50)
    
    clusters <- as.data.table(k_medoids$medoids)
    clusters[, cluster := 1:.N]
    
    buildings_dt[, cluster := k_medoids$clustering]
    
    cluster_area <- buildings_dt[, list(area = sum(area)), by = cluster]
    clusters <- merge(clusters, cluster_area, by = "cluster")
    
    transport_zones <- clusters[, list(
      transport_zone_id = cluster,
      weight = area/sum(area),
      x = X,
      y = Y
    )]
    
    internal_distances <- compute_cluster_internal_distance(buildings_dt)
    transport_zones <- merge(transport_zones, internal_distances, by.x  = "transport_zone_id", by.y = "cluster")
    
    # Create a voronoi tesselation around the cluster centers
    env <- geos_create_rectangle(
      xmin = min(buildings_dt$X),
      ymin = max(buildings_dt$Y),
      xmax = min(buildings_dt$X),
      ymax = max(buildings_dt$Y),
      crs = wk_crs(lau_geom)
    )
    
    env <- geos_buffer(env, 100e3)
    
    clusters_geos <- geos_make_collection(geos_read_xy(clusters[, list(X, Y)]))
    wk_crs(clusters_geos) <- wk_crs(lau_geom)
    
    voronoi <- geos_voronoi_polygons(clusters_geos, env)
    voronoi <- geos_geometry_n(voronoi, seq_len(geos_num_geometries(voronoi)))
    voronoi <- geos_intersection(voronoi, lau_geom)
    
    v_order <- st_intersects(
      st_as_sf(geos_geometry_n(clusters_geos, seq_len(geos_num_geometries(clusters_geos)))),
      st_as_sf(voronoi)
    )
    
    transport_zones[, geometry := voronoi[unlist(v_order)]]
    
    #
    k_medoids <- buildings_dt[, compute_k_medoids(.SD), by = list(transport_zone_id = cluster)]
    
    # library(ggplot2)
    # p <- ggplot(buildings_dt)
    # p <- p + geom_point(aes(x = X, y = Y, color = factor(cluster)), alpha = 0.5)
    # p <- p + geom_point(data = k_medoids[n_clusters == max(n_clusters)], aes(x = x, y = y), size = 2, alpha = 0.5)
    # p <- p + geom_point(data = clusters, aes(x = X, y = Y), size = 3)
    # p <- p + coord_equal()
    # p
    # 
    
    
  } else {
    
    
    transport_zones <- data.table(
      transport_zone_id = 1,
      weight = 1.0,
      geometry = lau_geom
    )
    
    buildings_dt[, cluster := 1]
    internal_distances <- compute_cluster_internal_distance(buildings_dt)
    transport_zones <- merge(transport_zones, internal_distances, by.x  = "transport_zone_id", by.y = "cluster")
    
    k_medoids <- compute_k_medoids(buildings_dt)
    k_medoids[, transport_zone_id := 1]
    
    transport_zones[, x := k_medoids[n_clusters == 1, x]]
    transport_zones[, y := k_medoids[n_clusters == 1, y]]
    
  }
  
  transport_zones[, local_admin_unit_id := lau_id]
  transport_zones[, geometry := geos_write_wkb(geometry)]
  
  k_medoids[, local_admin_unit_id := lau_id]
  
  return(list(transport_zones, k_medoids))
  
}

study_area <- st_read(study_area_fp, quiet = TRUE)
study_area_dt <- convert_sf_to_geos_dt(study_area)
study_area_dt[, geometry_wkb := geos_write_wkb(geometry)]

set.seed(0)

# plan(multisession, workers = max(parallel::detectCores(logical = FALSE)-3, 1))
# plan(sequential)

transport_zones_buildings <- lapply(
  
  study_area_dt$local_admin_unit_id,
  # future.seed = 0,
  
  FUN = function(lau_id) {
    
    info(logger, sprintf("Clustering buildings of LAU %s...", lau_id))
    
    lau_geom <- study_area_dt[local_admin_unit_id == lau_id, geometry_wkb]
    lau_geom <- geos_read_wkb(lau_geom)
    wk_crs(lau_geom) <- "EPSG:3035"
    
    result <- clusters_to_voronoi(
      lau_id = lau_id,
      lau_geom = lau_geom,
      level_of_detail = level_of_detail,
      buildings_area_threshold = buildings_area_threshold,
      n_buildings_sample = n_buildings_sample,
      minimum_building_area = min_building_area
    )
    
    return(result)
    
  }
)

# plan(sequential)

transport_zones <- rbindlist(lapply(transport_zones_buildings, "[[", 1), use.names = TRUE)
clusters <- rbindlist(lapply(transport_zones_buildings, "[[", 2), use.names = TRUE)

# Create a unique integer id for each transport zone
transport_zones[, new_transport_zone_id := 1:.N]

clusters <- merge(
  clusters,
  transport_zones[, list(local_admin_unit_id, transport_zone_id, new_transport_zone_id)],
  by = c("local_admin_unit_id", "transport_zone_id")
)
clusters[, transport_zone_id := NULL]
setnames(clusters, "new_transport_zone_id", "transport_zone_id")

transport_zones[, transport_zone_id := NULL]
setnames(transport_zones, "new_transport_zone_id", "transport_zone_id")

transport_zones[, geometry := geos_read_wkb(geometry)]
wk_crs(transport_zones$geometry) <- "EPSG:3035"
transport_zones <- st_as_sf(transport_zones)

# Write the result
st_write(transport_zones, output_fp, delete_dsn = TRUE, quiet = TRUE)
write_parquet(clusters, clusters_fp)