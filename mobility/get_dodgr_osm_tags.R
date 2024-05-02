library(dodgr)

dodgr_mode <- commandArgs(trailingOnly = TRUE)

profiles <- dodgr::weighting_profiles$weighting_profiles
highway_tags <- profiles[profiles$name == dodgr_mode & !is.na(profiles$max_speed) & profiles$value > 0.0, "way"]
highway_tags <- paste(highway_tags, collapse = ",")

cat(paste(highway_tags, collapse = ","))