source("mobility/load_packages.R")
packages <- c("dodgr", "optparse")
load_packages(packages)

option_list = list(
  make_option(c("-d", "--dodgr-mode"), type = "character")
)

opt_parser = OptionParser(option_list = option_list)
opt = parse_args(opt_parser)

profiles <- dodgr::weighting_profiles$weighting_profiles
highway_tags <- profiles[profiles$name == opt[["dodgr-mode"]] & !is.na(profiles$max_speed) & profiles$value > 0.0, "way"]
highway_tags <- paste(highway_tags, collapse = ",")

cat(paste(highway_tags, collapse = ","))