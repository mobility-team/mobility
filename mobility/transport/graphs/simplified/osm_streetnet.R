#' Normalize OSM oneway tags for dodgr.
#'
#' dodgr expects explicit yes/no oneway tags. This function rewrites unusual
#' oneway values and adds oneway=no rows for roads where the tag is missing.
#'
#' @param osm_data OSM data returned by osmdata_sc().
#' @return The same OSM data object with normalized oneway rows.
normalize_oneway_tags <- function(osm_data) {
  # dodgr expects explicit yes/no oneway values. Normalize unusual values and
  # add a no-oneway row for roads where the tag is missing.
  osm_data$object <- osm_data$object %>%
    mutate(
      oneway = if_else(key == "oneway", value, NA_character_)
    ) %>%
    mutate(
      oneway = case_when(
        oneway %in% c("yes") ~ "yes",
        oneway %in% c("no", "-1", "alternating", "reversible") ~ "no",
        TRUE ~ "no"
      )
    ) %>%
    mutate(
      value = if_else(key == "oneway", oneway, value)
    ) %>%
    select(
      -oneway
    )

  all_ids <- unique(osm_data$object$object_)
  oneway_ids <- osm_data$object %>%
    filter(key == "oneway") %>%
    pull(object_)

  no_oneway_ids <- setdiff(all_ids, oneway_ids)

  new_rows <- tibble(
    object_ = no_oneway_ids,
    key = "oneway",
    value = "no"
  )

  osm_data$object <- bind_rows(osm_data$object, new_rows)
  osm_data
}

#' Get the dodgr routing mode name used by a mobility mode.
#'
#' @param mode Mobility mode name.
#' @return dodgr mode name.
get_dodgr_mode <- function(mode) {
  modes <- list(
    walk = "foot",
    car = "motorcar",
    bicycle = "bicycle"
  )

  modes[[mode]]
}

#' Get extra OSM columns to keep when dodgr builds a street graph.
#'
#' @param mode Mobility mode name.
#' @return Character vector of extra columns, or NULL when no extra columns are needed.
get_weight_streetnet_keep_cols <- function(mode) {
  if (mode != "car") {
    return(NULL)
  }

  c(
    "hgv", "hov", "access",
    "motor_vehicle", "motorcar", "vehicle",
    "lanes:forward", "lanes:backward",
    "lanes:psv:forward", "lanes:psv:backward",
    "psv:lanes:forward", "psv:lanes:backward"
  )
}

#' Write a dodgr weight profile restricted to configured highway classes.
#'
#' @param osm_capacity_parameters Table of highway classes and car capacity parameters.
#' @param output_file_path Final graph marker path, used to name the profile file.
#' @return Path to the written dodgr weight profile JSON file.
write_dodgr_weight_profile <- function(osm_capacity_parameters, output_file_path) {
  # Restrict OSM ways to the ones specified in the OSMCapacityParameters object.
  wt_profiles <- dodgr::weighting_profiles
  df <- wt_profiles$weighting_profiles
  df <- df[df$way %in% osm_capacity_parameters$highway, ]
  wt_profiles$weighting_profiles <- df

  hash <- strsplit(basename(output_file_path), "-")[[1]][1]
  wt_fp <- paste0(dirname(output_file_path), "/", paste0(hash, "-dodgr-wt_profile.json"))
  write(toJSON(wt_profiles), wt_fp)

  wt_fp
}

#' Build a weighted dodgr street graph for a mobility path mode.
#'
#' @param osm_data OSM data returned by osmdata_sc().
#' @param mode Mobility mode name.
#' @param osm_capacity_parameters Table of highway classes and capacity parameters.
#' @param output_file_path Final graph marker path, used to name the dodgr profile file.
#' @return Weighted dodgr street graph restricted to the largest connected component.
weight_path_streetnet <- function(osm_data, mode, osm_capacity_parameters, output_file_path) {
  dodgr_cache_off()

  wt_fp <- write_dodgr_weight_profile(osm_capacity_parameters, output_file_path)

  graph <- weight_streetnet(
    osm_data,
    wt_profile = get_dodgr_mode(mode),
    wt_profile_file = wt_fp,
    turn_penalty = FALSE,
    keep_cols = get_weight_streetnet_keep_cols(mode)
  )

  graph[graph$component == 1, ]
}
