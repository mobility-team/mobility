r_universe_package_needs_install <- function(
  installed_version,
  available_version,
  force_reinstall = FALSE
) {
  if (force_reinstall || is.null(installed_version)) {
    return(TRUE)
  }

  !is.null(available_version) &&
    package_version(available_version) > package_version(installed_version)
}

package_version_meets_minimum <- function(installed_version, minimum_version) {
  is.null(minimum_version) ||
    package_version(installed_version) >= package_version(minimum_version)
}
