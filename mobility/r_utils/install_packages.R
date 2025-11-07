# -----------------------------------------------------------------------------
# Parse arguments
args <- commandArgs(trailingOnly = TRUE)

packages <- args[2]

force_reinstall <- args[3]
force_reinstall <- as.logical(force_reinstall)

download_method <- args[4]

# -----------------------------------------------------------------------------
# Install pak if needed
if (!("pak" %in% installed.packages()) | force_reinstall) {

  message("Installing pak...")

  tryCatch({

      install.packages(
        "pak",
        method = download_method,
        repos = sprintf(
          "https://r-lib.github.io/p/pak/%s/%s/%s/%s",
          "stable",
          .Platform$pkgType,
          R.Version()$os,
          R.Version()$arch
        )
      )

      return("pak" %in% installed.packages())

  }, error = function(e) {

    message("Pak installation failed with the default method, retrying with R_LIBCURL_SSL_REVOKE_BEST_EFFORT=TRUE.")
    Sys.setenv(R_LIBCURL_SSL_REVOKE_BEST_EFFORT=TRUE)

    install.packages(
      "pak",
      method = download_method,
      repos = sprintf(
        "https://r-lib.github.io/p/pak/%s/%s/%s/%s",
        "stable",
        .Platform$pkgType,
        R.Version()$os,
        R.Version()$arch
      )
    )

  })

}

library(pak)

pkg_install_if_needed <- function(packages, force_reinstall, log) {
  
  print(packages)
  
  installed_packages <- packages[packages %in% installed.packages()]

  if (force_reinstall) {
    remove.packages(installed_packages)
  }
  
  packages <- packages[!(packages %in% installed.packages())]

  if (log) {
    info(logger, paste0("Installing R packages: ", paste0(packages, collapse = ", ")))
  }
  
  pkg_install(packages)

}

pkg_install_with_fallback <- function(packages, force_reinstall, log = TRUE) {

  tryCatch({

    pkg_install_if_needed(packages, force_reinstall, log)

  }, error = function(e) {

    message("Package installation failed with the default method, retrying with R_LIBCURL_SSL_REVOKE_BEST_EFFORT=TRUE.")
    Sys.setenv(R_LIBCURL_SSL_REVOKE_BEST_EFFORT=TRUE)
    pkg_install_if_needed(packages, force_reinstall, log)

  })

}

# Install log4r if not available
pkg_install_with_fallback(
  c("log4r", "jsonlite"),
  force_reinstall,
  log = FALSE
)

library(log4r)
library(jsonlite)

logger <- logger(appenders = console_appender())
packages <- fromJSON(packages, simplifyDataFrame = FALSE)

# -----------------------------------------------------------------------------
# CRAN packages
cran_packages <- Filter(function(p) {p[["source"]]} == "CRAN", packages)
if (length(cran_packages) > 0) {
  cran_packages <- unlist(lapply(cran_packages, "[[", "name"))
} else {
  cran_packages <- c()
}

pkg_install_with_fallback(
  cran_packages,
  force_reinstall,
  log = TRUE
)

# -----------------------------------------------------------------------------
# Local packages
local_packages <- Filter(function(p) {p[["source"]]} == "local", packages)

if (length(local_packages) > 0) {
  binaries_paths <- unlist(lapply(local_packages, "[[", "path"))
  local_packages <- unlist(lapply(strsplit(basename(binaries_paths), "_"), "[[", 1))
} else {
  local_packages <- c()
}

if (force_reinstall == FALSE) {
  local_packages <- local_packages[!(local_packages %in% rownames(installed.packages()))]
}

if (length(local_packages) > 0) {
  info(logger, paste0("Installing R packages from local binaries : ", paste0(local_packages, collapse = ", ")))
  info(logger, binaries_paths)
  install.packages(
    binaries_paths,
    repos = NULL,
    type = "binary",
    quiet = FALSE
  )
}

# -----------------------------------------------------------------------------
# Github packages
github_packages <- Filter(function(p) {p[["source"]]} == "github", packages)
if (length(github_packages) > 0) {
  github_packages <- unlist(lapply(github_packages, "[[", "name"))
} else {
  github_packages <- c()
}

if (force_reinstall == FALSE) {
  github_packages <- github_packages[!(github_packages %in% rownames(installed.packages()))]
}

if (length(github_packages) > 0) {
  info(logger, paste0("Installing R packages from Github :", paste0(github_packages, collapse = ", ")))
  remotes::install_github(github_packages)
}


