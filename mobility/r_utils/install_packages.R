#!/usr/bin/env Rscript
# -----------------------------------------------------------------------------
# Parse arguments
args <- commandArgs(trailingOnly = TRUE)

# args <- c(
#   'D:\\dev\\mobility\\mobility',
#   '[{"source": "CRAN", "name": "remotes"}, {"source": "CRAN", "name": "dodgr"}, {"source": "CRAN", "name": "sf"}, {"source": "CRAN", "name": "dplyr"}, {"source": "CRAN", "name": "sfheaders"}, {"source": "CRAN", "name": "nngeo"}, {"source": "CRAN", "name": "data.table"}, {"source": "CRAN", "name": "arrow"}, {"source": "CRAN", "name": "hms"}, {"source": "CRAN", "name": "lubridate"}, {"source": "CRAN", "name": "future"}, {"source": "CRAN", "name": "future.apply"}, {"source": "CRAN", "name": "ggplot2"}, {"source": "CRAN", "name": "cppRouting"}, {"source": "CRAN", "name": "duckdb"}, {"source": "CRAN", "name": "gtfsrouter"}, {"source": "CRAN", "name": "geos"}, {"source": "CRAN", "name": "FNN"}, {"source": "CRAN", "name": "cluster"}, {"source": "CRAN", "name": "dbscan"}, {"source": "local", "path": "D:\\\\dev\\\\mobility\\\\mobility\\\\resources\\\\osmdata_0.2.5.005.zip"}]',
#   'False',
#   'auto'
# )

packages <- args[2]
force_reinstall <- as.logical(args[3])
download_method <- args[4]

# -----------------------------------------------------------------------------
# Install pak if needed
if (!("pak" %in% installed.packages()) | force_reinstall == TRUE) {

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
  
  installed_packages <- packages[packages %in% installed.packages()]

  if (force_reinstall) {
    remove.packages(installed_packages)
  }
  
  packages <- packages[!(packages %in% installed.packages())]
  
  if (length(packages) > 0) {
    
    if (log) {
      info(logger, paste0("Installing R packages: ", paste0(packages, collapse = ", ")))
    }
    
    pkg_install(packages)
    
  } 

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

is_linux   <- function() .Platform$OS.type == "unix" && Sys.info()[["sysname"]] != "Darwin"
is_windows <- function() .Platform$OS.type == "windows"

# Normalize download method: never use wininet on Linux
if (is_linux() && tolower(download_method) %in% c("wininet", "", "auto")) download_method <- "libcurl"
if (download_method == "") download_method <- if (is_windows()) "wininet" else "libcurl"

# Global options (fast CDN for CRAN)
options(
  repos = c(CRAN = "https://cloud.r-project.org"),
  download.file.method = download_method,
  timeout = 600
)

# -------- Logging helpers (no hard dependency on log4r) ----------------------
use_log4r <- "log4r" %in% rownames(installed.packages())
if (use_log4r) {
  suppressMessages(library(log4r, quietly = TRUE, warn.conflicts = FALSE))
  .logger   <- logger(appenders = console_appender())
  info_log  <- function(...) info(.logger, paste0(...))
  warn_log  <- function(...) warn(.logger, paste0(...))
  error_log <- function(...) error(.logger, paste0(...))
} else {
  info_log  <- function(...) cat("[INFO] ",  paste0(...), "\n", sep = "")
  warn_log  <- function(...) cat("[WARN] ",  paste0(...), "\n", sep = "")
  error_log <- function(...) cat("[ERROR] ", paste0(...), "\n", sep = "")
}

# -------- Minimal helpers -----------------------------------------------------
safe_install <- function(pkgs, ...) {
  missing <- setdiff(pkgs, rownames(installed.packages()))
  if (length(missing)) {
    install.packages(missing, dependencies = TRUE, ...)
  }
}

# -------- JSON parsing --------------------------------------------------------
if (!("jsonlite" %in% rownames(installed.packages()))) {
  # Try to install jsonlite; if it fails we must stop (cannot parse the package list)
  try(install.packages("jsonlite", dependencies = TRUE), silent = TRUE)
}

# -----------------------------------------------------------------------------
# Github packages
github_packages <- Filter(function(p) {p[["source"]]} == "github", packages)
if (length(github_packages) > 0) {
  github_packages <- unlist(lapply(github_packages, "[[", "name"))
} else {
  github_packages <- c()
}

# =============================================================================
# CRAN packages
# =============================================================================
cran_entries <- Filter(function(p) identical(p[["source"]], "CRAN"), packages)
cran_pkgs <- if (length(cran_entries)) unlist(lapply(cran_entries, `[[`, "name")) else character(0)

if (length(cran_pkgs)) {
  if (!force_reinstall) {
    cran_pkgs <- setdiff(cran_pkgs, already_installed)
  }
  if (length(cran_pkgs)) {
    info_log("Installing CRAN packages: ", paste(cran_pkgs, collapse = ", "))
    if (have_pak) {
      tryCatch(
        { pak::pkg_install(cran_pkgs) },
        error = function(e) {
          warn_log("pak::pkg_install() failed: ", conditionMessage(e), " -> falling back to install.packages()")
          install.packages(cran_pkgs, dependencies = TRUE)
        }
      )
    } else {
      install.packages(cran_pkgs, dependencies = TRUE)
    }
  } else {
    info_log("CRAN packages already satisfied; nothing to install.")
  }
}

# =============================================================================
# GitHub packages
# =============================================================================
github_entries <- Filter(function(p) identical(p[["source"]], "github"), packages)
gh_pkgs <- if (length(github_entries)) unlist(lapply(github_entries, `[[`, "name")) else character(0)

if (length(gh_pkgs)) {
  if (!force_reinstall) {
    gh_pkgs <- setdiff(gh_pkgs, already_installed)
  }
  if (length(gh_pkgs)) {
    info_log("Installing GitHub packages: ", paste(gh_pkgs, collapse = ", "))
    # Ensure 'remotes' is present
    if (!("remotes" %in% rownames(installed.packages()))) {
      try(install.packages("remotes", dependencies = TRUE), silent = TRUE)
    }
    if (!("remotes" %in% rownames(installed.packages()))) {
      stop("Required package 'remotes' is not available and could not be installed.")
    }
    remotes::install_github(gh_pkgs, upgrade = "never")
  } else {
    info_log("GitHub packages already satisfied; nothing to install.")
  }
}

info_log("All requested installations attempted. Done.")
