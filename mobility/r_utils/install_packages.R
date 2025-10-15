#!/usr/bin/env Rscript
# -----------------------------------------------------------------------------
# Cross-platform installer for local / CRAN / GitHub packages
# Works on Windows and Linux/WSL without requiring 'pak'.
#
# Args (trailingOnly):
#   args[1] : project root (kept for compatibility, unused here)
#   args[2] : JSON string of packages: list of {source: "local"|"CRAN"|"github", name?, path?}
#   args[3] : force_reinstall ("TRUE"/"FALSE")
#   args[4] : download_method ("auto"|"internal"|"libcurl"|"wget"|"curl"|"lynx"|"wininet")
#
# Env:
#   USE_PAK = "true"/"false" (default false). If true, try pak for CRAN installs; otherwise use install.packages().
# -----------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 4) {
  stop("Expected 4 arguments: <root> <packages_json> <force_reinstall> <download_method>")
}

root_dir        <- args[1]
packages_json   <- args[2]
force_reinstall <- as.logical(args[3])
download_method <- args[4]

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
if (!("jsonlite" %in% rownames(installed.packages()))) {
  stop("Required package 'jsonlite' is not available and could not be installed.")
}
suppressMessages(library(jsonlite, quietly = TRUE, warn.conflicts = FALSE))

packages <- tryCatch(
  fromJSON(packages_json, simplifyDataFrame = FALSE),
  error = function(e) {
    stop("Failed to parse packages JSON: ", conditionMessage(e))
  }
)

already_installed <- rownames(installed.packages())

# -------- Optional: pak (only if explicitly enabled) -------------------------
use_pak  <- tolower(Sys.getenv("USE_PAK", unset = "false")) %in% c("1","true","yes")
have_pak <- FALSE
if (use_pak) {
  info_log("USE_PAK=true: attempting to use 'pak' for CRAN installs.")
  try({
    if (!("pak" %in% rownames(installed.packages()))) {
      install.packages(
        "pak",
        method = download_method,
        repos  = sprintf("https://r-lib.github.io/p/pak/%s/%s/%s/%s",
                         "stable", .Platform$pkgType, R.Version()$os, R.Version()$arch)
      )
    }
    suppressMessages(library(pak, quietly = TRUE, warn.conflicts = FALSE))
    have_pak <- TRUE
    info_log("'pak' is available; will use pak::pkg_install() for CRAN packages.")
  }, silent = TRUE)
  if (!have_pak) warn_log("Could not use 'pak' (network or platform issue). Falling back to install.packages().")
}

# =============================================================================
# LOCAL packages
# =============================================================================
local_entries <- Filter(function(p) identical(p[["source"]], "local"), packages)
if (length(local_entries) > 0) {
  binaries_paths <- unlist(lapply(local_entries, `[[`, "path"))
  local_names <- if (length(binaries_paths)) {
    unlist(lapply(strsplit(basename(binaries_paths), "_"), `[[`, 1))
  } else character(0)

  to_install <- local_names
  if (!force_reinstall) {
    to_install <- setdiff(local_names, already_installed)
  }

  if (length(to_install)) {
    info_log("Installing R packages from local binaries: ", paste(to_install, collapse = ", "))
    info_log(paste(binaries_paths, collapse = "; "))
    install.packages(
      binaries_paths[local_names %in% to_install],
      repos = NULL,
      type  = "binary",
      quiet = FALSE
    )
  } else {
    info_log("Local packages already installed; nothing to do.")
  }
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
