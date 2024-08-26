
# Install pak if needed
lib <- Sys.getenv("R_LIBS")

# Install pak if not available
if (!("pak" %in% installed.packages())) {
  install.packages("pak", lib = lib, repos = sprintf(
    "https://r-lib.github.io/p/pak/%s/%s/%s/%s",
    "stable",
    .Platform$pkgType,
    R.Version()$os,
    R.Version()$arch
  ))
}
library(pak)


# Install log4r if not available
if (!("log4r" %in% installed.packages())) {
  pkg_install("log4r", lib = lib)
}
library(log4r)
logger <- logger(appenders = console_appender())

# Install log4r if not available
if (!("jsonlite" %in% installed.packages())) {
  pkg_install("jsonlite", lib = lib)
}
library(jsonlite)

# Parse arguments
args <- commandArgs(trailingOnly = TRUE)
args <- fromJSON(args, simplifyVector = FALSE)
packages <- args[["packages"]]
force_reinstall <- args[["force_reinstall"]]

# CRAN packages
cran_packages <- Filter(function(p) {p[["source"]]} == "CRAN", packages)
if (length(cran_packages) > 0) {
  cran_packages <- unlist(lapply(cran_packages, "[[", "name"))
} else {
  cran_packages <- c()
}

if (force_reinstall == FALSE) {
  cran_packages <- cran_packages[!(cran_packages %in% rownames(installed.packages()))]
}

if (length(cran_packages) > 0) {
  info(logger, paste0("Installing R packages from CRAN : ", paste0(cran_packages, collapse = ", ")))
  pkg_install(cran_packages, lib = lib)
}

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
  remotes::install_github(github_packages, lib = lib)
}

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
  install.packages(
    binaries_paths,
    repos = NULL,
    type = "binary",
    quiet = FALSE
  )
}

