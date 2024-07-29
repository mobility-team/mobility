# Parse arguments
args <- commandArgs(trailingOnly = TRUE)
packages <- args[-length(args)]
force_reinstall <- args[length(args)] == "True"

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


# Install packages
repos <- unlist(lapply(strsplit(packages, "/"), "[[", 1))

# CRAN packages
cran_packages <- unlist(lapply(strsplit(packages[repos == "CRAN"], "/"), "[[", 2))

if (force_reinstall == FALSE) {
  cran_packages <- cran_packages[!(cran_packages %in% rownames(installed.packages()))]
}

if (length(cran_packages) > 0) {
  info(logger, paste0("Installing R packages from CRAN : ", paste0(cran_packages, collapse = ", ")))
  pkg_install(cran_packages, lib = lib)
}

# Github packages
github_packages <- paste(
  unlist(lapply(strsplit(packages[repos == "github"], "/"), "[[", 2)),
  unlist(lapply(strsplit(packages[repos == "github"], "/"), "[[", 3)),
  sep = "/"
)

if (force_reinstall == FALSE) {
  github_packages <- github_packages[!(github_packages %in% rownames(installed.packages()))]
}

if (length(github_packages) > 0) {
  info(logger, paste0("Installing R packages from Github :", paste0(github_packages, collapse = ", ")))
  remotes::install_github(github_packages, lib = lib)
}

# Local packages
binaries_paths <- unlist(lapply(strsplit(packages[repos == "local"], "/"), "[[", 2))
local_packages <- unlist(lapply(strsplit(basename(binaries_paths), "_"), "[[", 1))

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

