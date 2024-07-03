library(stringr)
library(log4r)

invisible(lapply("log4r", library, character.only = TRUE))
logger <- logger(appenders = console_appender())

binary_paths <- commandArgs(trailingOnly = TRUE)

packages <- unlist(lapply(str_split(basename(binary_paths), "_"), "[[", 1))
installed_packages <- packages %in% rownames(installed.packages())

if (any(installed_packages == FALSE)) {
  info(logger, paste("Installing R packages from binaries :", paste(packages[!installed_packages], collapse = ", ")))
  install.packages(
    binary_paths[!installed_packages],
    repos = NULL,
    type = "binary",
    quiet = FALSE
  )
}

