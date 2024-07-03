
packages <- commandArgs(trailingOnly = TRUE)

if ("log4r" %in% rownames(installed.packages()) == FALSE) {
  install.packages(
    "log4r",
    repos =  "https://packagemanager.rstudio.com/cran/latest", 
    quiet = TRUE
  )
}

packages <- packages[!(packages %in% c("log4r"))]

library(log4r)
logger <- logger(appenders = console_appender())

installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  info(logger, paste("Installing R packages from Github :", paste(packages[!installed_packages], collapse = ", ")))
  remotes::install_github(
    packages[!installed_packages],
    quiet = TRUE
  )
}