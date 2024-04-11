
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
  info(logger, "Installing R packages...")
  info(logger, "It should take a while, but it is needed only once, for the first use of the mobility package.")
  install.packages(
    packages[!installed_packages],
    repos =  "https://packagemanager.rstudio.com/cran/latest",
    quiet = TRUE
  )
}