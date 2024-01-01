load_packages <- function(packages) {
  
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(
      packages[!installed_packages],
      repo = "https://cran.rstudio.com/",
      lib = file.path(Sys.getenv("MOBILITY_ENV_PATH"), "Lib/R/library"),
      quiet = TRUE
    )
  }
  
  invisible(lapply(packages, library, character.only = TRUE))
  
}