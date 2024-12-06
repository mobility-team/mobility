library(DBI)
library(duckdb)
library(jsonlite)
library(arrow)

duckdb_df_to_parquet <- function(df, con, path) {
  
  duckdb_register(con, "df", df)
  
  dbExecute(
    con,
    sprintf(
      "COPY df TO '%s' (FORMAT 'parquet')",
      path
    )
  )
  
  duckdb_unregister(con, "df")
  
}

duckdb_vector_to_parquet <- function(v, con, path) {
  
  df <- as.data.frame(v)
  
  duckdb_register(con, "df", df)
  
  dbExecute(
    con,
    sprintf(
      "COPY df TO '%s' (FORMAT 'parquet')",
      path
    )
  )
  
  duckdb_unregister(con, "df")
  
}


duckdb_parquet_to_df <- function(con, path) {
  dbExecute(
    con,
    sprintf(
      "CREATE TABLE df AS SELECT * FROM '%s'",
      path
    )
  )
  df <- dbGetQuery(con, "SELECT * FROM df")
  dbExecute(con, "DROP TABLE df")
  return(df)
}


duckdb_parquet_to_vector <- function(con, path) {
  dbExecute(
    con,
    sprintf(
      "CREATE TABLE df AS SELECT * FROM '%s'",
      path
    )
  )
  df <- dbGetQuery(con, "SELECT * FROM df")
  dbExecute(con, "DROP TABLE df")
  return(df[[1]])
}


save_cppr_graph <- function(graph, path, hash) {
  
  attrib <- data.table(
    i = 1:nrow(graph$data),
    aux = NA,
    alpha = NA,
    beta = NA,
    cap = NA
  )
  
  for (var in names(graph$attrib)) {
    attrib[[var]] <- graph$attrib[[var]]
  }
  
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  duckdb_df_to_parquet(graph$data, con, file.path(path, paste0(hash, "data.parquet")))
  duckdb_df_to_parquet(graph$dict, con, file.path(path, paste0(hash, "dict.parquet")))
  duckdb_df_to_parquet(attrib, con, file.path(path, paste0(hash, "attrib.parquet")))
  
  dbDisconnect(con, shutdown = TRUE)
  
}

read_cppr_graph <- function(path, hash) {
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  graph <- list(
    data = NULL,
    coords = NULL,
    nbnode = NULL,
    dict = NULL,
    attrib = list(
      aux = NULL,
      alpha = NULL,
      beta = NULL,
      cap = NULL
    )
  )
  
  graph[["data"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "data.parquet")))
  graph[["dict"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash,"dict.parquet")))
  
  attrib <- duckdb_parquet_to_df(con, file.path(path, paste0(hash,"attrib.parquet")))
  
  for (var in colnames(attrib)) {
    graph$attrib[[var]] <- attrib[[var]]
  }
  
  graph$nbnode <- nrow(graph[["dict"]])
  
  dbDisconnect(con, shutdown = TRUE)
  
  return(graph)
  
}


save_cppr_contracted_graph <- function(graph, path, hash) {

  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }

  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  duckdb_df_to_parquet(graph$data, con, file.path(path, paste0(hash, "data.parquet")))
  duckdb_vector_to_parquet(graph$rank, con, file.path(path, paste0(hash, "rank.parquet")))
  duckdb_df_to_parquet(graph$shortcuts, con, file.path(path, paste0(hash, "shortcuts.parquet")))
  duckdb_df_to_parquet(graph$dict, con, file.path(path, paste0(hash, "dict.parquet")))
  duckdb_df_to_parquet(graph$original$data, con, file.path(path, paste0(hash, "original_data.parquet")))
  duckdb_vector_to_parquet(graph$original$attrib$aux, con, file.path(path, paste0(hash, "original_data_attrib_aux.parquet")))

  dbDisconnect(con, shutdown = TRUE)

}

read_cppr_contracted_graph <- function(path, hash) {

  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  graph <- list(
    data = NULL,
    rank = NULL,
    shortcuts = NULL,
    nbnode = NULL,
    dict = NULL,
    original = list(
      data = NULL,
      attrib = list(
        aux = NULL,
        cap = NULL,
        alpha = NULL,
        beta = NULL
      )
    )
  )

  graph[["data"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "data.parquet")))
  graph[["rank"]] <- duckdb_parquet_to_vector(con, file.path(path, paste0(hash, "rank.parquet")))
  graph[["shortcuts"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "shortcuts.parquet")))
  graph[["nbnode"]] <- nrow(graph[["data"]])
  graph[["dict"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "dict.parquet")))
  graph[["original"]] <- list(data = NULL, attrib = list(aux = NULL, cap = NULL, alpha = NULL, beta = NULL))
  graph[["original"]][["data"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "original_data.parquet")))
  graph[["original"]][["attrib"]][["aux"]] <- duckdb_parquet_to_vector(con, file.path(path, paste0(hash, "original_data_attrib_aux.parquet")))

  dbDisconnect(con, shutdown = TRUE)

  return(graph)

}


save_graph_vertices <- function(vertices, path) {
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  duckdb_df_to_parquet(vertices, con, path)
  dbDisconnect(con, shutdown = TRUE)
  
}


read_graph_vertices <- function(path) {
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  vertices <- duckdb_parquet_to_df(con, path)
  dbDisconnect(con, shutdown = TRUE)
  
  return(vertices)
  
}
