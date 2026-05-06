library(DBI)
library(duckdb)
library(jsonlite)
library(arrow)
library(data.table)

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
    i = 1:nrow(graph[["data"]]),
    aux = NA,
    alpha = NA,
    beta = NA,
    cap = NA
  )
  
  graph_attrib <- graph[["attrib"]]
  for (var in names(graph_attrib)) {
    attrib[, (var) := graph_attrib[[var]]]
  }
  
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  duckdb_df_to_parquet(graph[["data"]], con, file.path(path, paste0(hash, "data.parquet")))
  duckdb_df_to_parquet(graph[["dict"]], con, file.path(path, paste0(hash, "dict.parquet")))
  duckdb_df_to_parquet(attrib, con, file.path(path, paste0(hash, "attrib.parquet")))
  
  dbDisconnect(con, shutdown = TRUE)
  
}

read_cppr_graph <- function(path, hash) {
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  data <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "data.parquet")))
  dict <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "dict.parquet")))
  
  attrib <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "attrib.parquet")))
  graph <- list(
    data = data[, c("from", "to", "dist")],
    coords = NULL,
    dict = dict,
    nbnode = nrow(dict),
    attrib = list(
      aux = NULL,
      cap = NULL,
      alpha = NULL,
      beta = NULL
    )
  )

  if ("aux" %in% colnames(attrib)) {
    graph[["attrib"]][["aux"]] <- attrib$aux
  }
  if ("cap" %in% colnames(attrib)) {
    graph[["attrib"]][["cap"]] <- attrib$cap
  }
  if ("alpha" %in% colnames(attrib)) {
    graph[["attrib"]][["alpha"]] <- attrib$alpha
  }
  if ("beta" %in% colnames(attrib)) {
    graph[["attrib"]][["beta"]] <- attrib$beta
  }

  for (var in setdiff(colnames(attrib), c("i", "aux", "cap", "alpha", "beta"))) {
    graph[["attrib"]][[var]] <- attrib[[var]]
  }
  
  dbDisconnect(con, shutdown = TRUE)
  
  return(graph)
  
}


simplify_cppr_graph <- function(graph, mode = NULL, rm_loop = TRUE, iterate = TRUE) {
  graph_dict <- copy(as.data.table(graph[["dict"]]))
  graph_dict[, ref := as.character(ref)]

  graph_simple <- cpp_simplify(
    graph,
    rm_loop = rm_loop,
    iterate = iterate
  )
  graph_simple_dict <- copy(as.data.table(graph_simple[["dict"]]))
  graph_simple_dict[, ref := as.character(ref)]

  if (!all(graph_simple_dict$ref %in% graph_dict$ref)) {
    suppressWarnings(graph_simple_dict[, ref_input_id := as.integer(ref)])

    graph_simple_dict <- merge(
      graph_simple_dict,
      graph_dict[, list(input_id = id, original_ref = ref)],
      by.x = "ref_input_id",
      by.y = "input_id",
      all.x = TRUE,
      sort = FALSE
    )

    if (anyNA(graph_simple_dict$original_ref)) {
      stop("Failed to preserve original vertex refs during cppRouting simplification.")
    }

    graph_simple_dict[, ref := original_ref]
    graph_simple_dict[, c("ref_input_id", "original_ref") := NULL]
  }

  graph_simple[["dict"]] <- graph_simple_dict

  graph_simple_attrib <- graph_simple[["attrib"]]

  aggregate_simplified_attrib <- function(graph, attrib_name) {
    graph[["data"]][["dist"]] <- graph[["attrib"]][[attrib_name]]

    graph_simplified <- cpp_simplify(
      graph,
      rm_loop = rm_loop,
      iterate = iterate
    )

    graph_simplified[["data"]][["dist"]]
  }

  aggregate_weighted_simplified_attrib <- function(graph, attrib_name, weight_name) {
    graph[["data"]][["dist"]] <- graph[["attrib"]][[attrib_name]] * graph[["attrib"]][[weight_name]]

    graph_simplified <- cpp_simplify(
      graph,
      rm_loop = rm_loop,
      iterate = iterate
    )

    graph_simplified[["data"]][["dist"]]
  }

  graph_simple_attrib[["aux"]] <- aggregate_simplified_attrib(graph, "aux")

  if (mode == "car") {
    required_attrib <- c("n_edges", "cap", "alpha", "beta")

    for (attrib_name in required_attrib) {
      attrib <- graph[["attrib"]][[attrib_name]]
      if (is.null(attrib) || length(attrib) != nrow(graph[["data"]])) {
        stop(
          sprintf(
            "Missing required congestion attribute during simplification: %s",
            attrib_name
          )
        )
      }
    }

    graph_simple_attrib[["n_edges"]] <- aggregate_simplified_attrib(graph, "n_edges")
    graph_simple_attrib[["cap"]] <- aggregate_weighted_simplified_attrib(graph, "cap", "aux") / graph_simple_attrib[["aux"]]
    graph_simple_attrib[["alpha"]] <- aggregate_weighted_simplified_attrib(graph, "alpha", "aux") / graph_simple_attrib[["aux"]]
    graph_simple_attrib[["beta"]] <- aggregate_weighted_simplified_attrib(graph, "beta", "aux") / graph_simple_attrib[["aux"]]

    if (!is.null(graph[["attrib"]][["direct_access"]])) {
      graph_simple_attrib[["direct_access"]] <- ifelse(
        aggregate_simplified_attrib(graph, "direct_access") == graph_simple_attrib[["n_edges"]],
        graph_simple_attrib[["n_edges"]],
        0.0
      )
    }
  }

  graph_simple[["attrib"]] <- graph_simple_attrib
  graph_simple

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
  graph[["dict"]] <- duckdb_parquet_to_df(con, file.path(path, paste0(hash, "dict.parquet")))
  graph[["nbnode"]] <- nrow(graph[["dict"]])
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
