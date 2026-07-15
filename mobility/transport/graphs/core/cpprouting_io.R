library(DBI)
library(duckdb)
library(jsonlite)
library(arrow)
library(data.table)

log_parquet_save_step <- function(path, save_expr) {
  message(sprintf("Starting parquet save step: %s", path))
  force(save_expr)
  message(sprintf("Finished parquet save step: %s", path))
}

duckdb_df_to_parquet <- function(df, con, path) {
  message(sprintf("Saving parquet with DuckDB: %s", path))
  
  duckdb_register(con, "df", df)
  
  dbExecute(
    con,
    sprintf(
      "COPY df TO '%s' (FORMAT 'parquet')",
      path
    )
  )
  
  duckdb_unregister(con, "df")
  message(sprintf("Saved parquet with DuckDB: %s", path))
  
}

duckdb_vector_to_parquet <- function(v, con, path) {
  message(sprintf("Saving parquet with DuckDB: %s", path))
  
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
  message(sprintf("Saved parquet with DuckDB: %s", path))
  
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

  data_fp <- file.path(path, paste0(hash, "data.parquet"))
  rank_fp <- file.path(path, paste0(hash, "rank.parquet"))
  shortcuts_fp <- file.path(path, paste0(hash, "shortcuts.parquet"))
  dict_fp <- file.path(path, paste0(hash, "dict.parquet"))
  original_data_fp <- file.path(path, paste0(hash, "original_data.parquet"))
  original_aux_fp <- file.path(path, paste0(hash, "original_data_attrib_aux.parquet"))

  log_parquet_save_step(data_fp, duckdb_df_to_parquet(graph$data, con, data_fp))
  log_parquet_save_step(rank_fp, duckdb_vector_to_parquet(graph$rank, con, rank_fp))
  log_parquet_save_step(shortcuts_fp, duckdb_df_to_parquet(graph$shortcuts, con, shortcuts_fp))
  log_parquet_save_step(dict_fp, duckdb_df_to_parquet(graph$dict, con, dict_fp))
  log_parquet_save_step(original_data_fp, duckdb_df_to_parquet(graph$original$data, con, original_data_fp))
  log_parquet_save_step(original_aux_fp, duckdb_vector_to_parquet(graph$original$attrib$aux, con, original_aux_fp))

  message("Disconnecting DuckDB after contracted graph save...")
  dbDisconnect(con, shutdown = TRUE)
  message("Disconnected DuckDB after contracted graph save.")

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


cch_cache_required_fields <- c(
  "rank",
  "tail",
  "head",
  "first_out",
  "adj_head",
  "adj_arc",
  "input_arc",
  "input_forward",
  "rank_first_out",
  "rank_adj_head",
  "rank_adj_arc",
  "elimination_tree_parent"
)

check_cppr_cch <- function(cch) {
  missing_fields <- setdiff(cch_cache_required_fields, names(cch))
  if (length(missing_fields) > 0) {
    stop(
      sprintf(
        "CCH cache is missing required field(s): %s",
        paste(missing_fields, collapse = ", ")
      )
    )
  }

  invisible(TRUE)
}

cch_cache_file <- function(path, hash, name, extension = "parquet") {
  file.path(path, paste0(hash, "cch_", name, ".", extension))
}

save_cppr_cch <- function(cch, path, hash) {
  check_cppr_cch(cch)

  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }

  metadata <- list(
    format = "cpprouting_cch_parquet",
    version = 1,
    hash = hash,
    nbnode = length(cch$rank),
    nbedge = length(cch$input_arc),
    fields = cch_cache_required_fields
  )

  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

  for (field in cch_cache_required_fields) {
    field_fp <- cch_cache_file(path, hash, field)
    log_parquet_save_step(
      field_fp,
      duckdb_vector_to_parquet(cch[[field]], con, field_fp)
    )
  }

  write_json(
    metadata,
    cch_cache_file(path, hash, "metadata", "json"),
    auto_unbox = TRUE,
    pretty = TRUE
  )

  invisible(path)
}

read_cppr_cch <- function(path, hash, graph = NULL) {
  metadata_fp <- cch_cache_file(path, hash, "metadata", "json")
  if (!file.exists(metadata_fp)) {
    stop(sprintf("Missing CCH cache metadata: %s", metadata_fp))
  }

  metadata <- read_json(metadata_fp)
  if (!identical(metadata$format, "cpprouting_cch_parquet") || metadata$version != 1) {
    stop("Unsupported CCH cache format. Regenerate it with save_cppr_cch().")
  }
  if (!identical(metadata$hash, hash)) {
    stop("CCH cache hash does not match the requested graph hash.")
  }
  if (!identical(unlist(metadata$fields), cch_cache_required_fields)) {
    stop("CCH cache fields do not match the current cppRouting CCH format.")
  }

  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cch <- lapply(cch_cache_required_fields, function(field) {
    duckdb_parquet_to_vector(con, cch_cache_file(path, hash, field))
  })
  names(cch) <- cch_cache_required_fields

  check_cppr_cch(cch)
  if (!is.null(graph)) {
    if (metadata$nbnode != graph$nbnode) {
      stop("CCH cache does not match the supplied graph node count.")
    }

    # cppRouting stores the reusable CCH topology separately from graph labels
    # and original edge attributes. Reattach them here when callers need a
    # routable cppRouting object, while still allowing raw topology reads for
    # cache tests and format validation.
    cch$nbnode <- graph$nbnode
    cch$dict <- graph$dict
    cch$original <- graph[c("data", "attrib")]
    cch <- structure(cch, class = "cppRouting_cch")
  }

  cch
}


read_cppr_routing_graph <- function(graph_fp, cch_fp, weights = NULL) {
  graph_hash <- strsplit(basename(graph_fp), "-")[[1]][1]
  cch_hash <- strsplit(basename(cch_fp), "-")[[1]][1]

  graph <- read_cppr_graph(dirname(graph_fp), graph_hash)
  cch <- read_cppr_cch(dirname(cch_fp), cch_hash, graph = graph)

  if (is.null(weights)) {
    weights <- graph$data$dist
  }

  list(
    graph = graph,
    cch = cch,
    metric = cpp_cch_customize(cch, weights)
  )
}

read_cppr_path_routing_graph <- function(graph_fp, backend, cch_fp = "", weights = NULL) {
  hash <- strsplit(basename(graph_fp), "-")[[1]][1]
  vertices <- as.data.table(read_parquet(file.path(dirname(dirname(graph_fp)), paste0(hash, "-vertices.parquet"))))

  if (backend == "ch") {
    graph <- read_cppr_contracted_graph(dirname(graph_fp), hash)
    return(list(
      graph = graph,
      search_graph = list(data = graph$original$data, dict = graph$dict),
      routing_graph = graph,
      distance_values = graph$original$attrib$aux,
      vertices = vertices
    ))
  }

  if (backend == "cch") {
    routing <- read_cppr_routing_graph(graph_fp, cch_fp, weights = weights)
    return(list(
      graph = routing$graph,
      search_graph = routing$graph,
      routing_graph = routing$metric,
      distance_values = routing$graph$attrib$aux,
      vertices = vertices,
      cch = routing$cch
    ))
  }

  stop("Routing backend must be 'ch' or 'cch'.")
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
