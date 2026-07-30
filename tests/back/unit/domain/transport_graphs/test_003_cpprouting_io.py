import pathlib
import subprocess


def _run_r_code(r_code: str) -> None:
    subprocess.run(["Rscript", "-e", r_code], check=True, capture_output=True, text=True)


def _helper_path() -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    return repo_root / "mobility" / "transport" / "graphs" / "core" / "cpprouting_io.R"


def test_cch_cache_round_trips_all_required_topology_fields():
    r_code = f"""
source("{_helper_path().as_posix()}")

cch <- list(
  rank = c(1L, 2L, 3L),
  tail = c(1L, 2L),
  head = c(2L, 3L),
  first_out = c(0L, 1L, 2L, 2L),
  adj_head = c(2L, 3L),
  adj_arc = c(1L, 2L),
  input_arc = c(1L, 2L, 1L, 2L),
  input_forward = c(TRUE, TRUE, FALSE, FALSE),
  rank_first_out = c(0L, 1L, 2L, 2L),
  rank_adj_head = c(2L, 3L),
  rank_adj_arc = c(1L, 2L),
  elimination_tree_parent = c(2L, 3L, 0L)
)

cache_dir <- tempfile("cch-cache-")
dir.create(cache_dir)
save_cppr_cch(cch, cache_dir, "abc-")
loaded <- read_cppr_cch(cache_dir, "abc-")

stopifnot(identical(names(loaded), names(cch)))
for (field in names(cch)) {{
  stopifnot(identical(as.vector(loaded[[field]]), as.vector(cch[[field]])))
}}
"""

    _run_r_code(r_code)


def test_cch_cache_rebuilds_a_routable_cpp_object():
    r_code = f"""
source("{_helper_path().as_posix()}")
library(cppRoutingCCH)

edges <- data.frame(
  from = c("a", "b"),
  to = c("b", "c"),
  dist = c(1, 2)
)
graph <- makegraph(edges, directed = TRUE)
cch <- cpp_cch_prepare(graph)

cache_dir <- tempfile("cch-cache-")
dir.create(cache_dir)
save_cppr_cch(cch, cache_dir, "abc-")

loaded <- read_cppr_cch(cache_dir, "abc-", graph = graph)
stopifnot(inherits(loaded, "cppRouting_cch"))

metric <- cpp_cch_customize(loaded, graph$data$dist)
values <- get_path_values_pair(
  metric,
  from = "a",
  to = "c",
  values = data.frame(distance = graph$data$dist)
)

stopifnot(values$cost == 3)
stopifnot(values$distance == 3)
"""

    _run_r_code(r_code)


def test_path_routing_helper_loads_ch_and_cch_backends():
    r_code = f"""
source("{_helper_path().as_posix()}")
library(cppRoutingCCH)
library(arrow)

edges <- data.frame(
  from = c("a", "b"),
  to = c("b", "c"),
  dist = c(1, 2),
  distance = c(10, 20)
)
graph <- makegraph(edges[, c("from", "to", "dist")], directed = TRUE)
graph$attrib <- list(aux = edges$distance)

cache_root <- tempfile("path-routing-cache-")
modified_dir <- file.path(cache_root, "path_graph_car", "modified")
contracted_dir <- file.path(cache_root, "path_graph_car", "contracted")
cch_dir <- file.path(cache_root, "path_graph_car", "cch")
dir.create(modified_dir, recursive = TRUE)
dir.create(contracted_dir, recursive = TRUE)
dir.create(cch_dir, recursive = TRUE)

modified_hash <- "aaa"
contracted_hash <- "bbb"
cch_hash <- "ccc"
vertices <- data.frame(vertex_id = c("a", "b", "c"), x = c(0, 1, 2), y = c(0, 0, 0))
write_parquet(vertices, file.path(cache_root, "path_graph_car", paste0(modified_hash, "-vertices.parquet")))
write_parquet(vertices, file.path(cache_root, "path_graph_car", paste0(contracted_hash, "-vertices.parquet")))
write_parquet(vertices, file.path(cache_root, "path_graph_car", paste0(cch_hash, "-vertices.parquet")))

save_cppr_graph(graph, modified_dir, modified_hash)
save_cppr_contracted_graph(cpp_contract(graph, silent = TRUE), contracted_dir, contracted_hash)
save_cppr_cch(cpp_cch_prepare(graph), cch_dir, cch_hash)

ch <- read_cppr_path_routing_graph(
  file.path(contracted_dir, paste0(contracted_hash, "-car-contracted-path-graph")),
  "ch"
)
cch <- read_cppr_path_routing_graph(
  file.path(modified_dir, paste0(modified_hash, "-car-modified-path-graph")),
  "cch",
  file.path(cch_dir, paste0(cch_hash, "-car-cch-path-graph"))
)

for (routing in list(ch, cch)) {{
  values <- get_path_values_pair(
    routing$routing_graph,
    from = "a",
    to = "c",
    values = data.frame(distance = routing$distance_values)
  )
  stopifnot(values$cost == 3)
  stopifnot(values$distance == 30)
  stopifnot(identical(as.character(routing$vertices$vertex_id), c("a", "b", "c")))
}}

stopifnot(is.list(ch$routing_graph))
stopifnot(inherits(cch$cch, "cppRouting_cch"))
"""

    _run_r_code(r_code)
