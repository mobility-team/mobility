import pathlib
import subprocess


def _run_r_code(r_code: str) -> None:
    subprocess.run(["Rscript", "-e", r_code], check=True, capture_output=True, text=True)


def _helper_path() -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parents[5]
    return (
        repo_root
        / "mobility"
        / "transport"
        / "graphs"
        / "simplified"
        / "car_edge_attributes.R"
    )


def test_car_access_keeps_motor_vehicle_override():
    """A road with `access=no` but `motor_vehicle=yes` must stay in the car graph."""
    r_code = f"""
source("{_helper_path().as_posix()}")

edges <- data.frame(
  access = c("no", "no", "private", NA),
  motor_vehicle = c("yes", "no", NA, NA),
  motorcar = c(NA, NA, "yes", NA),
  vehicle = c(NA, NA, NA, "designated"),
  stringsAsFactors = FALSE
)

allowed <- car_access_is_allowed(edges)
expected <- c(TRUE, FALSE, FALSE, TRUE)
stopifnot(identical(allowed, expected))
"""

    _run_r_code(r_code)


def test_car_access_decision_can_be_reused_after_osm_access_tags_are_dropped():
    """The graph can keep one access flag after the detailed OSM tags were used."""
    r_code = f"""
source("{_helper_path().as_posix()}")

graph <- data.frame(
  highway = c("secondary", "secondary"),
  access = c("no", "no"),
  motor_vehicle = c("yes", "no"),
  stringsAsFactors = FALSE
)

graph$car_access_allowed <- car_access_is_allowed(graph)
graph <- graph[graph$car_access_allowed, ]

edges <- graph[, c("highway", "access", "car_access_allowed")]
direct_access <- edges$highway %in% c("primary", "secondary")
direct_access <- direct_access & edges$car_access_allowed

stopifnot(identical(nrow(edges), 1L))
stopifnot(identical(direct_access, TRUE))
"""

    _run_r_code(r_code)


def test_mixed_hov_lane_road_is_not_dropped():
    """A HOV-designated road can still have a general traffic lane."""
    r_code = f"""
source("{_helper_path().as_posix()}")

graph <- data.frame(
  hgv = c(NA, NA),
  hov = c("designated", "designated"),
  `hov:lanes` = c("yes|designated", "designated|designated"),
  access = c(NA, NA),
  check.names = FALSE,
  stringsAsFactors = FALSE
)

filtered_graph <- filter_car_streetnet_graph(graph)

stopifnot(identical(nrow(filtered_graph), 1L))
stopifnot(identical(filtered_graph$`hov:lanes`, "yes|designated"))
"""

    _run_r_code(r_code)


def test_reserved_hov_and_psv_lane_is_counted_once_for_capacity():
    """The same reserved lane can be tagged both HOV and PSV."""
    r_code = f"""
library(data.table)
source("{_helper_path().as_posix()}")

graph <- data.frame(
  edge_ = "edge-1",
  .vx0 = "from-node",
  .vx1 = "to-node",
  time_weighted = 1.0,
  d = 100.0,
  highway = "secondary",
  access = NA,
  car_access_allowed = TRUE,
  `hov:lanes` = "yes|designated",
  lanes = "2",
  `lanes:forward` = NA,
  `lanes:backward` = NA,
  `lanes:psv:forward` = NA,
  `lanes:psv:backward` = NA,
  `psv:lanes` = "yes|designated",
  check.names = FALSE,
  stringsAsFactors = FALSE
)

osm_data <- list(
  object_link_edge = data.frame(edge_ = "edge-1", stringsAsFactors = FALSE)
)

osm_capacity_parameters <- data.table(
  highway = "secondary",
  capacity = 1000.0,
  alpha = 0.15,
  beta = 4.0
)

edges <- build_car_edges(graph, osm_data, osm_capacity_parameters)

stopifnot(identical(nrow(edges), 1L))
stopifnot(identical(edges$capacity, 1000.0))
"""

    _run_r_code(r_code)
