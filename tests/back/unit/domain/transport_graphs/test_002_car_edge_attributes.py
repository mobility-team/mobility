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
