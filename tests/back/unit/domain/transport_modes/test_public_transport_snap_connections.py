"""Building-to-graph distances belong in each public transport journey."""

from pathlib import Path
import shutil
import subprocess

import pytest


def test_snap_connections_are_charged_and_complete_limits_are_applied(tmp_path):
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript is required for the public transport router")
    helper = Path(__file__).resolve().parents[5] / "mobility/transport/modes/public_transport/add_snap_connections.R"
    script = tmp_path / "snap_connections.R"
    script.write_text(
        """
        library(data.table)
        source(commandArgs(trailingOnly=TRUE)[1])
        # Two buildings share the same graph path, but have different gaps.
        costs <- data.table(id=1:4, vertex_id_from_dist=c(0, 500, 1500, 500),
          vertex_id_to_dist=c(0, 250, 0, 250),
          start_distance=100, last_distance=200,
          start_real_time=c(120, 120, 300, 120), last_real_time=60,
          start_perceived_time=120, last_perceived_time=60,
          perceived_time=c(1800, 1800, 1800, 3450))
        first <- list(average_speed=5, max_travel_time=20/60)
        last <- list(average_speed=10, max_travel_time=20/60)
        settings <- list(max_traveltime=1, max_perceived_time=1)
        result <- add_snap_connections(costs, first, last, settings, 120)
        # 500 m at 5 km/h = 6 min; 250 m at 10 km/h = 1.5 min.
        stopifnot(identical(result$id, 1:2))
        stopifnot(result[id==1, start_real_time] == 120)
        stopifnot(result[id==2, start_real_time] == 480)
        stopifnot(result[id==2, last_real_time] == 150)
        stopifnot(result[id==2, start_distance] == 600)
        stopifnot(result[id==2, last_distance] == 450)
        stopifnot(result[id==2, perceived_time] == 2250)
        stopifnot(result[id==2, start_perceived_time] == 480)
        stopifnot(result[id==2, last_perceived_time] == 150)
        # Connections are charged before combining representative buildings.
        stopifnot(sum(result$start_real_time * c(0.25, 0.75)) == 390)
        # The third row exceeds the complete first-leg limit; the fourth
        # exceeds the total perceived-time limit once the gaps are added.
        empty <- add_snap_connections(copy(costs[id==3]), first, last, settings, 120)
        stopifnot(nrow(empty) == 0)
        """,
        encoding="utf-8",
    )
    result = subprocess.run([rscript, str(script), str(helper)], capture_output=True,
                            text=True, encoding="utf-8", errors="replace")
    assert result.returncode == 0, result.stdout + result.stderr


def test_destination_snap_is_included_before_selecting_arrival(tmp_path):
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript is required for the public transport router")
    helper = Path(__file__).resolve().parents[5] / "mobility/transport/modes/public_transport/connect_destinations.R"
    script = tmp_path / "destinations.R"
    script.write_text(
        """
        library(data.table)
        source(commandArgs(trailingOnly=TRUE)[1])
        # Both buildings use the same road vertex. A four-minute walk from the
        # stop reaches the first one; the second needs six more minutes.
        legs <- data.table(from="stop", to="road", time=240, distance=300)
        vertices <- data.table(vertex_id="road", x=0, y=0)
        buildings <- data.table(building_id=c(1L, 2L), x=c(0, 500), y=0)
        transfer <- list(average_speed=5, max_travel_time=20/60)
        result <- connect_destinations(legs, vertices, buildings, transfer, 8)
        first <- result$legs[to == "l2-b1"]
        second <- result$legs[to == "l2-b2"]
        stopifnot(first$time == 240, first$max_time_at_station == 7*3600+56*60)
        stopifnot(second$time == 600, second$distance == 800,
                  second$max_time_at_station == 7*3600+50*60)
        # A service arriving at 07:53 can reach only the first building on time.
        stopifnot(7*3600+53*60 <= first$max_time_at_station,
                  7*3600+53*60 > second$max_time_at_station)
        # Waiting edges only move forwards. A passenger arriving at 07:53 can
        # reach building 1's 07:56 exit, but not building 2's 07:50 exit.
        library(cppRoutingCCH)
        edges <- data.frame(from=c("vehicle", "l1-stop-28200", "l1-stop-28380",
                                   first$from_with_time, second$from_with_time),
                            to=c("l1-stop-28380", "l1-stop-28380", "l1-stop-28560",
                                 first$to, second$to),
                            dist=c(0, 180, 180, first$time, second$time))
        graph <- makegraph(edges)
        distances <- get_distance_matrix(graph, from="vehicle", to=c("l2-b1", "l2-b2"))
        stopifnot(is.finite(distances["vehicle", "l2-b1"]))
        stopifnot(is.na(distances["vehicle", "l2-b2"]) ||
                  !is.finite(distances["vehicle", "l2-b2"]))
        """,
        encoding="utf-8",
    )
    result = subprocess.run([rscript, str(script), str(helper)], capture_output=True,
                            text=True, encoding="utf-8", errors="replace")
    assert result.returncode == 0, result.stdout + result.stderr
