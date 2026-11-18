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
