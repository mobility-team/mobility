"""A passenger must not pay for both staying aboard and changing buses."""

from pathlib import Path
import shutil
import subprocess

import pytest


def test_parallel_edges_count_only_the_shortest_alternative(tmp_path):
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript is required for the public transport router")

    helper = Path(__file__).resolve().parents[5] / "mobility/transport/modes/public_transport/path_edge_attributes.R"
    test_script = tmp_path / "parallel_edges.R"
    test_script.write_text(
        """
        library(data.table)
        library(cppRoutingCCH)
        source(commandArgs(trailingOnly=TRUE)[1])
        # At the middle stop the passenger can stay aboard (0 minutes) or
        # transfer to the next bus (8 minutes, perceived as 16 minutes).
        edges <- data.table(from=c(1, 2, 2, 3), to=c(2, 3, 3, 4),
                            perceived_time=c(2, 0, 16, 3),
                            real_time=c(2, 0, 8, 3), distance=c(1, 0, 0, 1))
        graph <- makegraph(edges[, .(from, to, dist=perceived_time)])
        path <- as.data.table(get_path_pair(graph, from="1", to="4", algorithm="NBA", constant=0.1, long=TRUE))
        path[, previous := shift(node), by=.(from,to)]
        selected <- select_shortest_path_edges(edges)
        # Graph nodes returned by cppRoutingCCH are the supplied node labels.
        path[, previous := as.numeric(previous)]
        path[, node := as.numeric(node)]
        result <- merge(path, selected, by.x=c("previous", "node"), by.y=c("from", "to"))
        stopifnot(nrow(result) == 3L, sum(result$real_time) == 5,
                  sum(result$perceived_time) == 5)
        # Equal-cost edges remain separate alternatives; retain the first one.
        ties <- data.table(from=c(1, 1), to=c(2, 2), perceived_time=c(5, 5),
                           real_time=c(4, 3), distance=c(1, 2))
        choice <- select_shortest_path_edges(ties)
        stopifnot(nrow(choice) == 1L, choice$real_time == 4, choice$distance == 1)
        """,
        encoding="utf-8",
    )
    result = subprocess.run(
        [rscript, str(test_script), str(helper)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert result.returncode == 0, result.stdout + result.stderr
