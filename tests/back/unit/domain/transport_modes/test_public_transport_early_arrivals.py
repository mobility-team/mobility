"""Early-arrival limits are in hours, while graph event times are in seconds."""

from pathlib import Path
import shutil
import subprocess

import pytest


def test_early_arrival_allowance_uses_hours(tmp_path):
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript is required for the public transport router")

    helper = Path(__file__).resolve().parents[5] / "mobility/transport/modes/public_transport/filter_early_arrivals.R"
    script = tmp_path / "early_arrivals.R"
    script.write_text(
        """
        library(data.table)
        source(commandArgs(trailingOnly=TRUE)[1])
        # The earliest destination exit is 08:00. With a 15-minute
        # allowance, 07:46 is retained and 07:44 is too early.
        events <- data.table(v_id="stop", edge_type=c("inp", "inp", "out"),
                             time=c(7*3600+46*60, 7*3600+44*60, 8*3600))
        quarter_hour <- filter_early_arrivals(copy(events), 0.25)
        stopifnot(identical(quarter_hour$time, c(27960, 28800)))
        # One hour accepts both arrivals.
        one_hour <- filter_early_arrivals(copy(events), 1.0)
        stopifnot(nrow(one_hour) == 3L)
        """,
        encoding="utf-8",
    )
    result = subprocess.run(
        [rscript, str(script), str(helper)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert result.returncode == 0, result.stdout + result.stderr
