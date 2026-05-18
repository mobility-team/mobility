from __future__ import annotations

from pathlib import Path

from mobility.transport.modes.public_transport import public_transport_graph as pt_graph_module
from mobility.transport.modes.public_transport.public_transport_graph import (
    PublicTransportGraph,
    PublicTransportRoutingParameters,
)


def test_public_transport_graph_forwards_gtfs_edits_to_router(monkeypatch, tmp_path):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    calls = {}

    def fake_gtfs_router(transport_zones, additional_gtfs_files, gtfs_edits, expected_agencies):
        calls["gtfs_router"] = (
            transport_zones,
            additional_gtfs_files,
            gtfs_edits,
            expected_agencies,
        )
        return "fake-gtfs-router"

    monkeypatch.setattr(pt_graph_module, "GTFSRouter", fake_gtfs_router)

    params = PublicTransportRoutingParameters(
        additional_gtfs_files=["base.zip"],
        gtfs_edits=[{"mode": "all", "ops": []}],
        expected_agencies=["SNCF"],
    )

    graph = PublicTransportGraph("dummy-transport-zones", params)

    assert graph.cache_path.parent == Path(tmp_path) / "public_transport_graph" / "simplified"
    assert graph.cache_path.name.endswith("-public-transport-graph")
    assert calls["gtfs_router"] == (
        "dummy-transport-zones",
        ["base.zip"],
        [{"mode": "all", "ops": []}],
        ["SNCF"],
    )
