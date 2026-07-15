from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.graphs.core.graph_cache_cleanup import graph_cache_paths


class _TextFileAsset(FileAsset):
    def __init__(self, cache_folder):
        super().__init__({"name": "asset"}, cache_folder / "asset.txt")

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        self.cache_path.write_text("asset", encoding="utf-8")
        return "asset"


def test_file_asset_remove_deletes_marker_and_input_hash(tmp_path):
    asset = _TextFileAsset(tmp_path)
    asset.create_and_get_asset()

    assert asset.cache_path.exists()
    assert asset.hash_path.exists()

    asset.remove()

    assert asset.cache_path.exists() is False
    assert asset.hash_path.exists() is False


def test_graph_cache_paths_include_marker_hash_and_hash_prefixed_sidecars(tmp_path):
    graph_folder = tmp_path / "path_graph_car" / "congested"
    graph_folder.mkdir(parents=True)
    graph_hash = "abc123"
    marker = graph_folder / f"{graph_hash}-car-congested-path-graph"
    hash_marker = graph_folder / f"{graph_hash}-car-congested-path-graph.inputs-hash"
    data = graph_folder / f"{graph_hash}data.parquet"
    attrib = graph_folder / f"{graph_hash}attrib.parquet"
    vertices = graph_folder.parent / f"{graph_hash}-vertices.parquet"
    od_map = graph_folder.parent / f"{graph_hash}-od-vertex-map.parquet"
    other = graph_folder / "otherdata.parquet"

    for path in [marker, hash_marker, data, attrib, vertices, od_map, other]:
        path.write_text("x", encoding="utf-8")

    paths = set(graph_cache_paths(marker, hash_marker))

    assert {marker, hash_marker, data, attrib, vertices, od_map}.issubset(paths)
    assert other not in paths
