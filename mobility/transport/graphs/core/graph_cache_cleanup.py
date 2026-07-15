import pathlib


def graph_cache_paths(cache_path: pathlib.Path, hash_path: pathlib.Path) -> list[pathlib.Path]:
    """Return marker, hash, and sidecar paths owned by one graph asset."""
    cache_path = pathlib.Path(cache_path)
    graph_hash = cache_path.name.split("-", 1)[0]

    paths = [cache_path, pathlib.Path(hash_path)]
    paths.extend(cache_path.parent.glob(f"{graph_hash}*"))
    paths.extend(cache_path.parent.parent.glob(f"{graph_hash}-*.parquet"))

    return list(dict.fromkeys(paths))
